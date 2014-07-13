(ns com.manigfeald.knossos
  (:require [clojure.test :refer :all]
            [com.manigfeald.raft :refer :all]
            [clojure.core.async :refer [alt!! timeout <!! >!! chan
                                        sliding-buffer dropping-buffer
                                        close!]]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [robert.bruce :refer [try-try-again]]
            [com.manigfeald.raft.log :as rlog]
            [knossos.op :as kop]
            [knossos.core :as kn])
  (:import (knossos.core Model)))

;; stop sf4j or whatever from printing out nonsense when you run tests
(log/info "logging is terrible")

(defprotocol Cluster
  (add-node [cluster node arg1])
  (remove-node [cluster node])
  (list-nodes [cluster])
  (send-to [cluster node msg]))

(defrecord ChannelCluster []
  Cluster
  (add-node [cluster node arg1]
    (assoc cluster node arg1))
  (remove-node [cluster node]
    (dissoc cluster node))
  (list-nodes [cluster]
    (keys cluster))
  (send-to [cluster node msg]
    (assert (contains? cluster node) [node msg])
    (>!! (get cluster node) msg)))

(defn shut-it-down! [nodes]
  (doseq [i nodes]
    (future-cancel (:future i))))

(defn raft-obj [in id cluster]
  (let [s (atom nil)
        lock (java.util.concurrent.Semaphore. 1)
        commands (chan (sliding-buffer 2))
        f (future
            (try
              (loop [state (raft id (conj (set (list-nodes cluster)) id)
                                 (->Timer (System/currentTimeMillis)
                                          (System/currentTimeMillis)
                                          1500))]
                (.acquire lock)
                (let [state (cond-> state
                                    (= :leader (:node-type (:raft-state state)))
                                    (assoc-in [:timer :period] 300)
                                    (not= :leader (:node-type
                                                   (:raft-state state)))
                                    (assoc-in [:timer :period]
                                              (+ 500 (* 100 (rand-int 10)))))
                      message (alt!!
                               commands
                               ([message] message)
                               in
                               ([message] message)
                               ;; (timeout 100) ([_] nil)
                               :default nil
                               :priority true)]
                  (let [new-state (try
                                    (-> state
                                        (assoc-in [:timer :now]
                                                  (System/currentTimeMillis))
                                        (assoc-in [:io :message] message)
                                        (run-one))
                                    (finally
                                      (.release lock)))
                        ;; _ (when (= :operation (:type message))
                        ;;     (log/trace (:applied-rules new-state)))
                        _ (doseq [msg (:out-queue (:io new-state))]
                            (assert (not= :broadcast (:target msg)))
                            (send-to cluster (:target msg) msg))
                        _ (doseq [{:keys [level message context] :as m}
                                  (:running-log new-state)]
                            (case level
                              :trace (log/trace message
                                                :context context)))
                        new-state (update-in new-state [:io :out-queue] empty)
                        new-state (update-in new-state [:running-log] empty)
                        new-state (update-in new-state [:applied-rules] empty)]
                    (reset! s new-state)
                    (if (:stopped? new-state)
                      new-state
                      (recur new-state)))))
              (catch InterruptedException _)
              (catch Throwable t
                (locking #'*out*
                  (.printStackTrace t))
                (log/error t "whoops"))))]
    {:in in
     :id id
     :cluster cluster
     :raft s
     :lock lock
     :commands commands
     :future f}))

(defn with-leader [nodes action]
  (let [[[_ leader]] (sort-by
                      first
                      (for [node nodes
                            :let [{:keys [raft]} node
                                  v (deref raft)
                                  {{:keys [leader-id]} :raft-state} v]
                            :when leader-id
                            leader nodes
                            :when (= leader-id (:id leader))]
                        [(* -1 (:current-term (:raft-state v)))
                         leader]))]
    (when leader
      (action leader))
    nil))

(defn applied [nodes serial-w else]
  (let [greatest-term (apply
                       max
                       (for [node nodes]
                         (-> node :raft deref :raft-state :current-term)))]
    (or (first (for [node nodes
                     :let [{:keys [raft]} node
                           v (deref raft)
                           {{:keys [last-applied log]} :raft-state} v]
                     :when (= (:current-term (:raft-state v)) greatest-term)
                     :when log
                     :let [entry (rlog/entry-with-serial log serial-w)]
                     :when entry
                     :when (>= last-applied (:index entry))]
                 entry))
        else)))

(defn raft-write-and-forget
  ([nodes key value]
     (raft-write-and-forget nodes key value (java.util.UUID/randomUUID)))
  ([nodes key value id]
     (with-leader nodes
       (fn [leader]
         (>!! (:commands leader) {:type :operation
                                  :payload {:op :write
                                            :key key
                                            :value value}
                                  :operation-type ::bogon
                                  :serial id})))))

(defn raft-write [nodes key value]
  (let [id (java.util.UUID/randomUUID)]
    (try-try-again
     {:sleep 100
      :tries 300}
     (fn []
       (raft-write-and-forget nodes key value id)
       (when (= :dunno (applied nodes id :dunno))
         (throw (Exception.
                 (with-out-str
                   (println "write failed?")
                   (println "key" key "value" value)
                   (println)
                   (doseq [node nodes]
                     (-> node :raft deref prn)
                     (println)
                     (println))))))))))

(defn raft-read [nodes key]
  (let [id (java.util.UUID/randomUUID)]
    (try
      (try-try-again
       {:sleep 100
        :tries 600}
       (fn []
         (with-leader nodes
           (fn [leader]
             (>!! (:commands leader) {:type :operation
                                      :payload {:op :read
                                                :key key}
                                      :operation-type ::bogon
                                      :serial id})))
         (let [r (applied nodes id :dunno)]
           (if (= :dunno r)
             (throw (Exception. "read failed?"))
             (:return r)))))
      (catch Exception e
        (doseq [node nodes]
          (log/trace (-> node :raft deref)))
        (throw e)))))

(defrecord MapRegister [m]
  Model
  (step [r op]
    (condp = (:f op)
      :write (MapRegister.
              (let [[k v] (:value op)]
                (assoc m k v)))
      :read  (if (or (nil? (second (:value op)))
                     (= (get m (first (:value op)))
                        (second (:value op))))
               r
               (kn/inconsistent
                (str "read " (pr-str (:value op))
                     "from" m))))))

(deftest test-linear
  (let [node-ids-and-channels (into {} (for [i (range 5)]
                                         [i (chan (sliding-buffer 5))]))
        cluster (reduce #(add-node % (key %2) (val %2)) (->ChannelCluster)
                        node-ids-and-channels)
        nodes (doall (for [[node-id in] node-ids-and-channels]
                       (raft-obj in node-id cluster)))
        history (atom [])
        keys (repeatedly 5 gensym)
        stop? (atom false)
        clients (for [client-id (range 10)]
                  (future
                    (while (not @stop?)
                      (let [k (rand-nth keys)]
                        (if (zero? (rand-int 2))
                          (try
                            (swap! history conj (kop/invoke client-id :read [k nil]))
                            (let [r (raft-read nodes k)]
                              (swap! history conj (kop/ok client-id :read [k r])))
                            (catch Exception _
                              (swap! history conj (kop/fail client-id :read [k nil]))))
                          (let [v (gensym)
                                _ (swap! history conj (kop/invoke client-id :write [k v]))]
                            (try
                              (raft-write nodes k v)
                              (swap! history conj (kop/ok client-id :write [k v]))
                              (catch Exception _
                                (swap! history conj (kop/fail client-id :write [k v]))))))))))
        killer (future
                 (while (not @stop?)
                   (let [n (rand-nth nodes)]
                     (try
                       (.acquire (:lock n))
                       (Thread/sleep 500)
                       (finally
                         (.release (:lock n)))))))]
    (doall clients)
    (Thread/sleep (* 1000 (inc (rand-int 60))))
    (reset! stop? true)
    (doseq [f clients]
      (deref f))
    (deref killer)
    (is (:valid? (kn/analysis (->MapRegister {}) @history)))))

(defmacro foo [exp & body]
  (when-not (eval exp)
    `(do ~@body)))

(foo (resolve 'clojure.test/original-test-var)
     (in-ns 'clojure.test)

     (def original-test-var test-var)

     (defn test-var [v]
       (clojure.tools.logging/trace "testing" v)
       (let [start (System/currentTimeMillis)
             result (original-test-var v)]
         (clojure.tools.logging/trace
          "testing" v "took"
          (/ (- (System/currentTimeMillis) start) 1000.0)
          "seconds")
         result))

     (def original-test-ns test-ns)

     (defn test-ns [ns]
       (clojure.tools.logging/trace "testing namespace" ns)
       (let [start (System/currentTimeMillis)
             result (original-test-ns ns)]
         (clojure.tools.logging/trace
          "testing namespace" ns "took"
          (/ (- (System/currentTimeMillis) start) 1000.0)
          "seconds")
         result))

     (in-ns 'com.manigfeald.raft-test))
