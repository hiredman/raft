>(ns com.manigfeald.raft-test
   (:require [clojure.test :refer :all]
             [com.manigfeald.raft :refer :all]
             [clojure.core.async :refer [alt!! timeout <!! >!! chan
                                         sliding-buffer dropping-buffer
                                         close!]]
             [clojure.tools.logging :as log]
             [clojure.pprint :as pp]
             [robert.bruce :refer [try-try-again]]
             [com.manigfeald.raft.log :as rlog]))

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

;; (deftest test-kill-node
;;   (dotimes [i 8]
;;     (let [n (inc (* (inc i) 2))
;;           leader (chan (dropping-buffer 10))
;;           [leaders nodes] (f n leader)]
;;       (try
;;         (testing "elect leader"
;;           (is (stable-leader? nodes n)))
;;         (dotimes [ii (Math/floor (/ n 2))]
;;           (testing "kill leader and elect a new one"
;;             (let [[leader'] (leaders-of nodes 1)]
;;               (doseq [node nodes
;;                       :when (= leader' (:id node))]
;;                 (future-cancel (:future node)))
;;               (is (stable-leader?
;;                    (for [node nodes
;;                          :when (not (future-done? (:future node)))]
;;                      node)
;;                    (- n (inc ii)))))))
;;         (finally
;;           (shut-it-down! nodes))))))

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

(defn stable-leader? [nodes n]
  (try-try-again
   {:decay :exponential
    :sleep 10
    :tries 20}
   (fn []
     (let [lead (for [node nodes
                      :let [{:keys [raft]} node
                            v (deref raft)
                            {{:keys [leader-id]} :raft-state} v]
                      :when leader-id
                      leader nodes
                      :when (= leader-id (:id leader))]
                  leader)
           f (frequencies lead)
           [lead c] (last (sort-by second f))]
       #_(log/trace "stable-leader?" n (into {} (for [[a b] f]
                                                  [(-> a :raft deref :id) b])))
       (if (and lead
                (>= c n))
         lead
         (throw (Exception. "failed to get a leader")))))))

(defn with-leader [nodes action]
  (let [[[_ leader]] (sort-by first (for [node nodes
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

(defn await-applied [nodes serial-w dunno]
  (try
    (try-try-again
     {:sleep 100
      :tries 60}
     (fn []
       #_(log/trace "await-applied body")
       (let [r (for [node nodes
                     :let [{:keys [raft]} node
                           v (deref raft)
                           {{:keys [last-applied log]} :raft-state} v]
                     :when log
                     :let [entry (rlog/entry-with-serial log serial-w)]
                     :when entry
                     :when (>= last-applied (:index entry))]
                 (assoc entry
                   :last-applied last-applied))]
         ;; pass in n?
         (if (and (= (count nodes) (count r))
                  (apply = r))
           (first r)
           (throw (Exception.))))))
    (catch Exception e
      dunno)))

(defn last-applied [nodes]
  (frequencies
   (doall (for [node nodes]
            (-> node :raft deref :raft-state :last-applied)))))

(defn index [nodes serial]
  (frequencies
   (for [node nodes
         :let [log (-> node :raft deref :raft-state :log)
               entry (rlog/entry-with-serial log serial)]
         :when entry]
     (:index entry))))

(defn applied [nodes serial-w else]
  (let [greatest-term (apply max (for [node nodes]
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
     {:sleep 10
      :tries 6000}
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
       {:sleep 10
        :tries 6000}
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

(defmacro with-pings [nodes & body]
  `(let [nodes# ~nodes
         fut# (future
                (while true
                  (Thread/sleep 15000)
                  (future
                    (raft-write-and-forget nodes# "ping" "pong"))))]
     (try
       ~@body
       (finally
         (future-cancel fut#)))))

(deftest test-leader-election
  (let [node-ids-and-channels (into {} (for [i (range 3)]
                                         [i (chan (sliding-buffer 10))]))
        cluster (reduce #(add-node % (key %2) (val %2)) (->ChannelCluster)
                        node-ids-and-channels)
        nodes (doall (for [[node-id in] node-ids-and-channels]
                       (raft-obj in node-id cluster)))]
    (try
      (is (stable-leader? nodes 3))
      (finally
        (shut-it-down! nodes)))))

(deftest test-remove-node
  (let [node-ids-and-channels (into {} (for [i (range 5)]
                                         [i (chan (sliding-buffer 10))]))
        cluster (reduce #(add-node % (key %2) (val %2)) (->ChannelCluster)
                        node-ids-and-channels)
        nodes (doall (for [[node-id in] node-ids-and-channels]
                       (raft-obj in node-id cluster)))]
    (try
      (testing "elect leader"
        (is (stable-leader? nodes 5)))
      (testing "kill leader and elect a new one"
        (future-cancel (:future (stable-leader? nodes 5)))
        (is (stable-leader? nodes 4))
        (future-cancel (:future (stable-leader? nodes 4)))
        (is (stable-leader? nodes 3)))
      (finally
        (shut-it-down! nodes)))))

(deftest test-operations
  (let [node-ids-and-channels (into {} (for [i (range 5)]
                                         [i (chan (sliding-buffer 10))]))
        cluster (reduce #(add-node % (key %2) (val %2))
                        (->ChannelCluster) node-ids-and-channels)
        nodes (doall (for [[node-id in] node-ids-and-channels]
                       (raft-obj in node-id cluster)))]
    (with-pings nodes
      (try
        (testing "elect leader"
          (is (stable-leader? nodes 5)))
        (raft-write nodes "hello" "world")
        (doseq [node nodes
                :let [{:keys [raft]} node
                      {{{:strs [hello]} :value} :raft-state} (deref raft)]]
          (is (= hello "world") (deref (:raft (stable-leader? nodes 5)))))
        (finally
          (shut-it-down! nodes))))))

(deftest test-read-operations
  (let [node-ids-and-channels (into {} (for [i (range 5)]
                                         [i (chan (sliding-buffer 10))]))
        cluster (reduce #(add-node % (key %2) (val %2))
                        (->ChannelCluster) node-ids-and-channels)
        nodes (doall (for [[node-id in] node-ids-and-channels]
                       (raft-obj in node-id cluster)))]
    (with-pings nodes
      (try
        (testing "elect leader"
          (is (stable-leader? nodes 5)))
        (raft-write nodes "hello" "world")
        (let [x (raft-read nodes "hello")]
          (is (= "world" x)
              (with-out-str
                (prn x)
                (println)
                (doseq [node nodes]
                  (prn (-> node :raft deref))
                  (println)
                  (println)))))
        (finally
          (shut-it-down! nodes))))))

(deftest test-stress
  (let [node-ids-and-channels (into {} (for [i (range 3)]
                                         [i (chan (sliding-buffer 5))]))
        cluster (reduce #(add-node % (key %2) (val %2)) (->ChannelCluster)
                        node-ids-and-channels)
        nodes (doall (for [[node-id in] node-ids-and-channels]
                       (raft-obj in node-id cluster)))
        a (atom 0)]
    (with-pings nodes
      (try
        (raft-write nodes :key 0)
        (dotimes [i 10]
          (log/trace "start")
          (let [victim (rand-nth nodes)]
            (log/trace "victim" (:id victim))
            (.acquire (:lock victim))
            (try
              (let [rv (raft-read nodes :key)]
                (log/trace "read" rv)
                (when-not (= rv @a)
                  (log/trace "FAILLLLLLL")
                  (doseq [node (sort-by :id  nodes)
                          :let [_ (log/trace "node" (:id node))
                                {:keys [raft id]} node
                                v (deref raft)]
                          entry (sort-by :index (vals (:log (:raft-state v))))]
                    (log/trace id "log entry" entry))
                  (assert nil))
                (is (= @a (raft-read nodes :key))
                    (with-out-str
                      (pp/pprint nodes))))
              (swap! a inc)
              (log/trace "writing" @a)
              (raft-write nodes :key @a)
              (finally
                (.release (:lock victim))))))
        (finally
          (shut-it-down! nodes))))))

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
