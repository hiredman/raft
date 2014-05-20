(ns com.thelastcitadel.raft-test
  (:require [clojure.test :refer :all]
            [com.thelastcitadel.raft :refer :all]
            [clojure.core.async :refer [alt!! timeout <!! >!! chan sliding-buffer dropping-buffer
                                        close!]]
            [clojure.tools.logging :as log]))

(use-fixtures :once
  (fn [f]
    (time
     (do
       (f)
       (print "global timing ")))))

(use-fixtures :each
  (fn [f]
    (time
     (do
       (print "start test ")
       (f)
       (print "timing ")))))

;; stop sf4j or whatever from printing out nonsense when you run tests
(log/info "logging is terrible")

(defprotocol Cluster
  (add-node [cluster node arg1])
  (remove-node [cluster node])
  (list-nodes [cluster])
  (send-to [cluster node msg]))

(defn v [channel to r]
  (alt!!
   channel ([message] message)
   (timeout to)
   ([_] r)))

(defn run
  ([in id cluster]
     (run in id cluster identity))
  ([in id cluster callback]
     (loop [state (raft id (conj (set (list-nodes cluster)) id))
            callbacks {}]
       (assert (instance? clojure.lang.PersistentQueue (:running-log state)))
       (let [message (alt!!
                      in ([message] message)
                      (timeout
                       (if (= :leader (:node-type (:raft-state state)))
                         300
                         (+ 500 (rand-int 1000))))
                      ([_] {:type :timeout
                            :term 0}))]
         (if (= :await (:type message))
           (recur state (update-in callbacks [(:serial message)]
                                   conj (:callback message)))
           (let [new-state (run-one state message)
                 _ (callback state new-state)
                 _ (doseq [msg (:out-queue new-state)]
                     (assert (not= :broadcast (:target msg)))
                     (send-to cluster (:target msg) msg))
                 _ (doseq [{:keys [level message] :as m} (:running-log new-state)]
                     (case level
                       :trace (log/trace message)))
                 new-state (update-in new-state [:out-queue] empty)
                 new-state (update-in new-state [:running-log] empty)
                 callbacks (reduce
                            (fn [cbs fun] (fun cbs))
                            callbacks
                            (for [{:keys [serial] :as entry}
                                  (:applied new-state)
                                  :when (contains? callbacks serial)
                                  callback (get callbacks serial)]
                              (fn [callbacks]
                                (callback entry)
                                (dissoc callbacks serial))))]
             (if (:stopped? new-state)
               new-state
               (recur new-state callbacks))))))))

(defrecord ChannelCluster []
  Cluster
  (add-node [cluster node arg1]
    (assoc cluster node arg1))
  (remove-node [cluster node]
    (dissoc cluster node))
  (list-nodes [cluster]
    (keys cluster))
  (send-to [cluster node msg]
    (assert (contains? cluster node))
    (>!! (get cluster node) msg)))

(defn f [n l]
  (let [leaders (atom {})
        commited (atom {})
        value (atom {})
        n (for [i (range n)]
            {:id i
             :in (chan (sliding-buffer 10))})
        n (for [{:keys [id] :as m} n]
            (assoc m
              :cluster (->> n
                            (remove #(= id (:id %)))
                            (reduce
                             #(add-node %1 (:id %2) (:in %2))
                             (->ChannelCluster)))))]
    [leaders
     (doall (for [{:keys [id in cluster] :as m} n]
              (assoc m
                :future
                (future
                  (try
                    (run in id cluster
                         (fn [old-state state]
                           (try
                             (when (not= (:leader-id (:raft-state old-state))
                                         (:leader-id (:raft-state state)))
                               (swap! leaders assoc
                                      (:id state)
                                      (:leader-id (:raft-state state)))
                               (>!! l (:leader-id (:raft-state state))))
                             (swap! value assoc
                                    (:id state)
                                    (:value (:raft-state state)))
                             (doseq [entry (:log (:raft-state state))
                                     :when (:serial entry)
                                     :when
                                     (>= (:commit-index (:raft-state state))
                                         (:index entry))]
                               (swap! commited assoc-in
                                      [(:id state) (:serial entry)] entry))

                             (catch Throwable t
                               (prn t)))))
                    (catch Throwable e
                      (log/error e "whoops")))))))
     commited
     value]))

(def n 1000)

(defn shut-it-down! [nodes]
  (doseq [i nodes]
    (>!! (:in i) {:type :stop :term 0}))
  (Thread/sleep (* n 0.01))
  (doseq [i nodes]
    (future-cancel (:future i))))

(deftest test-election
  (let [leader (chan (dropping-buffer 10))
        [leaders nodes] (f 3 leader)]
    (try
      (dotimes [_ 3] (v leader (* 60 1000) :timeout))
      (is (= 3 (count @leaders)) @leaders)
      (is (every? identity (vals @leaders)) @leaders)
      (is (apply = (vals @leaders)) @leaders)
      (finally
        (shut-it-down! nodes)))))

(deftest test-remove-node
  (let [leader (chan (dropping-buffer 10))
        [leaders nodes] (f 5 leader)]
    (try
      (testing "elect leader"
        (dotimes [_ 5] (v leader (* 60 1000) :timeout))
        (is (= 5 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (testing "kill leader and elect a new one"
        (let [leader' (first (vals @leaders))]
          (reset! leaders {})
          (doseq [node nodes
                  :when (= leader' (:id node))]
            (>!! (:in node) {:type :operation
                             :op :write
                             :node leader'
                             :operation-type :remove-node
                             :serial 1})
            (doseq [node nodes
                    :when (not (future-done? (:future node)))]
              (let [c (chan)]
                (>!! (:in node) {:type :await
                                 :callback (fn [_]
                                             (close! c))
                                 :serial 1})
                (v c (* 1000 60) :timeout)))
            (>!! (:in node) {:type :stop :term 0}))
          (dotimes [_ 4]
            (v leader (* 60 1000) :timeout)))
        (is (= 4 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (testing "kill leader again and elect a new one"
        (let [leader (first (vals @leaders))]
          (reset! leaders {})
          (doseq [node nodes
                  :when (= leader (:id node))]
            (>!! (:in node) {:type :operation
                             :op :write
                             :node leader
                             :operation-type :remove-node
                             :serial 2})
            (doseq [node nodes
                    :when (not (future-done? (:future node)))]
              (let [c (chan)]
                (>!! (:in node) {:type :await
                                 :callback (fn [_]
                                             (close! c))
                                 :serial 2})
                (v c (* 1000 60) :timeout)))
            (>!! (:in node) {:type :stop})))
        (dotimes [_ 3] (v leader (* 1000 60) :timeout))
        (is (= 3 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (finally
        (shut-it-down! nodes)))))

(deftest test-operations
  (let [leader (chan (dropping-buffer 10))
        [leaders nodes commited value] (f 5 leader)]
    (try
      (testing "elect leader"
        (dotimes [_ 5] (v leader (* 60 1000) :timeout))
        (is (= 5 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (let [leader (first (vals @leaders))
            c (chan 2)]
        (doseq [node nodes
                :when (= leader (:id node))]
          (>!! (:in node) {:type :operation
                           :op :write
                           :key "hello"
                           :value "world"
                           :operation-type :write
                           :serial 1})
          (>!! (:in node) {:type :await
                           :serial 1
                           :callback (fn [e]
                                       (close! c))}))
        (is (not= :timeout (v c (* 60 1000) :timeout))))
      (Thread/sleep 1000)
      (doseq [[node commited-entries] @commited]
        (is (contains? commited-entries 1)))
      (is (apply = (vals @value)))
      (doseq [[node value] @value]
        (is (= (get value "hello") "world")))
      (finally
        (shut-it-down! nodes)))))

(deftest test-read-operations
  (let [leader (chan (dropping-buffer 2))
        [leaders nodes commited value] (f 3 leader)]
    (try
      (testing "elect leader"
        (dotimes [_ 3] (<!! leader))
        (is (= 3 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (let [leader (first (vals @leaders))
            c (chan 1)]
        (doseq [node nodes
                :when (= leader (:id node))]
          (>!! (:in node) {:type :operation
                           :op :write
                           :key "hello"
                           :value "world"
                           :operation-type :write
                           :serial 1})
          (>!! (:in node) {:type :operation
                           :op :write
                           :key "hello"
                           :value "bob"
                           :operation-type :write
                           :serial 2})
          (>!! (:in node) {:type :operation
                           :op :read
                           :key "hello"
                           :operation-type :read
                           :serial 3})
          (>!! (:in node) {:type :await
                           :callback (fn [{:keys [value]}]
                                       (>!! c value))
                           :serial 3}))
        (is (= (v c (* 1000 60) :timeout) "bob")))
      (finally
        (shut-it-down! nodes)))))
