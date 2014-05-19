(ns com.thelastcitadel.raft-test
  (:require [clojure.test :refer :all]
            [com.thelastcitadel.raft :refer :all]
            [clojure.core.async :refer [alt!! timeout >!! chan sliding-buffer]]
            [clojure.tools.logging :as log]))

;; stop sf4j or whatever from printing out nonsense when you run tests
(log/info "logging is terrible")

(defn run
  ([in id cluster]
     (run in id cluster identity))
  ([in id cluster callback]
     (loop [state (raft id cluster)]
       (let [message (alt!!
                      in ([message] message)
                      (timeout
                       (if (= :leader (:node-type (:raft-state state)))
                         300
                         (+ 600 (rand-int 400))))
                      ([_] {:type :timeout
                            :term 0}))
             new-state (run-one state message)
             _ (callback new-state)
             _ (doseq [msg (:out-queue new-state)]
                 (if (= :broadcast (:target msg))
                   (broadcast cluster msg)
                   (send-to cluster (:target msg) msg)))
             new-state (update-in new-state [:out-queue] empty)]

         (if (:stopped? new-state)
           new-state
           (recur new-state))))))

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

(defn f [n]
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
                         (fn [state]
                           (try
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
                             (swap! leaders assoc
                                    (:id state)
                                    (:leader-id (:raft-state state)))
                             (catch Throwable t
                               (prn t)))))
                    (catch Throwable e
                      (log/error e "whoops")))))))
     commited
     value]))

(deftest test-election
  (let [[leaders nodes] (f 3)]
    (try
      (Thread/sleep 10000)
      (is (= 3 (count @leaders)) @leaders)
      (is (every? identity (vals @leaders)) @leaders)
      (is (apply = (vals @leaders)) @leaders)
      (finally
        (doseq [i nodes]
          (future-cancel (:future i)))))))

(deftest test-remove-node
  (let [[leaders nodes] (f 5)]
    (try
      (testing "elect leader"
        (Thread/sleep (* 1000 30))
        (is (= 5 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (testing "kill leader and elect a new one"
        (let [leader (first (vals @leaders))]
          (doseq [node nodes
                  :when (= leader (:id node))]
            (>!! (:in node) {:type :stop :term 0})))
        (Thread/sleep (* 1000 20))
        (reset! leaders {})
        (Thread/sleep (* 1000 60))
        (is (= 4 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (testing "kill leader again and elect a new one"
        (let [leader (first (vals @leaders))]
          (doseq [node nodes
                  :when (= leader (:id node))]
            (>!! (:in node) {:type :stop :term 0})))
        (Thread/sleep (* 1000 20))
        (reset! leaders {})
        (Thread/sleep (* 1000 60))
        (is (= 3 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (finally
        (doseq [i nodes]
          (>!! (:in i) {:type :stop :term 0}))
        (Thread/sleep 50)
        (doseq [i nodes]
          (future-cancel (:future i)))))))

(deftest test-operations
  (let [[leaders nodes commited value] (f 5)]
    (try
      (testing "elect leader"
        (Thread/sleep (* 1000 30))
        (is (= 5 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (let [leader (first (vals @leaders))]
        (doseq [node nodes
                :when (= leader (:id node))]
          (>!! (:in node) {:type :operation
                           :op :write
                           :key "hello"
                           :value "world"
                           :operation-type :write
                           :serial 1})))
      (Thread/sleep (* 1000 60))
      (doseq [[node commited-entries] @commited]
        (is (contains? commited-entries 1)))
      (is (apply = (vals @value)))
      (doseq [[node value] @value]
        (is (= (get value "hello") "world")))
      (finally
        (doseq [i nodes]
          (>!! (:in i) {:type :stop :term 0}))
        (Thread/sleep 50)
        (doseq [i nodes]
          (future-cancel (:future i))
          (log/info @(:future i)))))))

(deftest test-read-operations
  (let [[leaders nodes commited value] (f 3)]
    (try
      (testing "elect leader"
        (Thread/sleep (* 1000 30))
        (is (= 3 (count @leaders)) @leaders)
        (is (every? identity (vals @leaders)) @leaders)
        (is (apply = (vals @leaders)) @leaders))
      (let [leader (first (vals @leaders))
            read (atom nil)]
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
                           :callback (fn [serial value]
                                       (reset! read value))
                           :serial 3}))
        (Thread/sleep (* 30 1000))
        (is (= @read "bob")))
      (finally
        (doseq [i nodes]
          (>!! (:in i) {:type :stop :term 0}))
        (Thread/sleep 50)
        (doseq [i nodes]
          (future-cancel (:future i))
          (log/info @(:future i)))))))
