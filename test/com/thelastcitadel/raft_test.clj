(ns com.thelastcitadel.raft-test
  (:require [clojure.test :refer :all]
            [com.thelastcitadel.raft :refer :all]
            [clojure.core.async :refer [alt!! timeout >!! chan sliding-buffer]]
            [clojure.tools.logging :as log]))

;; stop sf4j or whatever from printing out nonsense when you run tests
(log/info "logging is terrible")

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
        n (for [i (range n)]
            {:id i
             :in (chan (sliding-buffer 10))})
        n (for [{:keys [id] :as m} n]
            (assoc m
              :cluster (->> n
                            (remove #(= id (:id %)))
                            (reduce
                             #(add-node %1 (:id %2) (:in %2))
                             (->ChannelCluster)
                             ))))]
    [leaders (doall (for [{:keys [id in cluster] :as m} n]
                      (assoc m
                        :future (future
                                  (try
                                    (run in id cluster (fn [state]
                                                         (swap! leaders assoc (:id state) (:leader-id (:raft-state state)))))
                                    (catch Throwable e
                                      (log/error e "whoops")))))))]))

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
  (let [[leaders nodes] (f 5)]
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
                           :serial 1})))
      (Thread/sleep (* 1000 60))
      (doseq [i nodes]
        (>!! (:in i) {:type :stop :term 0}))
      (doseq [i nodes
              :let [{:as state {:keys [log commit-index value]} :raft-state} @(:future i)
                    idx (group-by :serial (vals log))
                    [one] (get idx 1)]]
        (is (>= commit-index (:index one)))
        (is (= "world" (get value "hello"))))
      (finally
        (doseq [i nodes]
          (>!! (:in i) {:type :stop :term 0}))
        (Thread/sleep 50)
        (doseq [i nodes]
          (future-cancel (:future i))
          (log/info @(:future i)))))))
