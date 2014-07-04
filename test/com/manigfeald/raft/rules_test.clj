(ns com.manigfeald.raft.rules-test
  (:require [clojure.test :refer :all]
            [com.manigfeald.raft.rules :refer :all]
            [clojure.tools.logging :as log]
            [com.manigfeald.raft.core :refer :all])
  (:import (clojure.lang PersistentQueue)))

;; TODO: when applied always have a :return

;; TODO: these tests really suck

(deftest t-keep-up-apply
  (is (= [false {:raft-state {:commit-index 0 :last-applied 0}}]
         (keep-up-apply {:raft-state {:commit-index 0 :last-applied 0}})))
  (is (= [true {:raft-state {:commit-index 1
                             :last-applied 1
                             :log {1 {:payload {:op :write
                                                :key :foo
                                                :value :bar}
                                      :index 1
                                      :return nil}}
                             :value (assoc (->MapValue)
                                      :foo :bar)}}]
         (keep-up-apply {:raft-state {:commit-index 1
                                      :last-applied 0
                                      :log {1 {:payload {:op :write
                                                         :key :foo
                                                         :value :bar}
                                               :index 1}}
                                      :value (->MapValue)}}))))

(deftest t-jump-to-newer-term
  (is (= [false {:io {:message {:term 1}}
                 :running-log PersistentQueue/EMPTY
                 :raft-state {:current-term 1}}]
         (jump-to-newer-term {:io {:message {:term 1}}
                              :running-log PersistentQueue/EMPTY
                              :raft-state {:current-term 1}})))
  (is (= [true {:io {:message {:term 2}}
                :raft-state {:current-term 2
                             :node-type :follower
                             :votes 0
                             :last-applied 0
                             :commit-index 0
                             :log {}
                             :leader-id nil
                             :voted-for nil}}]
         (update-in (jump-to-newer-term
                     {:io {:message {:term 2}}
                      :running-log PersistentQueue/EMPTY
                      :raft-state {:current-term 1
                                   :last-applied 0
                                   :commit-index 0
                                   :log {}
                                   :node-type :candidate-id}})
                    [1] dissoc :running-log))))

(deftest t-follower-respond-to-append-entries
  (testing "failing old term"
    (is (= [true {:id :com.manigfeald.raft.rules-test/me,
                  :raft-state {:last-applied 1,
                               :node-type :follower,
                               :commit-index 1,
                               :current-term 1},
                  :timer {:next-timeout 8,
                          :now 5,
                          :period 3},
                  :io {:out-queue [{:type :append-entries-response,
                                    :term 1,
                                    :success? false,
                                    :from ::me,
                                    :target ::bob}],
                       :message nil}}]
           (update-in (follower-respond-to-append-entries
                       {:id ::me
                        :running-log PersistentQueue/EMPTY
                        :raft-state {:node-type :follower
                                     :current-term 1
                                     :commit-index 1
                                     :last-applied 1}
                        :timer {:period 3
                                :now 5}
                        :io {:message {:type :append-entries
                                       :leader-id ::bob
                                       :term 0}}})
                      [1] dissoc :running-log))))
  (testing "entries not in log"
    (let [[matched? result] (follower-respond-to-append-entries
                             {:id ::me
                              :raft-state {:node-type :follower
                                           :current-term 1
                                           :commit-index 1
                                           :last-applied 1
                                           :log {}}
                              :timer {:period 3
                                      :now 5}
                              :running-log PersistentQueue/EMPTY
                              :io {:message {:type :append-entries
                                             :term 1
                                             :prev-log-index 1
                                             :prev-log-term 1}}})
          {{messages :out-queue} :io} result]
      (is matched? result)
      (doseq [{:keys [type term success? from]} messages]
        (is (= type :append-entries-response))
        (is (= term 1))
        (is (not success?))
        (is (= from ::me))))))
(testing "success"
  (let [[matched? result] (follower-respond-to-append-entries
                           {:id ::me
                            :raft-state {:node-type :follower
                                         :current-term 1
                                         :commit-index 0
                                         :last-applied 0
                                         :log {1 {:term 1}}}
                            :timer {:period 3
                                    :now 5}
                            :running-log PersistentQueue/EMPTY
                            :io {:message {:type :append-entries
                                           :term 1
                                           :leader-commit 0
                                           :leader-id ::bob
                                           :prev-log-index 1
                                           :prev-log-term 1}}})
        {{messages :out-queue} :io} result]
    (is matched? result)
    (is (= ::bob (:leader-id (:raft-state result))))
    (doseq [{:keys [type term success? from last-log-index]}
            messages]
      (is (= type :append-entries-response))
      (is (= term 1))
      (is success?)
      (is (= from ::me))
      (is (= 1 last-log-index)))))

(deftest t-follower-respond-to-request-vote
  (testing "not voted zero last term and index"
    (let [[applied? result] (follower-respond-to-request-vote
                             {:id ::me
                              :raft-state {:node-type :follower
                                           :current-term 1
                                           :voted-for nil
                                           :commit-index 0
                                           :last-applied 0
                                           :log {}}
                              :timer {:period 3
                                      :now 5}
                              :running-log PersistentQueue/EMPTY
                              :io {:message {:type :request-vote
                                             :term 1
                                             :candidate-id ::bob
                                             :last-log-index 0
                                             :last-log-term 0}}})
          messages (:out-queue (:io result))]
      (is applied? result)
      (is (= 1 (count messages)) messages)
      (doseq [{:keys [type target from success?]} messages]
        (is success?)
        (is (= from ::me))
        (is (= target ::bob))
        (is (= type :request-vote-response)))))
  (testing "not voted and last term and index match"
    (let [[applied? result] (follower-respond-to-request-vote
                             {:id ::me
                              :raft-state {:node-type :follower
                                           :current-term 1
                                           :voted-for nil
                                           :commit-index 0
                                           :last-applied 0
                                           :log {1 {:term 1}}}
                              :timer {:period 3
                                      :now 5}
                              :running-log PersistentQueue/EMPTY
                              :io {:message {:type :request-vote
                                             :term 1
                                             :candidate-id ::bob
                                             :last-log-index 1
                                             :last-log-term 1}}})
          messages (:out-queue (:io result))]
      (is applied? result)
      (is (= 1 (count messages)) messages)
      (doseq [{:keys [type target from success?]} messages]
        (is success?)
        (is (= from ::me))
        (is (= target ::bob))
        (is (= type :request-vote-response)))))
  (testing "not voted and last term and last index don't match"
    (let [[applied? result] (follower-respond-to-request-vote
                             {:id ::me
                              :raft-state {:node-type :follower
                                           :current-term 1
                                           :voted-for nil
                                           :commit-index 0
                                           :last-applied 0
                                           :log {}}
                              :timer {:period 3
                                      :now 5}
                              :running-log PersistentQueue/EMPTY
                              :io {:message {:type :request-vote
                                             :term 1
                                             :candidate-id ::bob
                                             :last-log-index -1
                                             :last-log-term -11}}})
          messages (:out-queue (:io result))]
      (is applied? result)
      (is (= 1 (count messages)) messages)
      (doseq [{:keys [type target from success?]} messages]
        (is (not success?))
        (is (= from ::me))
        (is (= target ::bob))
        (is (= type :request-vote-response)))))
  (testing "already voted"
    (let [[applied? result] (follower-respond-to-request-vote
                             {:id ::me
                              :raft-state {:node-type :follower
                                           :current-term 1
                                           :voted-for ::alice
                                           :commit-index 1
                                           :last-applied 1
                                           :log {}}
                              :timer {:period 3
                                      :now 5}
                              :running-log PersistentQueue/EMPTY
                              :io {:message {:type :request-vote
                                             :term 1
                                             :candidate-id ::bob
                                             :last-log-index 1
                                             :last-log-term 1}}})
          messages (:out-queue (:io result))]
      (is applied? result)
      (is (= 1 (count messages)) messages)
      (doseq [{:keys [type target from success?]} messages]
        (is (not success?))
        (is (= from ::me))
        (is (= target ::bob))
        (is (= type :request-vote-response))))))

(deftest t-become-candidate-and-call-for-election
  (let [[applied? result] (become-candidate-and-call-for-election
                           {:id ::me
                            :raft-state {:node-type :follower
                                         :current-term 1
                                         :commit-index 0
                                         :last-applied 0
                                         :voted-for nil
                                         :node-set #{::a ::b ::c}
                                         :log {1 {:index 1
                                                  :term 1}}}
                            :running-log PersistentQueue/EMPTY
                            :timer {:period 3
                                    :next-timeout 3
                                    :now 5}
                            :io {:message nil}})
        messages (:out-queue (:io result))]
    (is (= 8 (:next-timeout (:timer result))) result)
    (is applied? result)
    (is (= 3 (count messages)) messages)
    (is (= #{::a ::b ::c} (set (map :target messages))))
    (is (= 2 (:current-term (:raft-state result))))
    (doseq [{:keys [type target candidate-id from last-log-term
                    last-log-index term]}
            messages]
      (is (= term 2))
      (is (= last-log-index 1))
      (is (= last-log-term 1))
      (is (= from ::me))
      (is (= candidate-id ::me))
      (is (= type :request-vote)))))

(deftest t-candidate-receive-votes
  (testing "receive vote, become master"
    (let [[applied? result] (candidate-receive-votes
                             {:id ::me
                              :raft-state {:node-type :candidate
                                           :current-term 1
                                           :last-applied 0
                                           :commit-index 0
                                           :log {}
                                           :node-set #{1 2 3}
                                           :votes 2}
                              :timer {:now 10
                                      :period 3}
                              :running-log PersistentQueue/EMPTY
                              :io {:message {:type :request-vote-response
                                             :success? true}}})
          {{messages :in-queue} :io} result]
      (is applied? result)
      (is (= :leader (:node-type (:raft-state result))))
      (is (= 13 (:next-timeout (:timer result))))))
  (testing "receive vote, count it"
    (let [[applied? result] (candidate-receive-votes
                             {:id ::me
                              :raft-state {:node-type :candidate
                                           :current-term 1
                                           :last-applied 0
                                           :commit-index 0
                                           :log {}
                                           :node-set #{1 2 3 4 5}
                                           :votes 1}
                              :running-log PersistentQueue/EMPTY
                              :timer {:now 10
                                      :period 3}
                              :io {:message {:type :request-vote-response
                                             :success? true}}})
          {{messages :in-queue} :io} result]
      (is applied? result)
      (is (= :candidate (:node-type (:raft-state result))))
      (is (= 2 (:votes (:raft-state result))))))
  (testing "don't receive vote"
    (let [[applied? result] (candidate-receive-votes
                             {:id ::me
                              :raft-state {:node-type :candidate
                                           :current-term 1
                                           :node-set #{1 2 3 4 5}
                                           :votes 1}
                              :running-log PersistentQueue/EMPTY
                              :timer {:now 10
                                      :period 3}
                              :io {:message {:type :request-vote-response
                                             :success? false}}})
          {{messages :in-queue} :io} result]
      (is (not applied?) result))))

(deftest t-candidate-respond-to-append-entries
  (let [[applied? result] (candidate-respond-to-append-entries
                           {:id ::me
                            :raft-state {:node-type :candidate
                                         :current-term 1
                                         :commit-index 0
                                         :last-applied 0
                                         :node-set #{1 2 3}
                                         :votes 2}
                            :timer {:now 10
                                    :period 3}
                            :running-log PersistentQueue/EMPTY
                            :io {:message {:type :append-entries
                                           :term 1
                                           :leader-id ::bob
                                           :prev-log-index 0
                                           :prev-log-term 0
                                           :entries []
                                           :leader-commit 0}}})
        {{messages :in-queue} :io} result]
    (is applied? result)
    (is (= :follower (:node-type (:raft-state result))))
    (is (= 13 (:next-timeout (:timer result))))))

(deftest t-candidate-election-timeout
  (let [[applied? result] (candidate-election-timeout
                           {:id ::me
                            :raft-state {:node-type :candidate
                                         :current-term 1
                                         :node-set #{::a ::b ::c}
                                         :commit-index 0
                                         :last-applied 0
                                         :log {}}
                            :running-log PersistentQueue/EMPTY
                            :timer {:now 10
                                    :next-timeout 9
                                    :period 3}
                            :io {:message nil}})
        {{messages :out-queue} :io} result]
    (is applied? result)
    (is (= #{::a ::b ::c} (set (map :target messages))))
    (is (= 13 (:next-timeout (:timer result))))
    (is (= 2 (:current-term (:raft-state result))))
    (doseq [{:keys [type candidate-id term]} messages]
      (is (= type :request-vote))
      (is (= candidate-id ::me))
      (is (= 2 term)))))

(deftest t-heart-beat
  (let [[applied? result] (heart-beat
                           {:id ::me
                            :raft-state {:node-type :leader
                                         :current-term 1
                                         :commit-index 0
                                         :last-applied 0
                                         :log {}
                                         :node-set #{::a ::b ::me}}
                            :timer {:now 10
                                    :next-timeout 9
                                    :period 3}
                            :io {:message nil}})
        {{messages :out-queue} :io} result]
    (is applied? result)
    (is (= 2 (count messages)))
    (is (= #{::a ::b} (set (map :target messages))))
    (is (= #{:append-entries} (set (map :type messages))))))

(deftest t-update-followers
  (let [[applied? result] (update-followers
                           {:id ::me
                            :raft-state {:node-type :leader
                                         :current-term 1
                                         :commit-index 1
                                         :last-applied 1
                                         :log {1 {:index 1 :term 1}}
                                         :node-set #{::a ::b ::me}}
                            :raft-leader-state {:next-index {::a 1
                                                             ::b 1}}
                            :io {:message nil}})
        {{messages :out-queue} :io} result]
    (is (= 2 (count messages)))
    (is applied? result)
    (doseq [{:keys [type entries prev-log-term prev-log-index]} messages]
      (is (zero? prev-log-term))
      (is (zero? prev-log-index))
      (is (= type :append-entries))
      (is (= [{:index 1 :term 1}] entries))))
  (let [[applied? result] (update-followers
                           {:id ::me
                            :raft-state {:node-type :leader
                                         :current-term 1
                                         :log {1 {:index 1 :term 1}}
                                         :node-set #{::a ::b ::me}}
                            :raft-leader-state {:next-index {::a 2
                                                             ::b 2}}
                            :io {:message nil}})
        {{messages :out-queue} :io} result]
    (is (not applied?) result)))

(deftest t-update-commit
  (let [[applied? result] (update-commit
                           {:id ::me
                            :running-log PersistentQueue/EMPTY
                            :raft-state {:node-type :leader
                                         :current-term 1
                                         :last-applied 0
                                         :log {1 {:index 1 :term 1}}
                                         :node-set #{::a ::b ::me}
                                         :commit-index 0}
                            :raft-leader-state {:match-index {::a 1
                                                              ::b 1}}
                            :io {:message nil}})
        {{messages :out-queue} :io} result]
    (is applied? result)
    (is (= 1 (:commit-index (:raft-state result))))))

(deftest t-leader-handle-append-entries-response
  (let [[applied? result] (leader-handle-append-entries-response
                           {:id ::me
                            :raft-state {:node-type :leader
                                         :current-term 1
                                         :last-applied 0
                                         :log {1 {:index 1 :term 1}}
                                         :node-set #{::a ::b ::me}
                                         :commit-index 0}
                            :raft-leader-state {:match-index {::a 0}
                                                :next-index {::a 1}}
                            :io {:message {:type :append-entries-response
                                           :from ::a
                                           :success? false}}})
        {{messages :out-queue} :io} result]
    (is applied? result)
    (is (= 0 (::a (:next-index (:raft-leader-state result))))))
  (let [[applied? result] (leader-handle-append-entries-response
                           {:id ::me
                            :raft-state {:node-type :leader
                                         :current-term 1
                                         :last-applied 0
                                         :log {1 {:index 1 :term 1}}
                                         :node-set #{::a ::b ::me}
                                         :commit-index 0}
                            :raft-leader-state {:match-index {::a 0}
                                                :next-index {::a 1}}
                            :io {:message {:type :append-entries-response
                                           :from ::a
                                           :success? true
                                           :last-log-index 1}}})
        {{messages :out-queue} :io} result]
    (is applied? result)
    (is (= 2 (::a (:next-index (:raft-leader-state result)))))
    (is (= 1 (::a (:match-index (:raft-leader-state result)))))))

(deftest t-leader-receive-command
  (let [[applied? result]
        (leader-receive-command
         {:io {:message {:type :operation,
                         :payload {:op :write,
                                   :key :hello,
                                   :value :world},
                         :operation-type :com.manigfeald.raft-test/bogon,
                         :serial #uuid "73315024-7517-4506-8428-275a8d1a6d84"},
               :out-queue PersistentQueue/EMPTY},
          :raft-state {:current-term 2N,
                       :voted-for 1,
                       :log {},
                       :commit-index 0N,
                       :last-applied 0N,
                       :node-type :leader,
                       :value #com.manigfeald.raft.core.MapValue{},
                       :votes 0, :leader-id 1, :node-set #{0 1 4 3 2}},
          :raft-leader-state {:next-index {},
                              :match-index {}},
          :id 2,
          :running-log PersistentQueue/EMPTY,
          :timer {:now 1401418884457, :next-timeout 1401418884545, :period 1005}
          :run-count 16530N})]
    (is applied?)
    (is (contains? (:log (:raft-state result)) 1N))))

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

     (in-ns 'com.manigfeald.raft.rules-test))
