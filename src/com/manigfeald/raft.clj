(ns com.manigfeald.raft
  "runs the raft algorithm one step at a time."
  (:require [com.manigfeald.raft.core :refer :all]
            [com.manigfeald.raft.rules :refer [rules-of-raft]])
  (:import (clojure.lang PersistentQueue)))

;; TODO: document log entry format
;; TODO: knossos
;; TODO: strict mode
;; defrecords mainly just to document the expected fields
(defrecord RaftLeaderState [next-index match-index])
(defrecord RaftState [current-term voted-for log commit-index last-applied
                      node-type value votes leader-id node-set])
(defrecord IO [message out-queue])
(defrecord Timer [now next-timeout period])
(defrecord ImplementationState [io raft-state raft-leader-state id running-log
                                timer])

(alter-meta! #'map->RaftLeaderState assoc :no-doc true)
(alter-meta! #'map->RaftState assoc :no-doc true)
(alter-meta! #'map->IO assoc :no-doc true)
(alter-meta! #'map->Timer assoc :no-doc true)
(alter-meta! #'map->ImplementationState assoc :no-doc true)

(defn raft
  "return an init state when given a node id and a node-set"
  [id node-set timer]
  (->ImplementationState
   (->IO nil PersistentQueue/EMPTY)
   (->RaftState 0N
                nil
                (empty-log)
                0N
                0N
                :follower
                (->MapValue)
                0N
                nil
                (set node-set))
   (->RaftLeaderState {} {})
   id
   PersistentQueue/EMPTY
   timer))

(defn run-one [raft-state]
  {:post [(not (seq (for [message (:out-queue (:io %))
                          :when (= (:type message) :request-vote-response)
                          :when (:success? message)
                          :when (not= (:voted-for (:raft-state %))
                                      (:target message))]
                      message)))
          #_(>= (log-count (:raft-state %))
                (log-count (:raft-state raft-state)))
          (or (zero? (:last-applied (:raft-state %)))
              (contains? (log-entry-of (:raft-state %) (:last-applied (:raft-state %))) :return))]}
  (let [[applied? new-state] (rules-of-raft raft-state)
        r (as-> new-state new-state
                (cond-> new-state
                        (and (not (zero? (:last-applied (:raft-state new-state))))
                             (not (contains? (log-entry-of (:raft-state new-state)
                                                           (:last-applied (:raft-state new-state)))
                                             :return)))
                        ((fn [x]
                           (locking #'*out*
                             (prn x))
                           x))
                        (not= (:node-type (:raft-state new-state))
                              (:node-type (:raft-state raft-state)))
                        (log-trace
                         (:node-type (:raft-state raft-state))
                         "=>"
                         (:node-type (:raft-state new-state))
                         (:run-count new-state))
                        ;; (not= (:votes (:raft-state new-state))
                        ;;       (:votes (:raft-state raft-state)))
                        ;; (log-trace "votes"
                        ;;            (:votes (:raft-state new-state))
                        ;;            (:run-count new-state))
                        (not= (:current-term (:raft-state new-state))
                              (:current-term (:raft-state raft-state)))
                        (log-trace "current-term"
                                   (:current-term (:raft-state new-state))
                                   (:run-count new-state))
                        (not= (:commit-index (:raft-state new-state))
                              (:commit-index (:raft-state raft-state)))
                        (log-trace "commit index"
                                   (:commit-index (:raft-state new-state))
                                   (:run-count new-state)))
                (update-in new-state [:run-count] (fnil inc 0N)))]
    (assert (not (seq (for [message (:out-queue (:io r))
                            :when (= (:type message) :request-vote-response)
                            :when (:success? message)
                            :when (not= (:voted-for (:raft-state r))
                                        (:target message))]
                        message)))
            (pr-str
             (update-in r [:io :out-queue] seq)))
    r))
