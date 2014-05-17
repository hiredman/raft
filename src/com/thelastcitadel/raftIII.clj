(ns com.thelastcitadel.raftIII
  (:require [clojure.core.async :refer [alt!! timeout >!]]
            [clojure.core.match :refer [match]])
  (:import (java.util UUID)))

(defrecord RaftState [term voted-for log commit-index last-applied value])
(defrecord LeaderState [next-index match-index])

(defprotocol Raft
  (maintain [_])
  (leader? [_])
  (prepare-request-vote [_ candidate-id])
  (request-vote [_ term candidate-id last-log-index last-log-term])
  (append-entries [_ term leader-id prev-log-index prev-log-term entries leader-commit])
  (receive-vote-response [_ term vote-granted? node])
  (receive-append-entries-response [_ term success? node current-index])
  (to-follower [_ term]))

;; {:op :delete :args []}
;; {:op :write :args ["foo" "bar"]}
;; {:op :read}
;; {:op :write-if :args ["foo" "bar"]}

(defn maintain-raft-state [raft-state]
  (if (> (:commit-index raft-state)
         (:last-applied raft-state))
    (let [new-last (inc (:last-applied raft-state))
          op (get (:log raft-state) new-last)
          value (reduce
                 (fn [value {:keys [op args]}]
                   (case op
                     :delete (dissoc value (first args))
                     :write (assoc value (first args) (second args))
                     :read value
                     :write-if (if (contains? state (first args))
                                 state
                                 (assoc value (first args) (second args)))))
                 (:value raft-state)
                 (:entries op))
          state (assoc raft-state
                  :last-applied new-last
                  :value value)]
      (recur state))
    raft-state))

(defrecord Follower [raft-state]
  Raft
  (maintain [_]
    (->Follower (maintain-raft-state raft-state)))
  (to-follower [_ term]
    (->Follower (assoc raft-state
                  :voted-for nil
                  :term term)))
  (leader? [_] false)
  (prepare-request-vote [_ candidate-id]
    [(->Candidate raft-state votes)
     {:term (:term raft-state)
      :candidate-id candidate-id
      :last-log-index (last-log-index raft-state)
      :last-log-term (last-log-term raft-state)}])
  (request-vote [_ term candidate-id last-log-index last-log-term]
    (cond
     (< term (:term raft-state))
     [(->Follower raft-state) {:term (:term raft-state)
                               :vote-granted? false}]
     (and (or (not (:voted-for raft-state))
              (= candidate-id (:voted-for raft-state)))
          (log-contains? raft-state last-log-index last-log-term))
     [(->Follower raft-state) {:term (:term raft-state)
                               :vote-granted? true}]
     :else
     [(->Follower raft-state) {:term (:term raft-state)
                               :vote-granted? false}]))
  (append-entries [_ term leader-id prev-log-index prev-log-term entries leader-commit]
    (cond
     (< term (:term raft-state))
     [(->Follower raft-state) {:term (:term raft-state)
                               :success? false}]
     (not (log-contains? raft-state prev-log-index prev-log-term))
     [(->Follower raft-state) {:term (:term raft-state)
                               :success? false}]
     :else
     [(->Follower (-> raft-state
                      (clear-log-after prev-log-index)
                      (append-log entries)
                      (set-commit leader-commit)))
      {:term (:term raft-state)
       :current-index (last-log-index raft-state)
       :success? true}])))

(defrecord Candidate [raft-state votes]
  Raft
  (maintain [node]
    (->Candidate (maintain-raft-state raft-state)))
  (leader [_] false)
  (to-follower [_ term]
    (->Follower (assoc raft-state
                  :voted-for nil
                  :term term)))
  (prepare-request-vote [node candidate-id]
    [node
     {:term (:term raft-state)
      :candidate-id candidate-id
      :last-log-index (last-log-index raft-state)
      :last-log-term (last-log-term raft-state)}])
  (receive-vote-response [node term vote-granted? nodes]
    (let [votes (if vote-granted?
                  (inc votes)
                  votes)]
      (if (> 2 (/ nodes votes))
        (->Leader raft-state
                  (->LeaderState (into {} (for [node nodes]
                                            [node (last-log-index raft-state)]))
                                 (into {} (for [node nodes]
                                            [node 0])))
                  false)
        (->Candidate raft-state votes))))
  (request-vote [_ term candidate-id last-log-index last-log-term]
    (cond
     (< term (:term raft-state))
     [(->Candidate raft-state) {:term (:term raft-state)
                                :vote-granted? false}]
     (and (or (not (:voted-for raft-state))
              (= candidate-id (:voted-for raft-state)))
          (log-contains? raft-state last-log-index last-log-term))
     [(->Candidate raft-state) {:term (:term raft-state)
                                :vote-granted? true}]
     :else
     [(->Candidate raft-state) {:term (:term raft-state)
                                :vote-granted? false}]))
  (append-entries [node term leader-id prev-log-index prev-log-term entries leader-commit]
    (append-entries (to-follower node) term leader-id prev-log-index prev-log-term entries leader-commit)))

(defrecord Leader [raft-state raft-leader-state asserted?]
  Raft
  (maintain [_]
    (let [new-raft-state (maintain-raft-state raft-state)
          ns (sort (map second (:match-index raft-leader-state)))
          n (apply max (take (dec (/ (count (:match-index raft-leader-state)) 2)) ns))]
      (if (and (> n (:commit-index new-raft-state))
               (= (:term new-raft-state)) (:term (get (:log new-raft-state) n)))
        (->Leader (assoc new-raft-state
                    :commit-index n)
                  raft-leader-state)
        (->Leader new-raft-state raft-leader-state))))
  (to-follower [_ term]
    (->Follower (assoc raft-state
                  :voted-for nil
                  :term term)))
  (leader? [_] true)
  (request-vote [_ term candidate-id last-log-index last-log-term]
    [(->Leader raft-state) {:term (:term raft-state)
                            :vote-granted? false}])
  (append-entries [_ term leader-id prev-log-index prev-log-term entries leader-commit]
    [(->Leader raft-state) {:term (:term raft-state)
                            :success? false}])
  (receive-append-entries-response [leader term success? follower current-index]
    (if success?
      (->Leader raft-state
                (-> raft-leader-state
                    (assoc-in [:next-index follower] (inc current-index))
                    (assoc-in [:match-index follower] current-index)))
      leader)))


(defn run [node in cluster id]
  (let [to (if (leader? node)
             (* 0.75 heart-beat-time-out)
             heart-beat-time-out)]
    (alts!!
     in ([message]
           (if (> (:term message) (:term (:raft-state node)))
             #(run (to-follower node) in cluster id)
             (case (:type message)
               :request-vote (let [[new-node result] (request-vote node
                                                                   (:term message)
                                                                   (:candidate-id message)
                                                                   (:last-log-index message)
                                                                   (:last-log-term message))]
                               (send-to cluster (:candidate-id message) (assoc result
                                                                          :from id
                                                                          :type :request-vote-response))
                               #(run new-node in cluster id))
               :request-vote-response (let [new-node (receive-vote-response node
                                                                            (:term message)
                                                                            (:vote-granted? message)
                                                                            (list-nodes cluster))]
                                        #(run new-node in cluster id))
               :append-entries (let [[new-node result] (append-entries node
                                                                       (:term message)
                                                                       (:leader-id message)
                                                                       (:prev-log-index message)
                                                                       (:prev-log-term message)
                                                                       (:entries message)
                                                                       (:leader-commit message))]
                                 (send-to cluster (:leader-id message) (assoc result
                                                                         :from id
                                                                         :type :append-entries-response))
                                 #(run new-node in cluster id))
               :append-entries-response (let [new-node (receive-append-entries-response node
                                                                                        (:term message)
                                                                                        (:success? message)
                                                                                        (:from message)
                                                                                        (:current-index message))]
                                          #(run new-node in cluster id)))))
     (timeout to) ([_]
                     (if (leader? node)
                       (do
                         (broadcast cluster (assoc (prepare-heartbeat node id)
                                              :from id
                                              :type :append-entries))
                         #(run node in cluster id))
                       (do
                         (broadcast cluster (assoc (prepare-request-vote node id)
                                              :from id
                                              :type :request-vote))
                         #(run node in cluster id)))))))
