(ns com.thelastcitadel.raft
  (:require [clojure.core.async :refer [alt!! timeout >!]])
  (:import (java.util UUID)))

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
    (assert (contains? cluster node))
    (>! (get cluster node) msg)))

(defn broadcast [cluster msg]
  (doseq [node (list-nodes cluster)]
    (send-to cluster node msg)))

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
  (to-follower [_ term])
  (prepare-heartbeat [_]))

(defprotocol API
  (delete [_ key])
  (write [_ key value])
  (read-op [_ key])
  (write-if [_ key value]))

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
                     :write-if (if (contains? value (first args))
                                 value
                                 (assoc value (first args) (second args)))))
                 (:value raft-state)
                 (:entries op))
          state (assoc raft-state
                  :last-applied new-last
                  :value value)]
      (recur state))
    raft-state))

(defn last-log-index [raft-state]
  (apply max 0 (keys raft-state)))

(defn last-log-term [raft-state]
  (:term (get (:log raft-state) (last-log-index raft-state))))

(defn log-contains? [raft-state log-index log-term]
  (and (contains? (:log raft-state) log-index)
       (= log-term (get (:log raft-state) log-index))))

(defn set-commit [raft-state leader-commit]
  (if (> leader-commit (:commit-index raft-state))
    (assoc raft-state
      :commit-index (min leader-commit (last-log-index raft-state)))
    raft-state))

(defn append-log [raft-state entries]
  (update-in raft-state [:log] (merge (into {} (for [entry entries] [(:index entry) entry])))))

(defn clear-log-after [raft-state log-index]
  (let [next-index (inc log-index)]
    (if (contains? (:log raft-state) next-index)
      (recur (update-in raft-state [:log] dissoc next-index) next-index)
      raft-state)))

(declare ->Candidate)

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
    [(->Candidate raft-state 0)
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

(declare ->Leader)

(defrecord Candidate [raft-state votes]
  Raft
  (maintain [node]
    (->Candidate (maintain-raft-state raft-state)))
  (leader? [_] false)
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
    (append-entries (to-follower node term) term leader-id prev-log-index prev-log-term entries leader-commit)))

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
  (receive-append-entries-response [leader term success? follower current-index]
    (if success?
      (->Leader raft-state
                (-> raft-leader-state
                    (assoc-in [:next-index follower] (inc current-index))
                    (assoc-in [:match-index follower] current-index)))
      leader))
  (prepare-heartbeat [_]
    {:term (:term raft-state)
     :prev-log-index (last-log-index raft-state)
     :prev-log-term (last-log-term raft-state)
     :entries []
     :leader-commit (:commit-index raft-state)})
  API
  (delete [_ key]
    (let [op {:op :delete
              :args [key]
              :index (inc (last-log-index rift-state))}]
      [(->Leader (-> raft-state
                     (append-entries [op])))
       op]))
  (write [_ key value]
    (let [op {:op :write
              :args [key value]
              :index (inc (last-log-index rift-state))}]
      [(->Leader (-> raft-state
                     (append-entries [op])))
       op]))
  (read-op [_ key]
    (let [op {:op :read
              :args [key]
              :index (inc (last-log-index rift-state))}]
      [(->Leader (-> raft-state
                     (append-entries [op])))
       op]))
  (write-if [_ key value]
    (let [op {:op :write-if
              :args [key value]
              :index (inc (last-log-index rift-state))}]
      [(->Leader (-> raft-state
                     (append-entries [op])))
       op])))

(def heart-beat-time-out 500)

(defn run [node in cluster id]
  (let [to (if (leader? node)
             (* 0.75 heart-beat-time-out)
             heart-beat-time-out)]
    (alt!!
     in ([message]
           (if (> (:term message) (:term (:raft-state node)))
             #(run (to-follower node (:term message)) in cluster id)
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
                         (broadcast cluster (assoc (prepare-heartbeat node)
                                              :from id
                                              :leader-id id
                                              :type :append-entries))
                         #(run node in cluster id))
                       (let [[new-node request] (prepare-request-vote node id)]
                         (broadcast cluster (assoc request
                                              :from id
                                              :type :request-vote))
                         #(run new-node in cluster id)))))))
