(ns com.manigfeald.raft.core)

(defprotocol RaftOperations
  "The value that you want raft to maintain implements this protocol"
  (apply-read [value read-operation]
    "part of the API protocol, given a value and a read-operation,
    return the value that would be read")
  (apply-write [value write-operation]
    "part of the API protocol, given a write-operation returns an
    updated value with that write operation applied"))

(defrecord MapValue []
  RaftOperations
  (apply-read [value operation]
    (case (:op operation)
      :read (get value (:key operation))))
  (apply-write [value operation]
    (case (:op operation)
      :write (assoc value
               (:key operation) (:value operation))
      :write-if (if (contains? value (:key operation))
                  value
                  (assoc value
                    (:key operation) (:value operation)))
      :delete (dissoc value (:key operation)))))

(defn advance-applied-to-commit
  "given a RaftState, ensure all commited operations have been applied
  to the value"
  [raft-state]
  (if (> (:commit-index raft-state)
         (:last-applied raft-state))
    (let [new-last (inc (:last-applied raft-state))
          op (get (:log raft-state) new-last)]
      (assert op "op is in the log")
      (case (:operation-type op)
        :read (let [read-value (apply-read (:value raft-state) op)
                    new-state (assoc-in raft-state [:log (:index op) :value]
                                        read-value)]
                (recur (assoc new-state
                         :last-applied new-last)))
        :write (recur (assoc raft-state
                        :last-applied new-last
                        :value (apply-write (:value raft-state) op)))
        :add-node (recur (-> raft-state
                             (assoc :last-applied new-last)
                             (update-in [:node-set] conj (:node op))))
        :remove-node (recur (-> raft-state
                                (assoc :last-applied new-last)
                                (update-in [:node-set] disj (:node op))))))
    raft-state))

(defn consume-message [state]
  (assoc-in state [:io :message] nil))

(defn publish [state messages]
  {:pre [(not (map? messages))
         (every? :from messages)]}
  (update-in state [:io :out-queue] into
             (for [message messages
                   :when (not= (:target message) (:id state))]
               message)))

(defn log-contains? [raft-state log-term log-index]
  (or (and (zero? log-term)
           (zero? log-index))
      (and (contains? (:log raft-state) log-index)
           (= log-term (:term (get (:log raft-state) log-index))))))

(defn last-log-index [raft-state]
  (apply max 0 (keys (:log raft-state))))

(defn last-log-term [raft-state]
  (let [idx (last-log-index raft-state)]
    (if (zero? idx)
      0
      (:term (get (:log raft-state) idx)))))

(defn broadcast [node-set msg]
  (for [node node-set]
    (assoc msg :target node)))

(defn enough-votes? [total votes]
  (>= votes (inc (Math/floor (/ total 2.0)))))

(defn possible-new-commit [commit-index raft-state match-index node-set current-term]
  (first (sort (for [[n c] (frequencies (for [n (range commit-index (inc (last-log-index raft-state)))
                                              [node match-index] match-index
                                              :when (>= match-index n)
                                              :when (= current-term (or (:term (get (:log raft-state) n)) 0))]
                                          n))
                     :when (>= c (inc (Math/floor (/ (count node-set) 2.0))))]
                 n))))
