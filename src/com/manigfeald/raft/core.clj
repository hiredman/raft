(ns com.manigfeald.raft.core
  (:require [com.manigfeald.raft.log :as log])
  (:import (clojure.lang PersistentQueue)))

(defprotocol RaftOperations
  "The value that you want raft to maintain implements this protocol"
  (apply-operation [value operation]))

(defrecord MapValue []
  RaftOperations
  (apply-operation [value operation]
    (case (:op operation)
      :read [(get value (:key operation)) value]
      :write [nil (assoc value
                    (:key operation) (:value operation))]
      :write-if [nil (if (contains? value (:key operation))
                       value
                       (assoc value
                         (:key operation) (:value operation)))]
      :delete [nil (dissoc value (:key operation))]
      (assert nil operation))))

(declare log-entry-of
         insert-entries)

(defn set-return-value [raft-state index value]
  (let [entry (log-entry-of raft-state index)]
    (insert-entries raft-state [(assoc entry :return value)])))

;; TODO: move add and remove node in to its own code
(defn advance-applied-to-commit
  "given a RaftState, ensure all commited operations have been applied
  to the value"
  [raft-state]
  (if (> (:commit-index raft-state)
         (:last-applied raft-state))
    (let [new-last (inc (:last-applied raft-state))
          ;; op (get (:log raft-state) new-last)
          op (log-entry-of raft-state new-last)]
      (assert op "op is in the log")
      (case (:operation-type op)
        :add-node (recur (-> raft-state
                             (assoc :last-applied new-last)
                             (update-in [:node-set] conj (:node op))))
        :remove-node (recur (-> raft-state
                                (assoc :last-applied new-last)
                                (update-in [:node-set] disj (:node op))))
        (let [[return new-value] (apply-operation (:value raft-state)
                                                  (:payload op))
              new-state (assoc-in raft-state [:log (:index op) :return] return)
              new-state2 (set-return-value raft-state (:index op) return)]
          (assert (= new-state new-state2) [new-state new-state2])
          (recur (assoc new-state
                   :value new-value
                   :last-applied new-last)))))
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
  (apply max 0N (keys (:log raft-state))))

(defn last-log-term [raft-state]
  {:post [(number? %)
          (not (neg? %))]}
  (biginteger
   (let [idx (last-log-index raft-state)]
     (if (zero? idx)
       0N
       (:term (get (:log raft-state) idx))))))

(defn broadcast [node-set msg]
  (for [node node-set]
    (assoc msg :target node)))

(defn enough-votes? [total votes]
  (>= votes (inc (Math/floor (/ total 2.0)))))

(defn possible-new-commit
  [commit-index raft-state match-index node-set current-term]
  (first (sort (for [[n c] (frequencies (for [[index entry] (:log raft-state)
                                              [node match-index] match-index
                                              :when (>= match-index index)
                                              :when (= current-term
                                                       (:term entry))
                                              :when (> index commit-index)]
                                          index))
                     :when (>= c (inc (Math/floor (/ (count node-set) 2))))]
                 n))))

(defn log-trace
  "given a state and a log message (as a seq of strings) append the
  message to the log at the trace level"
  [state & message]
  {:pre [(instance? PersistentQueue (:running-log state))]
   :post [(instance? PersistentQueue (:running-log %))]}
  (update-in state [:running-log]
             (fnil conj PersistentQueue/EMPTY)
             {:level :trace
              :message (apply print-str (:id state) message)}))

(defn serial-exists? [raft-state serial]
  (first (for [[_ entry] (:log raft-state)
               :when (= (:serial entry) serial)]
           entry)))

(defn add-to-log [raft-state operation]
  {:pre [(contains? operation :operation-type)
         (contains? operation :payload)
         (contains? operation :term)
         (number? (:term operation))
         (not (neg? (:term operation)))]
   :post [(= 1 (count (for [[_ entry] (:log %)
                            :when (= (:serial entry) (:serial operation))]
                        entry)))]}
  (if (serial-exists? raft-state (:serial operation))
    raft-state
    (assoc-in raft-state [:log (inc (last-log-index raft-state))]
              (assoc operation :index (inc (last-log-index raft-state))))))

(defn insert-entries [raft-state entries]
  (assoc raft-state
    :log (reduce
          #(log/add-to-log %1 (:index %2) %2)
          (:log raft-state)
          entries)))

(defn rewrite-terms [raft-state target-index new-term]
  (assoc-in raft-state [:log]
            (into {} (for [[index entry] (:log raft-state)]
                       (if (> index target-index)
                         [index (assoc entry :term new-term)]
                         [index entry])))))


(defn log-entry-of [raft-state index]
  (log/log-entry-of (:log raft-state) index))

(defn log-count [raft-state]
  (log/log-count (:log raft-state)))

(defn empty-log []
  {})

(defn reject-append-entries [state leader-id current-term id]
  (-> state
      (consume-message)
      (publish [{:type :append-entries-response
                 :term current-term
                 :success? false
                 :from id
                 :target leader-id}])
      (assoc-in [:timer :next-timeout] (+ (-> state :timer :period)
                                          (-> state :timer :now)))))

(defn accept-append-entries [state leader-id current-term id]
  (-> state
      (consume-message)
      (publish [{:type :append-entries-response
                 :target leader-id
                 :term current-term
                 :success? true
                 :from id
                 :last-log-index (last-log-index (:raft-state state))}])
      (assoc-in [:raft-state :leader-id] leader-id)
      (assoc-in [:timer :next-timeout] (+ (-> state :timer :period)
                                          (-> state :timer :now)))))
