(ns com.thelastcitadel.raft
  "runs the raft algorithm one step at a time."
  (:require [clojure.tools.logging :as log]))

;; TODO: this may not belong here, see about simplifying
(defprotocol Cluster
  (add-node [cluster node arg1])
  (remove-node [cluster node])
  (list-nodes [cluster])
  (send-to [cluster node msg]))

(defprotocol API
  "The value that you want raft to maintain implements this protocol"
  (apply-read [value read-operation]
    "part of the API protocol, given a value and a read-operation,
    return the value that would be read")
  (apply-write [value write-operation]
    "part of the API protocol, given a write-operation returns an
    updated value with that write operation applied"))

;; TODO: desugar this in the machine
(defn broadcast
  "send a message to all the nodes in a cluster"
  [cluster msg]
  (doseq [node (list-nodes cluster)]
    (send-to cluster node msg)))

;; defrecords mainly just to document the expected fields
(defrecord RaftLeaderState [next-index match-index])
(defrecord RaftState [current-term voted-for log commit-index last-applied
                      node-type value votes leader-id])
(defrecord State [in-queue out-queue raft-state raft-leader-state id cluster
                  waiters])

(defrecord MapValue []
  API
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

(defn raft
  "return an init state when given a node id and a cluster"
  [id cluster]
  (->State clojure.lang.PersistentQueue/EMPTY
           clojure.lang.PersistentQueue/EMPTY
           (->RaftState 0 nil {} 0 0 :follower (->MapValue) 0 nil)
           (->RaftLeaderState {} {})
           id
           cluster
           {}))

;; TODO: try using core.match to combine condition and binding?
(defmacro rule
  "a rule is a function that returns either nil or an updated version
  of whatever was passed in. the rule macro takes a condition, a
  change, and a binding. if the condition is true the result of the
  change is returned, other wise nil. the binding is like the
  arguments to a function and you can destructure, etc as normal"
  [condition change binding]
  `(fn [& args#]
     (let [~binding (first args#)]
       (when ~condition
         ~change))))

(defn advance-applied-to-commit
  "given a RaftState, ensure all commited operations have been applied
  to the value"
  [raft-state]
  (if (> (:commit-index raft-state)
         (:last-applied raft-state))
    (let [new-last (inc (:last-applied raft-state))
          op (get (:log raft-state) new-last)]
      (if (= :read (:operation-type op))
        (let [read-value (apply-read (:value raft-state) op)
              new-state (assoc-in raft-state [:log (:index op) :value] read-value)]
          (recur (assoc new-state
                   :last-applied new-last)))
        (recur (assoc raft-state
                 :last-applied new-last
                 :value (apply-write (:value raft-state) op)))))
    raft-state))

(defn last-log-index [raft-state]
  {:pre [(contains? raft-state :log)]}
  (apply max 0 (keys (:log raft-state))))

(defn last-log-term [raft-state]
  (or (:term (get (:log raft-state) (last-log-index raft-state))) 0))

(defn log-contains? [raft-state log-term log-index]
  (and (contains? (:log raft-state) log-index)
       (= log-term (:term (get (:log raft-state) log-index)))))

(defn append-log [raft-state entries]
  {:post [(map? (:log %))]}
  (assert (every? (comp number? :index) entries) entries)
  (update-in raft-state [:log]
             merge (into {} (for [entry entries] [(:index entry) entry]))))

(defn clear-log-after [raft-state log-index]
  (let [next-index (inc log-index)]
    (if (contains? (:log raft-state) next-index)
      (recur (update-in raft-state [:log] dissoc next-index) next-index)
      raft-state)))

(defn set-commit [raft-state leader-commit]
  (if (> leader-commit (:commit-index raft-state))
    (assoc raft-state
      :commit-index (min leader-commit (last-log-index raft-state)))
    raft-state))

(defn entries-and-callbacks [waiters log commit-index]
  (for [[serial callbacks] waiters
        [index entry] log
        :when (= serial (:serial entry))
        :when (>= commit-index index)]
    [entry callbacks]))

;;; Raft Rules

(def keep-up-commited
  "if a log entry has been commited but hasn't been applied to the
  value, apply it"
  (rule
   (and (> commit-index last-applied)
        (contains? log commit-index))
   (update-in state [:raft-state] advance-applied-to-commit)
   {{:keys [commit-index last-applied log]} :raft-state :as state}))

(def jump-to-new-term
  "if you see a message from a term greater than your current term,
  jump on that band wagon"
  (rule
   (and (not (empty? in-queue))
        (number? (:term (peek in-queue)))
        (> (:term (peek in-queue)) current-term))
   (-> state
       (update-in [:raft-state] merge {:node-type :follower
                                       :voted-for nil
                                       :votes 0
                                       :current-term (:term (peek in-queue))}))
   {{:keys [current-term]} :raft-state
    :keys [in-queue] :as state}))

(def new-election-on-timeout
  "if a candidate or follower receives a timeout message, ask for
  votes in a new term"
  (rule
   (and (not (empty? in-queue))
        (= :timeout (:type (peek in-queue)))
        (or (= node-type :candidate)
            (= node-type :follower)))
   (do
     (log/trace id "a" node-type "requests a vote for" (inc current-term))
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:out-queue]
                    conj {:type :request-vote
                          :target :broadcast
                          :term (inc current-term)
                          :candidate-id id
                          :last-log-index (last-log-index raft-state)
                          :last-log-term (last-log-term raft-state)
                          :from id})
         (update-in [:raft-state] merge {:term (inc current-term)
                                         :node-type :candidate
                                         :voted-for id
                                         :votes 0})))
   {:keys [id in-queue raft-state]
    {:keys [current-term node-type]} :raft-state
    :as state}))

(def respond-to-vote-request-success
  (rule
   (and (not (empty? in-queue))
        (= :request-vote (:type (peek in-queue)))
        (or (nil? voted-for)
            (= voted-for (:candidate-id (peek in-queue))))
        (or (= node-type :candidate)
            (= node-type :follower)))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:raft-state]
                  merge {:voted-for (:candidate-id (peek in-queue))})
       (update-in [:out-queue] conj {:type :request-vote-response
                                     :target (:candidate-id (peek in-queue))
                                     :term current-term
                                     :vote? true
                                     :from id}))
   {:as state
    :keys [id in-queue]
    {:keys [voted-for node-type current-term]} :raft-state}))

(def respond-to-vote-request-failure
  (rule
   (and (not (empty? in-queue))
        (= :request-vote (:type (peek in-queue)))
        (and (not (nil? voted-for))
             (not= voted-for (:candidate-id (peek in-queue))))
        (or (= node-type :candidate)
            (= node-type :follower)
            (= node-type :leader)))
   (do
     (log/trace id "doesn't vote for" (:candidate-id (peek in-queue)))
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:out-queue] conj {:type :request-vote-response
                                       :target (:candidate-id (peek in-queue))
                                       :term current-term
                                       :vote? false
                                       :from id})))
   {:as state
    :keys [id in-queue]
    {:keys [voted-for node-type current-term]} :raft-state}))

(def receive-vote-success
  (rule
   (and (not (empty? in-queue))
        (= :request-vote-response (:type (peek in-queue)))
        (:vote? (peek in-queue)))
   (do
     (log/trace (:from (peek in-queue))
                "voted for" (:id state) "in"
                (:current-term (:raft-state state)))
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:raft-state :votes] inc)))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def receive-vote-failure
  (rule
   (and (not (empty? in-queue))
        (= :request-vote-response (:type (peek in-queue)))
        (not (:vote? (peek in-queue)))
        #_(= node-type :candidate))
   (-> state
       (update-in [:in-queue] pop))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def become-leader
  (rule
   (and (number? votes)
        (> votes 0)
        (> 2 (/ (inc (count (list-nodes cluster))) votes))
        #_(= node-type :candidate))
   (do
     (log/trace (:id state) "becomes leader in term"
                current-term "with" votes "votes")
     (-> state
         (update-in [:out-queue]
                    conj {:target :broadcast
                          :type :append-entries
                          :term current-term
                          :leader-id id
                          :prev-log-index (last-log-index raft-state)
                          :prev-log-term (last-log-term raft-state)
                          :entries []
                          :from id
                          :leader-commit commit-index})
         (update-in [:raft-state] merge {:node-type :leader
                                         :voted-for nil
                                         :votes 0
                                         :leader-id id})
         (update-in [:raft-leader-state]
                    merge {:next-index
                           (into {} (for [node (list-nodes cluster)]
                                      [node (inc (last-log-index raft-state))]))
                           :match-index
                           (into {} (for [node (list-nodes cluster)]
                                      [node 0]))})))
   {:as state
    :keys [cluster id]
    {:keys [votes current-term commit-index node-type]
     :as raft-state} :raft-state}))

(def heart-beat
  (rule
   (and (= node-type :leader)
        (not (empty? in-queue))
        (= :timeout (:type (peek in-queue))))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:out-queue] conj {:target :broadcast
                                     :type :append-entries
                                     :term current-term
                                     :leader-id id
                                     :prev-log-index (last-log-index raft-state)
                                     :prev-log-term (last-log-term raft-state)
                                     :entries []
                                     :from id
                                     :leader-commit commit-index}))
   {:as state
    :keys [id in-queue]
    {:keys [current-term commit-index node-type] :as raft-state} :raft-state}))

(def follow-the-leader
  (rule
   (and (= :append-entries (:type (peek in-queue)))
        (= node-type :candidate))
   (update-in state [:raft-state] merge {:node-type :follower
                                         :voted-for nil
                                         :votes 0})
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def reject-append-entries-from-old-leaders
  (rule
   (and (= node-type :follower)
        (not (empty? in-queue))
        (= :append-entries (:type (peek in-queue)))
        (number? (:term (peek in-queue)))
        (> current-term (:term (peek in-queue))))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:out-queue] conj {:target (:leader-id (peek in-queue))
                                     :type :append-entries-response
                                     :term current-term
                                     :success? false
                                     :from id}))
   {:as state
    :keys [in-queue id]
    {:keys [node-type current-term]} :raft-state}))

(def reject-append-entries-with-unknown-prevs
  (rule
   (and (= node-type :follower)
        (not (empty? in-queue))
        (= :append-entries (:type (peek in-queue)))
        (and (not (zero? (:prev-log-term (peek in-queue))))
             (not (log-contains? raft-state
                                 (:prev-log-term (peek in-queue))
                                 (:prev-log-index (peek in-queue))))))
   (do
     (log/trace "rejecting append-entries with unknown prevs" (peek in-queue) (keys (:log raft-state)))
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:out-queue] conj {:target (:leader-id (peek in-queue))
                                       :type :append-entries-response
                                       :term current-term
                                       :success? false
                                       :from id})))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term]} :raft-state}))

(def skip-append-entries
  (rule
   (and (= node-type :follower)
        (not (empty? in-queue))
        (= :append-entries (:type (peek in-queue)))
        (or (zero? (:prev-log-term (peek in-queue)))
            (log-contains? raft-state
                           (:prev-log-term (peek in-queue))
                           (:prev-log-index (peek in-queue))))
        (every? #(log-contains? raft-state (:term (peek in-queue)) (:index %))
                (:entries (peek in-queue))))
   (-> state
       (update-in [:in-queue] pop)
       (assoc-in [:raft-state :voted-for] nil)
       (assoc-in [:raft-state :leader-id] (:leader-id (peek in-queue)))
       (update-in [:raft-state] set-commit (:leader-commit (peek in-queue)))
       (as-> state
             (update-in state [:out-queue]
                        conj {:target (:leader-id (peek in-queue))
                              :type :append-entries-response
                              :term current-term
                              :success? true
                              :last-index (last-log-index (:raft-state state))
                              :from id})))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term voted-for]} :raft-state}))

(def accept-append-entries
  (rule
   (and (= node-type :follower)
        (not (empty? in-queue))
        (= :append-entries (:type (peek in-queue)))
        (or (zero? (:prev-log-term (peek in-queue)))
            (log-contains? raft-state
                           (:prev-log-term (peek in-queue))
                           (:prev-log-index (peek in-queue))))
        (not (every? #(log-contains? raft-state (:term (peek in-queue)) (:index %))
                     (:entries (peek in-queue)))))
   (-> state
       (update-in [:in-queue] pop)
       (assoc-in [:raft-state :voted-for] nil)
       (assoc-in [:raft-state :leader-id] (:leader-id (peek in-queue)))
       (update-in [:raft-state] clear-log-after (:prev-log-term (peek in-queue)))
       (update-in [:raft-state] append-log (:entries (peek in-queue)))
       (update-in [:raft-state] set-commit (:leader-commit (peek in-queue)))
       (as-> state
             (update-in state [:out-queue]
                        conj {:target (:leader-id (peek in-queue))
                              :type :append-entries-response
                              :term current-term
                              :success? true
                              :last-index (last-log-index (:raft-state state))
                              :from id})))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term voted-for]} :raft-state}))

(def handle-unsuccessful-append-entries
  (rule
   (and (not (empty? in-queue))
        (= :append-entries-response (:type (peek in-queue)))
        (not (:success? (peek in-queue)))
        (not (zero? (get next-index (:from (peek in-queue))))))
   (do
     (log/trace "unsuccessful append entries" (seq in-queue) state)
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:raft-leader-state :next-index (:from (peek in-queue))] dec)))
   {:as state
    :keys [in-queue]
    {:keys [next-index]} :raft-leader-state
    {:keys [node-type]} :raft-state}))

(def handle-successful-append-entries
  (rule
   (and (= node-type :leader)
        (not (empty? in-queue))
        (= :append-entries-response (:type (peek in-queue)))
        (:success? (peek in-queue)))
   (do
     (assert (>= (:last-index (peek in-queue))
                 (get-in state [:raft-leader-state :match-index (:from (peek in-queue))])))
     (-> state
         (update-in [:in-queue] pop)
         (assoc-in [:raft-leader-state :match-index (:from (peek in-queue))]
                   (:last-index (peek in-queue)))
         (assoc-in [:raft-leader-state :next-index (:from (peek in-queue))]
                   (inc (:last-index (peek in-queue))))))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def drop-append-entry-responses-from-previous-terms
  (rule
   (and (not (empty? in-queue))
        (= :append-entries-response (:type (peek in-queue)))
        (number? (:term (peek in-queue)))
        (> current-term (:term (peek in-queue))))
   (-> state
       (update-in [:in-queue] pop))
   {:as state
    :keys [in-queue]
    {:keys [current-term]} :raft-state}))

(def advance-laggy-followers
  (rule
   (and (= node-type :leader)
        ;; there are lagged followers
        (seq (for [[node-id next-index] next-index
                   :when (>= (last-log-index raft-state) next-index)
                   :when (get log next-index)]
               true))
        (empty? out-queue))
   (let [min-next (apply min (vals next-index))]
     ;; (log/trace "advance laggy followers to" next-index)
     (-> state
         (update-in [:raft-state :next-index]
                    merge (into {} (for [[node-id next-index] next-index
                                         :when (>= (last-log-index raft-state) next-index)]
                                     [node-id (inc next-index)])))
         (update-in [:out-queue]
                    into (for [[node-id next-index] next-index
                               :when (>= (last-log-index raft-state) next-index)
                               :when (get log next-index)]
                           {:target node-id
                            :type :append-entries
                            :term current-term
                            :leader-id id
                            :prev-log-index (max 0 (dec next-index))
                            :prev-log-term (or (:term (get log (max 0 (dec next-index)))) 0)
                            :entries [(get log next-index)]
                            :from id
                            :leader-commit commit-index}))))
   {:as state
    :keys [out-queue id]
    {:keys [node-type commit-index log current-term] :as raft-state} :raft-state
    {:keys [next-index]} :raft-leader-state}))

(def advance-commit
  (rule
   (and (= :leader node-type)
        (let [ns (sort (map second match-index))
              quorum-count (Math/ceil (/ (count match-index) 2.0))
              n (apply max 0 (take quorum-count ns))]
          (and (> n commit-index)
               (= current-term (:term (get log n))))))
   (do
     (log/trace "advance-commit")
     (let [ns (sort (map second match-index))
           quorum-count (Math/ceil (/ (count match-index) 2.0))
           n (apply max 0 (take quorum-count ns))]
       (update-in state [:raft-state] assoc :commit-index n)))
   {:as state
    {:keys [match-index]} :raft-leader-state
    {:keys [commit-index current-term log node-type]} :raft-state}))

(def raft-remove-node
  (rule
   (and (not (empty? in-queue))
        (= :remove-node (:type (peek in-queue))))
   (do
     (log/trace "removing" (:node (peek in-queue)) "from" id "'s cluster")
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:cluster] remove-node (:node (peek in-queue)))))
   {:as state
    :keys [in-queue id]}))

(def leader-accept-operations
  (rule
   (and (not (empty? in-queue))
        (= :operation (:type (peek in-queue)))
        (= :leader node-type))
   (do
     (log/trace "new index" (inc (last-log-index raft-state)))
     (assert (contains? (peek in-queue) :op) (peek in-queue))
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:raft-state]
                    append-log [(assoc (peek in-queue)
                                  :term current-term
                                  :index
                                  (inc (last-log-index raft-state)))])
         (update-in [:out-queue]
                    into (for [[node-id next-index] next-index]
                           {:target node-id
                            :type :append-entries
                            :term current-term
                            :leader-id id
                            :prev-log-index (last-log-index raft-state)
                            :prev-log-term (last-log-term raft-state)
                            :entries [(assoc (peek in-queue)
                                        :term current-term
                                        :index (inc (last-log-index raft-state)))]
                            :from id
                            :leader-commit commit-index}))))
   {:as state
    :keys [in-queue id raft-state]
    {:keys [next-index]} :raft-leader-state
    {:keys [node-type current-term commit-index]} :raft-state}))

(def add-commit-waiters
  (rule
   (and (not (empty? in-queue))
        (= :await (:type (peek in-queue))))
   (do
     (log/trace "add-commit-waiters")
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:waiters (:serial (peek in-queue))]
                    conj (:callback (peek in-queue)))))
   {:as state
    :keys [in-queue id raft-state]
    {:keys [node-type current-term]} :raft-state}))

(def notify-commit-waiters
  (rule
   (seq (entries-and-callbacks waiters log last-applied))
   (do
     (log/trace "notify-commit-waiters")
     (let [entries-and-callbacks (entries-and-callbacks waiters log commit-index)
           callbacks-to-remove (map :serial (map first entries-and-callbacks))]
       (doseq [[entry callbacks] entries-and-callbacks
               callback callbacks]
         (log/trace "callback" callback "for" (:serial entry) "with" (:value entry) entry)
         (callback (:serial entry) (:value entry)))
       (assoc state
         :waiters (apply dissoc waiters callbacks-to-remove))))
   {:as state
    :keys [waiters]
    {:keys [commit-index log last-applied]} :raft-state}))

;;;

(def raft-rules
  "list of rules for raft"
  [#'keep-up-commited
   #'jump-to-new-term
   #'new-election-on-timeout
   #'respond-to-vote-request-success
   #'respond-to-vote-request-failure
   #'receive-vote-success
   #'receive-vote-failure
   #'become-leader
   #'heart-beat
   #'follow-the-leader
   #'reject-append-entries-from-old-leaders
   #'reject-append-entries-with-unknown-prevs
   #'skip-append-entries
   #'accept-append-entries
   #'handle-unsuccessful-append-entries
   #'handle-successful-append-entries
   #'advance-laggy-followers
   #'advance-commit
   #'drop-append-entry-responses-from-previous-terms
   #'raft-remove-node
   #'leader-accept-operations
   #'add-commit-waiters
   #'notify-commit-waiters])

(defn step
  "given a raft state, run the machine forward until no more progress
  can be made, return the new state"
  [raft-state]
  (let [new-raft-state (reduce
                        (fn [raft-state rule]
                          (if-let [result (rule raft-state)]
                            result
                            raft-state))
                        raft-state
                        raft-rules)]
    (if (= raft-state new-raft-state)
      new-raft-state
      (recur new-raft-state))))

(defn run-one
  "given a state of raft and an input message, step the machine to a
  new state, returning it"
  [state message]
  (let [cluster (:cluster state)]
    (if (= :stop (:type message))
      (do
        (log/trace "stopping" (:id state))
        (-> state
            (assoc :stopped? true)
            (update-in [:out-queue] conj {:term 0
                                          :target :broadcast
                                          :from (:id state)
                                          :type :remove-node
                                          :node (:id state)})))
      (let [state (update-in state [:in-queue] conj message)
            new-state (step state)]
        (assert (not= (:in-queue state)
                      (:in-queue new-state))
                (pr-str new-state (seq (:in-queue new-state))))
        (when (not= (:current-term (:raft-state state))
                    (:current-term (:raft-state new-state)))
          (log/trace (:id state) "changed terms"
                     (:current-term (:raft-state state)) "=>"
                     (:current-term (:raft-state new-state))))
        (when (not= (:commit-index (:raft-state state))
                    (:commit-index (:raft-state new-state)))
          (assert (> (:commit-index (:raft-state new-state))
                     (:commit-index (:raft-state state))))
          (log/trace (:id state) "changed commits from"
                     (:commit-index (:raft-state state))
                     "to"
                     (:commit-index (:raft-state new-state))))
        new-state))))
