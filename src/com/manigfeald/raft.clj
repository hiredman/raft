(ns com.manigfeald.raft
  "runs the raft algorithm one step at a time."
  (:import (clojure.lang PersistentQueue)))

(defprotocol API
  "The value that you want raft to maintain implements this protocol"
  (apply-read [value read-operation]
    "part of the API protocol, given a value and a read-operation,
    return the value that would be read")
  (apply-write [value write-operation]
    "part of the API protocol, given a write-operation returns an
    updated value with that write operation applied"))

;; TODO: refactor these states, have a top level Env defrecord
;; TODO: document log entry format
;; TODO: knossos
;; defrecords mainly just to document the expected fields
(defrecord RaftLeaderState [next-index match-index])
(defrecord RaftState [current-term voted-for log commit-index last-applied
                      node-type value votes leader-id node-set])
(defrecord State [in-queue out-queue raft-state raft-leader-state id
                  running-log applied])

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
  "return an init state when given a node id and a node-set"
  [id node-set]
  (->State PersistentQueue/EMPTY
           PersistentQueue/EMPTY
           (->RaftState 0 nil {} 0 0 :follower (->MapValue) 0 nil
                        (set node-set))
           (->RaftLeaderState {} {})
           id
           PersistentQueue/EMPTY
           #{}))

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

(defn broadcast
  "given a node-set and a msg, generate a seq of messages for every
  node in the node-set"
  [node-set msg]
  (for [node node-set]
    (assoc msg
      :target node)))

(defn consume-message
  "given a state pop the in-queue"
  [state]
  (update-in state [:in-queue] pop))

(defn publish
  "given a state and a collection of messages, add the messages to the
  out-queue"
  [state messages]
  (update-in state [:out-queue] into
             (for [message messages
                   :when (not= (:id state) (:target message))]
               message)))

(defmacro in->
  "given a map and a key, thread the value of the key in the map
  through steps, then assoc in to the map at the key"
  [m k & steps]
  `(let [m# ~m
         k# ~k]
     (assoc m#
       k# (-> (get m# k#) ~@steps))))

(defn log-trace
  "given a state and a log message (as a seq of strings) append the
  message to the log at the trace level"
  [state & message]
  {:pre [(instance? PersistentQueue (:running-log state))]
   :post [(instance? PersistentQueue (:running-log %))]}
  (update-in state [:running-log]
             (fnil conj PersistentQueue/EMPTY)
             {:level :trace
              :message (apply print-str message)}))

;; predicates

(defn received-message-of-type?
  "is the oldest message in the in-queue of the given type?"
  [in-queue type]
  (and (not (empty? in-queue))
       (= type (:type (peek in-queue)))))

(defn commit-index-newer-than-last-applied-and-exists-in-log?
  [commit-index last-applied log]
  (and (> commit-index last-applied)
       (contains? log commit-index)))

(defn message-has-later-term? [in-queue current-term]
  (and (not (empty? in-queue))
       (number? (:term (peek in-queue)))
       (> (:term (peek in-queue)) current-term)))

(defn candidate-or-follower-timed-out-waiting-for-message? [in-queue node-type]
  (and (received-message-of-type? in-queue :timeout)
       (or (= node-type :candidate)
           (= node-type :follower))))

(defn recv-vote-request-and-havent-voted-yet? [in-queue voted-for node-type]
  (and (received-message-of-type? in-queue :request-vote)
       (or (nil? voted-for)
           (= voted-for (:candidate-id (peek in-queue))))
       (or (= node-type :candidate)
           (= node-type :follower))))

(defn recv-vote-request-and-alread-voted? [in-queue node-type voted-for]
  (and (received-message-of-type? in-queue :request-vote)
       (and (not (nil? voted-for))
            (not= voted-for (:candidate-id (peek in-queue))))
       (or (= node-type :candidate)
           (= node-type :follower)
           (= node-type :leader))))

(defn recv-vote-for-this-node? [in-queue]
  (and (received-message-of-type? in-queue :request-vote-response)
       (:vote? (peek in-queue))))

(defn this-node-voted-in-to-power? [votes node-set]
  (and (number? votes)
       (>= votes (Math/ceil (/ (count node-set) 2)))
       #_(= node-type :candidate)))

(defn should-send-heart-beat? [node-type in-queue]
  (and (= node-type :leader)
       (received-message-of-type? in-queue :timeout)))

;;; Raft Rules

(def keep-up-commited
  "if a log entry has been commited but hasn't been applied to the
  value, apply it"
  (rule
   (commit-index-newer-than-last-applied-and-exists-in-log?
    commit-index last-applied log)
   (-> state
       (update-in [:raft-state] advance-applied-to-commit)
       (as-> new-state
             (update-in
              new-state
              [:applied]
              into
              (for [[index entry] (:log (:raft-state new-state))
                    :when (:serial entry)
                    :when (> index (:last-applied (:raft-state state)))
                    :when (>= (:last-applied (:raft-state new-state))
                              index)]
                entry))))
   {{:keys [commit-index last-applied log]} :raft-state :as state}))

(def jump-to-new-term
  "if you see a message from a term greater than your current term,
  jump on that band wagon"
  (rule
   (message-has-later-term? in-queue current-term)
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
   (candidate-or-follower-timed-out-waiting-for-message? in-queue node-type)
   (do
     (assert (instance? PersistentQueue (:running-log state)))
     (-> state
         (log-trace id "a" node-type "requests a vote for" (inc current-term))
         (consume-message)
         (publish
          (broadcast node-set
                     {:type :request-vote
                      :term (inc current-term)
                      :candidate-id id
                      :last-log-index (last-log-index raft-state)
                      :last-log-term (last-log-term raft-state)
                      :from id}))
         (in-> :raft-state
               (assoc :term (inc current-term)
                      :node-type :candidate
                      :voted-for id
                      :votes 0))))
   {:keys [id in-queue raft-state]
    {:keys [current-term node-type node-set]} :raft-state
    :as state}))

(def respond-to-vote-request-success
  (rule
   (recv-vote-request-and-havent-voted-yet? in-queue voted-for node-type)
   (-> state
       (consume-message)
       (in-> :raft-state (assoc :voted-for (:candidate-id (peek in-queue))))
       (publish [{:type :request-vote-response
                  :target (:candidate-id (peek in-queue))
                  :term current-term
                  :vote? true
                  :from id}]))
   {:as state
    :keys [id in-queue]
    {:keys [voted-for node-type current-term]} :raft-state}))

(def respond-to-vote-request-failure
  (rule
   (recv-vote-request-and-alread-voted? in-queue node-type voted-for)
   (-> state
       (log-trace id "doesn't vote for" (:candidate-id (peek in-queue)))
       (consume-message)
       (publish [{:type :request-vote-response
                  :target (:candidate-id (peek in-queue))
                  :term current-term
                  :vote? false
                  :from id}]))
   {:as state
    :keys [id in-queue]
    {:keys [voted-for node-type current-term]} :raft-state}))

(def receive-vote-success
  (rule
   (recv-vote-for-this-node? in-queue)
   (-> state
       (log-trace (:from (peek in-queue))
                  "voted for" (:id state) "in"
                  (:current-term (:raft-state state)))
       (consume-message)
       (update-in [:raft-state :votes] inc))
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
       (consume-message))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def become-leader
  (rule
   (this-node-voted-in-to-power? votes node-set)
   (-> state
       (log-trace (:id state) "becomes leader in term"
                  current-term "with" votes "votes")
       (publish (broadcast
                 node-set
                 {:type :append-entries
                  :term current-term
                  :leader-id id
                  :prev-log-index (last-log-index raft-state)
                  :prev-log-term (last-log-term raft-state)
                  :entries []
                  :from id
                  :leader-commit commit-index}))
       (in-> :raft-state
             (assoc :node-type :leader
                    :voted-for nil
                    :votes 0
                    :leader-id id))
       (update-in [:raft-leader-state]
                  merge {:next-index
                         (into {} (for [node node-set]
                                    [node (inc (last-log-index raft-state))]))
                         :match-index
                         (into {} (for [node node-set]
                                    [node 0]))}))
   {:as state
    :keys [id]
    {:keys [votes current-term commit-index node-type node-set]
     :as raft-state} :raft-state}))

(def heart-beat
  (rule
   (should-send-heart-beat? node-type in-queue)
   (-> state
       (consume-message)
       (publish
        (broadcast node-set
                   {:type :append-entries
                    :term current-term
                    :leader-id id
                    :prev-log-index (last-log-index raft-state)
                    :prev-log-term (last-log-term raft-state)
                    :entries []
                    :from id
                    :leader-commit commit-index})))
   {:as state
    :keys [id in-queue]
    {:keys [current-term commit-index node-type node-set]
     :as raft-state} :raft-state}))

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
        (received-message-of-type? in-queue :append-entries)
        (number? (:term (peek in-queue)))
        (> current-term (:term (peek in-queue))))
   (-> state
       (consume-message)
       (publish [{:target (:leader-id (peek in-queue))
                  :type :append-entries-response
                  :term current-term
                  :success? false
                  :from id}]))
   {:as state
    :keys [in-queue id]
    {:keys [node-type current-term]} :raft-state}))

(def reject-append-entries-with-unknown-prevs
  (rule
   (and (= node-type :follower)
        (received-message-of-type? in-queue :append-entries)
        (and (not (zero? (:prev-log-term (peek in-queue))))
             (not (log-contains? raft-state
                                 (:prev-log-term (peek in-queue))
                                 (:prev-log-index (peek in-queue))))))
   (-> state
       (log-trace "rejecting append-entries with unknown prevs" (peek in-queue)
                  (keys (:log raft-state)))
       (consume-message)
       (publish [{:target (:leader-id (peek in-queue))
                  :type :append-entries-response
                  :term current-term
                  :success? false
                  :from id}]))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term]} :raft-state}))

(def skip-append-entries
  (rule
   (and (= node-type :follower)
        (received-message-of-type? in-queue :append-entries)
        (or (zero? (:prev-log-term (peek in-queue)))
            (log-contains? raft-state
                           (:prev-log-term (peek in-queue))
                           (:prev-log-index (peek in-queue))))
        (every? #(log-contains? raft-state (:term (peek in-queue)) (:index %))
                (:entries (peek in-queue))))
   (-> state
       (consume-message)
       (in-> :raft-state
             (assoc :voted-for nil
                    :leader-id (:leader-id (peek in-queue)))
             (set-commit (:leader-commit (peek in-queue))))
       (as-> state
             (publish state [{:target (:leader-id (peek in-queue))
                              :type :append-entries-response
                              :term current-term
                              :success? true
                              :last-index (last-log-index (:raft-state state))
                              :from id}])))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term voted-for]} :raft-state}))

(def accept-append-entries
  (rule
   (and (= node-type :follower)
        (received-message-of-type? in-queue :append-entries)
        (or (zero? (:prev-log-term (peek in-queue)))
            (log-contains? raft-state
                           (:prev-log-term (peek in-queue))
                           (:prev-log-index (peek in-queue))))
        (not (every? #(log-contains? raft-state (:term (peek in-queue))
                                     (:index %))
                     (:entries (peek in-queue)))))
   (-> state
       (consume-message)
       (in-> :raft-state
             (assoc :voted-for nil
                    :leader-id (:leader-id (peek in-queue)))
             (clear-log-after (:prev-log-term (peek in-queue)))
             (append-log (:entries (peek in-queue)))
             (set-commit (:leader-commit (peek in-queue))))
       (as-> state
             (publish state [{:target (:leader-id (peek in-queue))
                              :type :append-entries-response
                              :term current-term
                              :success? true
                              :last-index (last-log-index (:raft-state state))
                              :from id}])))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term voted-for]} :raft-state}))

(def handle-unsuccessful-append-entries
  (rule
   (and (received-message-of-type? in-queue :append-entries-response)
        (not (:success? (peek in-queue)))
        (not (zero? (get next-index (:from (peek in-queue))))))
   (-> state
       (log-trace "unsuccessful append entries" (seq in-queue) state)
       (consume-message)
       (update-in [:raft-leader-state :next-index (:from (peek in-queue))]
                  dec))
   {:as state
    :keys [in-queue]
    {:keys [next-index]} :raft-leader-state
    {:keys [node-type]} :raft-state}))

(def handle-successful-append-entries
  (rule
   (and (= node-type :leader)
        (received-message-of-type? in-queue :append-entries-response)
        (:success? (peek in-queue)))
   (do
     (assert (>= (:last-index (peek in-queue))
                 (get-in state [:raft-leader-state :match-index
                                (:from (peek in-queue))])))
     (-> state
         (consume-message)
         (assoc-in [:raft-leader-state :match-index (:from (peek in-queue))]
                   (:last-index (peek in-queue)))
         (assoc-in [:raft-leader-state :next-index (:from (peek in-queue))]
                   (inc (:last-index (peek in-queue))))))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def drop-append-entry-responses-from-previous-terms
  (rule
   (and (number? (:term (peek in-queue)))
        (received-message-of-type? in-queue :append-entries-response)
        (> current-term (:term (peek in-queue))))
   (-> state
       (consume-message))
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
     (-> state
         (update-in [:raft-state :next-index]
                    merge (into {} (for [[node-id next-index] next-index
                                         :when (>= (last-log-index raft-state)
                                                   next-index)]
                                     [node-id (inc next-index)])))
         (publish (for [[node-id next-index] next-index
                        :when (>= (last-log-index raft-state) next-index)
                        :when (get log next-index)]
                    {:target node-id
                     :type :append-entries
                     :term current-term
                     :leader-id id
                     :prev-log-index (max 0 (dec next-index))
                     :prev-log-term
                     (or (:term (get log (max 0 (dec next-index)))) 0)
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
   (let [ns (sort (map second match-index))
         quorum-count (Math/ceil (/ (count match-index) 2.0))
         n (apply max 0 (take quorum-count ns))]
     (-> state
         (log-trace "advance-commit" n (get (:log (:raft-state state)) n))
         (update-in [:raft-state] assoc :commit-index n)))
   {:as state
    {:keys [match-index]} :raft-leader-state
    {:keys [commit-index current-term log node-type]} :raft-state}))

(def leader-accept-operations
  (rule
   (and (not (empty? in-queue))
        (= :operation (:type (peek in-queue)))
        (= :leader node-type)
        (not (seq (for [[index entry] log
                        :when (= (:serial (peek in-queue))
                                 (:serail entry))]
                    entry))))
   (let [ts (System/currentTimeMillis)]
     (assert (contains? (peek in-queue) :op) (peek in-queue))
     (assert (contains? #{:read :write :add-node :remove-node}
                        (:operation-type (peek in-queue)))
             (peek in-queue))
     (-> state
         (log-trace "new index" (inc (last-log-index raft-state)))
         (consume-message)
         (in-> :raft-state
               (append-log [(assoc (peek in-queue)
                              :term current-term
                              :timestamp ts
                              :index
                              (inc (last-log-index raft-state)))]))
         (publish (for [[node-id next-index] next-index]
                    {:target node-id
                     :type :append-entries
                     :term current-term
                     :leader-id id
                     :prev-log-index (last-log-index raft-state)
                     :prev-log-term (last-log-term raft-state)
                     :entries
                     [(assoc (peek in-queue)
                        :term current-term
                        :timestamp ts
                        :index (inc (last-log-index raft-state)))]
                     :from id
                     :leader-commit commit-index}))))
   {:as state
    :keys [in-queue id raft-state]
    {:keys [next-index]} :raft-leader-state
    {:keys [node-type current-term commit-index log]} :raft-state}))

(def follower-forward-operations-when-leader
  (rule
   (and (not (empty? in-queue))
        (= :operation (:type (peek in-queue)))
        (not= :leader node-type)
        leader-id
        (not (seq (for [[index entry] log
                        :when (= (:serial (peek in-queue))
                                 (:serail entry))]
                    entry))))
   (-> state
       (consume-message)
       (publish [(assoc (peek in-queue)
                   :target leader-id)]))
   {:as state
    :keys [in-queue id raft-state]
    {:keys [next-index]} :raft-leader-state
    {:keys [node-type current-term commit-index log leader-id]} :raft-state}))

(def follower-skip-operations-no-leader
  (rule
   (and (not (empty? in-queue))
        (= :operation (:type (peek in-queue)))
        (not= :leader node-type)
        (nil? leader-id)
        (not (seq (for [[index entry] log
                        :when (= (:serial (peek in-queue))
                                 (:serail entry))]
                    entry))))
   (-> state
       (consume-message))
   {:as state
    :keys [in-queue id raft-state]
    {:keys [next-index]} :raft-leader-state
    {:keys [node-type current-term commit-index log leader-id]} :raft-state}))

(def leader-skip-accepted-operation
  "if the leader receives an operation with a serial id already in the
  log, ignore it"
  (rule
   (and (not (empty? in-queue))
        (= :operation (:type (peek in-queue)))
        (= :leader node-type)
        (seq (for [[index entry] log
                   :when (= (:serial (peek in-queue))
                            (:serail entry))]
               entry)))
   (-> state
       (consume-message))
   {:as state
    :keys [in-queue]
    {:keys [node-type log]} :raft-state}))

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
   #'leader-accept-operations
   #'leader-skip-accepted-operation
   #'follower-forward-operations-when-leader
   #'follower-skip-operations-no-leader])

(defn step
  "given a raft state, run the machine forward until no more progress
  can be made, return the new state"
  [raft-state]
  (reduce
   (fn [raft-state rule]
     (assert (instance? PersistentQueue (:running-log raft-state)))
     (if-let [result (rule raft-state)]
       (do
         (assert (instance? PersistentQueue
                            (:running-log raft-state)))
         result)
       raft-state))
   raft-state
   raft-rules))

(defn run-one
  "given a state of raft and an input message, step the machine to a
  new state, returning it"
  [state message]
  (assert (instance? PersistentQueue (:running-log state)))
  (let [node-set (:node-set (:raft-state state))]
    (if (= :stop (:type message))
      (-> state
          (log-trace "stopping" (:id state))
          (assoc :stopped? true))
      (let [state (update-in state [:in-queue] conj message)
            new-state (step state)]
        (assert (set? (:node-set (:raft-state new-state))))
        (assert (>= (count (:node-set (:raft-state state)))
                    (count (:node-set (:raft-state new-state)))))
        (assert (instance? PersistentQueue (:running-log state)))
        (assert (not= (:in-queue state)
                      (:in-queue new-state))
                (pr-str new-state (seq (:in-queue new-state))))
        (assert (>= (:commit-index (:raft-state new-state))
                    (:commit-index (:raft-state state))))
        (cond-> new-state
                (not= (:current-term (:raft-state state))
                      (:current-term (:raft-state new-state)))
                (log-trace (:id state) "changed terms"
                           (:current-term (:raft-state state)) "=>"
                           (:current-term (:raft-state new-state)))
                (not= (:commit-index (:raft-state state))
                      (:commit-index (:raft-state new-state)))
                (log-trace (:id state) "changed commits from"
                           (:commit-index (:raft-state state))
                           "to"
                           (:commit-index (:raft-state new-state)))
                (not= (:node-set (:raft-state state))
                      (:node-set (:raft-state new-state)))
                (log-trace "node-set" (:id new-state)
                           (:node-set (:raft-state new-state))))))))
