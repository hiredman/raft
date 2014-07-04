(ns com.manigfeald.raft.rules
  "rules are a combination of a head which is a predicate and a body
that updates and returns the passed in state. when a rule is applied
to a value, if the head returns true for a given value that value is
give to the body, and the updated value is returned, if the head
returns false the passed in value is returned unmodified.

syntactically rules have a third component, which is the name/binding
form that the value will be bound to in both the head and the body."
  (:require [com.manigfeald.raft.core :refer :all]))

(defrecord Rule [head body]
  clojure.lang.IFn
  (invoke [this arg]
    (if (head arg)
      (let [r (binding [*log-context* (:name this)]
                (body arg))]
        (assert (or (zero? (:last-applied (:raft-state r)))
                    (contains? (get (:log (:raft-state r))
                                    (:last-applied (:raft-state r)))
                               :return))
                (with-out-str
                  (prn "rule" this)
                  (println "before:")
                  (prn arg)
                  (println "after:")
                  (prn r)
                  (println)))
        (assert (>= (:commit-index (:raft-state r))
                    (:last-applied (:raft-state r)))
                (with-out-str
                  (prn "rule" this)
                  (println "before:")
                  (prn arg)
                  (println "after:")
                  (prn r)
                  (println)))
        [true r #_(update-in r [:applied-rules] conj this)])
      [false arg])))

(defrecord CompoundRule [head subrules]
  clojure.lang.IFn
  (invoke [_ arg]
    (reduce
     (fn [[applied? arg] rule]
       (if (head arg)
         (let [[new-applied? new-arg] (rule arg)]
           [(or applied? new-applied?) new-arg])
         (reduced [applied? arg])))
     [false arg]
     subrules)))

(alter-meta! #'->CompoundRule assoc :no-doc true)
(alter-meta! #'->Rule assoc :no-doc true)
(alter-meta! #'map->CompoundRule assoc :no-doc true)
(alter-meta! #'map->Rule assoc :no-doc true)

(defmacro rule
  "a rule is a combination of some way to decide if the rule
  applies (the head) and some way to apply the rule (the body)"
  ([head body bindings]
     (if-let [line (:line (meta &form))]
       `(rule ~(str "line-" line)
              ~head ~body ~bindings)
       `(->Rule (fn [~bindings]
                  ~head)
                (fn [~bindings]
                  ~body))))
  ([name head body bindings]
     `(assoc (->Rule (fn ~(symbol (str (clojure.core/name name) "-head"))
                       [~bindings]
                       ~head)
                     (fn ~(symbol (str (clojure.core/name name) "-body"))
                       [~bindings]
                       ~body))
        :name ~name)))

(defmacro guard
  "a guard is a rule with a normal rule head, but the body is a
  composition of other rules"
  [head & things]
  (let [rules (butlast things)
        bindings (last things)]
    `(->CompoundRule (fn [~bindings]
                       ~head)
                     ~(vec rules))))

;;
(def keep-up-apply
  (rule
   (and (> commit-index last-applied)
        ;; only apply when the latest thing to apply is from the
        ;; current term, this deals with split writes
        (seq (for [[k v] (:log raft-state)
                   :when (= current-term (:term v))
                   :when (>= commit-index (:index v))]
               true)))
   (-> state
       (update-in [:raft-state] advance-applied-to-commit))
   {:as state
    {:keys [commit-index last-applied current-term]
     :as raft-state} :raft-state}))

(def jump-to-newer-term
  (rule
   (and message-term
        (> message-term current-term))
   (-> state
       (log-trace "jump-to-newer-term" message-term)
       (update-in [:raft-state] merge {:node-type :follower
                                       :current-term message-term
                                       :leader-id nil
                                       :votes 0N
                                       :voted-for nil}))
   {:as state
    :keys [id]
    {{message-term :term} :message} :io
    {:keys [current-term]} :raft-state}))
;;

(def follower-respond-to-append-entries-with-wrong-term
  (rule
   :fail-old-term
   (> current-term message-term)
   (-> state
       (reject-append-entries leader-id current-term id)
       (log-trace "rejecting append entries from" leader-id))
   {:as state
    :keys [id]
    {{leader-id :leader-id
      message-term :term} :message} :io
    {:keys [current-term node-type]} :raft-state}))

(def follower-respond-to-append-entries-with-unknown-prev
  (rule
   :fail-missing-prevs
   (and (= current-term message-term)
        (not (log-contains? raft-state prev-log-term prev-log-index)))
   (-> state
       (log-trace "rejecting append entries from" leader-id)
       (reject-append-entries leader-id current-term id))
   {:as state
    :keys [id]
    {{prev-log-index :prev-log-index
      prev-log-term :prev-log-term
      leader-id :leader-id
      message-term :term} :message} :io
    {:keys [current-term node-type] :as raft-state} :raft-state}))

(def follower-respond-to-append-entries
  (guard
   (and (= :follower node-type)
        (= message-type :append-entries))
   #'follower-respond-to-append-entries-with-wrong-term
   #'follower-respond-to-append-entries-with-unknown-prev
   (rule
    :good
    (and (= message-term current-term)
         (log-contains? raft-state prev-log-term prev-log-index))
    (-> state
        ((fn [s]
           (assert (not (seq
                         (for [[k v] (:log (:raft-state s))
                               :when (>= (:last-applied (:raft-state s)) k)
                               :when (not (contains? v :return))]
                           v)))
                   (pr-str ["before before"
                            (:last-applied (:raft-state s))
                            (for [[k v] (:log (:raft-state s))
                                  :when (>= (:last-applied (:raft-state s)) k)
                                  :when (not (contains? v :return))]
                              v)]))
           s))
        ((fn [s]
           (let [rs (with-meta (:raft-state s)
                      {:id id
                       :message-term message-term
                       :current-term current-term
                       :leader-id leader-id})]
             (assoc s
               :raft-state (insert-entries rs entries)))))
        #_(update-in [:raft-state] insert-entries entries)
        ((fn [s]
           (assert (not (seq
                         (for [[k v] (:log (:raft-state s))
                               :when (>= (:last-applied (:raft-state s)) k)
                               :when (not (contains? v :return))]
                           v)))
                   (pr-str ["before"
                            (:last-applied (:raft-state s))
                            (for [[k v] (:log (:raft-state s))
                                  :when (>= (:last-applied (:raft-state s)) k)
                                  :when (not (contains? v :return))]
                              v)]))
           s))
        (accept-append-entries leader-id current-term id)
        ((fn [s]
           (assert (not (seq
                         (for [[k v] (:log (:raft-state s))
                               :when (>= (:last-applied (:raft-state s)) k)
                               :when (not (contains? v :return))]
                           v)))
                   (pr-str ["after"
                            (:last-applied (:raft-state s))
                            (for [[k v] (:log (:raft-state s))
                                  :when (>= (:last-applied (:raft-state s)) k)
                                  :when (not (contains? v :return))]
                              v)]))
           s))
        (as-> n
              (if (> leader-commit commit-index)
                (assoc-in n [:raft-state :commit-index]
                          (min leader-commit
                               (last-log-index (:raft-state n))))
                n)))
    {:as state
     :keys [id]
     {{prev-log-index :prev-log-index
       prev-log-term :prev-log-term
       message-term :term
       entries :entries
       leader-id :leader-id
       leader-commit :leader-commit} :message} :io
     {:keys [current-term node-type commit-index] :as raft-state} :raft-state})
   {{{message-type :type
      :as message} :message} :io
      {:keys [node-type]} :raft-state}))

(def follower-respond-to-request-vote
  (guard
   (and (= :follower node-type)
        (= message-type :request-vote))
   (rule
    :success
    (and (or (= voted-for candidate-id)
             (nil? voted-for))
         (>= last-log-index (com.manigfeald.raft.core/last-log-index
                             raft-state))
         (>= last-log-term (com.manigfeald.raft.core/last-log-term raft-state))
         (or (log-contains? raft-state last-log-term last-log-index)
             (nil? (log-entry-of raft-state last-log-index))))
    (-> state
        (log-trace "votes for" candidate-id "in" current-term)
        (consume-message)
        (assoc-in [:raft-state :voted-for] candidate-id)
        (publish [{:type :request-vote-response
                   :target candidate-id
                   :term current-term
                   :success? true
                   :from id}]))
    {:as state
     :keys [id]
     {{message-type :type
       candidate-id :candidate-id
       :keys [last-log-index last-log-term] :as message} :message} :io
     {:keys [current-term node-type voted-for] :as raft-state} :raft-state})
   (rule
    (not (and (or (= voted-for candidate-id)
                  (nil? voted-for))
              (or (log-contains? raft-state last-log-term last-log-index)
                  (and (:term (log-entry-of raft-state last-log-index))
                       (> last-log-term
                          (:term (log-entry-of raft-state last-log-index)))))))
    (-> state
        (log-trace "doesn't vote for" candidate-id "in" current-term
                   ;; {:last-log-index last-log-index
                   ;;  :last-log-term last-log-term
                   ;;  :voted-for voted-for}
                   ;; (com.manigfeald.raft.log/indices-and-terms
                   ;;  (:log (:raft-state state)))
                   )
        (consume-message)
        (publish [{:type :request-vote-response
                   :target candidate-id
                   :term current-term
                   :success? false
                   :from id}]))
    {:as state
     :keys [id]
     {{message-type :type
       candidate-id :candidate-id
       :keys [last-log-index last-log-term]
       :as message} :message} :io
     {:keys [current-term node-type voted-for] :as raft-state} :raft-state})
   {{{message-type :type} :message} :io
    {:keys [node-type]} :raft-state}))

(def become-candidate-and-call-for-election
  (rule
   (and (= node-type :follower)
        (>= now next-timeout))
   (-> state
       (log-trace "call for election")
       (consume-message)
       (update-in [:raft-state] merge
                  {:node-type :candidate
                   :votes 1
                   :voted-for id
                   :current-term (inc current-term)
                   :leader-id nil})
       (assoc-in [:timer :next-timeout]
                 (+ now period))
       (update-in [:io :out-queue] empty)
       (publish (broadcast
                 node-set
                 {:type :request-vote
                  :candidate-id id
                  :last-log-index (last-log-index raft-state)
                  :last-log-term (last-log-term raft-state)
                  :term (inc current-term)
                  :from id})))
   {:as state
    :keys [id]
    {:keys [now period next-timeout]} :timer
    {:keys [current-term node-set node-type] :as raft-state} :raft-state}))
;;

(def candidate-receive-votes
  (guard
   (and (= node-type :candidate)
        (= message-type :request-vote-response)
        success?)
   (rule
    (enough-votes? (count node-set) (inc votes))
    (-> state
        (consume-message)
        (log-trace "received vote from" from "in" current-term)
        (log-trace "becoming leader with" (inc votes) "in" current-term)
        (update-in [:raft-state] merge {:node-type :leader
                                        :votes 0N
                                        :voted-for nil
                                        :leader-id id})
        (update-in [:raft-leader-state] merge
                   {:match-index
                    (into {} (for [node node-set]
                               [node 0N]))
                    :next-index
                    (into {} (for [node node-set]
                               [node (inc (last-log-index raft-state))]))})
        (assoc-in [:timer :next-timeout] (+ now period))
        ;; TODO: this is a departure from strict raft
        #_(update-in [:raft-state] rewrite-terms commit-index current-term)
        (publish (broadcast node-set
                            {:type :append-entries
                             :term current-term
                             :leader-id id
                             :prev-log-index (last-log-index raft-state)
                             :prev-log-term (last-log-term raft-state)
                             :entries []
                             :leader-commit commit-index
                             :from id})))
    {:as state
     :keys [id]
     {{:keys [success? from]} :message} :io
     {:keys [now period]} :timer
     {:keys [votes node-set current-term commit-index]
      :as raft-state} :raft-state})
   (rule
    (not (enough-votes? (count node-set) (inc votes)))
    (-> state
        (consume-message)
        (log-trace "received vote from" from "in" current-term)
        (update-in [:raft-state :votes] inc))
    {:as state
     :keys [id]
     {{:keys [from]} :message} :io
     {:keys [votes node-set current-term]} :raft-state})
   {{{:keys [success?]
      message-type :type} :message} :io
      {:keys [node-type]} :raft-state}))

(def candidate-respond-to-append-entries
  (rule
   (and (= node-type :candidate)
        (= message-type :append-entries))
   (-> state
       (log-trace "candidate-respond-to-append-entries")
       (consume-message)
       (update-in [:raft-state] merge {:node-type :follower
                                       :voted-for leader-id
                                       :votes 0N})
       (assoc-in [:timer :next-timeout] (+ period now)))
   {:as state
    {{message-type :type
      leader-id :leader-id} :message} :io
    {:keys [node-type]} :raft-state
    {:keys [period now]} :timer}))

(def candidate-election-timeout
  (rule
   (and (= node-type :candidate)
        (>= now next-timeout))
   (-> state
       (log-trace "candidate-election-timeout")
       (consume-message)
       (update-in [:raft-state] merge
                  {:node-type :candidate
                   :votes 1
                   :voted-for id
                   :current-term (inc current-term)})
       (assoc-in [:timer :next-timeout]
                 (+ now period))
       (publish (broadcast
                 node-set
                 {:type :request-vote
                  :candidate-id id
                  :last-log-index (last-log-index raft-state)
                  :last-log-term (last-log-term raft-state)
                  :term (inc current-term)
                  :from id})))
   {:as state
    :keys [id]
    {:keys [now next-timeout period]} :timer
    {:keys [node-type current-term node-set] :as raft-state} :raft-state}))
;;
(def heart-beat
  (rule
   (and (= node-type :leader)
        (>= now next-timeout))
   (-> state
       (publish (broadcast node-set
                           {:type :append-entries
                            :term current-term
                            :leader-id id
                            :prev-log-index (last-log-index raft-state)
                            :prev-log-term (last-log-term raft-state)
                            :entries []
                            :leader-commit commit-index
                            :from id})))
   {:as state
    :keys [id]
    {:keys [now next-timeout]} :timer
    {:keys [node-type node-set current-term commit-index] :as raft-state}
    :raft-state}))

;; TODO: need a test to ensure that the leaders last-log-index is
;; considered when deciding to advance the commit index
(def leader-receive-command
  (rule
   (and (= node-type :leader)
        (= message-type :operation))
   (-> state
       (log-trace "received command serial" (:serial message))
       ;; (log-trace (:raft-leader-state state))
       ;; (log-trace "commit-index" commit-index)
       (consume-message)
       (update-in [:raft-state] add-to-log (assoc message
                                             :term current-term))
       (as-> state
             (assoc-in state [:raft-leader-state :match-index id]
                       (last-log-index (:raft-state state)))))
   {:as state
    :keys [id]
    {{:as message message-type :type} :message} :io
    {:keys [match-index]} :raft-leader-state
    {:keys [current-term node-type commit-index node-set]
     :as raft-state} :raft-state}))

;; TODO: rate limit this rule
(def update-followers
  (rule
   (and (= :leader node-type)
        (seq (for [[node next-index] next-index
                   :when (>= (last-log-index raft-state) next-index)]
               node)))
   (-> state
       ;; (log-trace "update followers")
       (publish (for [[node next-index] next-index
                      :when (not= node id)
                      :when (>= (last-log-index raft-state) next-index)]
                  {:type :append-entries
                   :target node
                   :term current-term
                   :leader-id id
                   :prev-log-index (max 0N (dec next-index))
                   :prev-log-term (or (:term (log-entry-of raft-state
                                                           (dec next-index)))
                                      0N)
                   :entries (for [index (range (max 0N (dec next-index))
                                               (inc next-index))
                                  :when (not (zero? index))]
                              (dissoc (log-entry-of raft-state index) :return))
                   :leader-commit commit-index
                   :from id})))
   {:as state
    :keys [id]
    {:keys [next-index]} :raft-leader-state
    {:keys [now next-timeout period]} :timer
    {:as raft-state
     :keys [commit-index current-term node-type]} :raft-state}))

(def update-commit
  (rule
   :update-commit
   (and (= :leader node-type)
        (possible-new-commit commit-index raft-state match-index node-set
                             current-term))
   (-> state
       (log-trace "update-commit"
                  (possible-new-commit
                   commit-index raft-state match-index node-set current-term)
                  "match-index value frequencies"
                  (frequencies (vals (:match-index (:raft-leader-state state)))))
       (assoc-in [:raft-state :commit-index]
                 (possible-new-commit
                  commit-index raft-state match-index node-set current-term)))
   {:as state
    {:keys [match-index]} :raft-leader-state
    {:keys [commit-index node-set current-term node-type]
     :as raft-state} :raft-state}))

(def leader-handle-append-entries-response
  (guard
   (and (= node-type :leader)
        (= message-type :append-entries-response))
   (rule
    (not success?)
    (-> state
        (consume-message)
        (update-in [:raft-leader-state :next-index from] dec)
        (update-in [:raft-leader-state :next-index from] max 0N))
    {:as state
     {{:keys [success? from]} :message} :io})
   (rule
    success?
    (-> state
        (consume-message)
        (assoc-in [:raft-leader-state :next-index from] (inc last-log-index))
        (assoc-in [:raft-leader-state :match-index from] last-log-index))
    {:as state
     {{:keys [success? from last-log-index]} :message} :io})
   {:as state
    {{message-type :type} :message} :io
    {:keys [node-type]} :raft-state}))

(def ignore-messages-from-old-terms
  (rule
   (and message-term
        (> current-term message-term))
   (-> state
       (consume-message))
   {:as state
    {{message-term :term} :message} :io
    {:keys [current-term]} :raft-state}))

(def rules-of-raft
  (guard
   true
   #'ignore-messages-from-old-terms
   #'keep-up-apply
   #'jump-to-newer-term
   #'follower-respond-to-append-entries
   #'follower-respond-to-request-vote
   #'become-candidate-and-call-for-election
   #'candidate-receive-votes
   #'candidate-respond-to-append-entries
   #'candidate-election-timeout
   #'heart-beat
   #'leader-receive-command
   #'update-followers
   #'update-commit
   #'leader-handle-append-entries-response
   _))
