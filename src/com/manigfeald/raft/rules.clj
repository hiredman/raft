(ns com.manigfeald.raft.rules
  (:require [com.manigfeald.raft.core :refer :all]))

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

(defrecord Rule [head body]
  clojure.lang.IFn
  (invoke [this arg]
    (if (head arg)
      (let [r (body arg)]
        [true r])
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

(defmacro rule
  "a rule is a combination of some way to decide if the rule
  applies (the head) and some way to apply the rule (the body)"
  ([head body bindings]
     (if (:line (meta head))
       `(rule ~(str "line-" (:line (meta head)))
              ~head ~body ~bindings)
       `(->Rule (fn [~bindings]
                  ~head)
                (fn [~bindings]
                  ~body))))
  ([name head body bindings]
     `(assoc (->Rule (fn ~(symbol (str (clojure.core/name name) "-head")) [~bindings]
                       ~head)
                     (fn ~(symbol (str (clojure.core/name name) "-body"))[~bindings]
                       ~body))
        :name ~name)))

(defmacro guard
  [head & things]
  (let [rules (butlast things)
        bindings (last things)]
    `(->CompoundRule (fn [~bindings]
                       ~head)
                     ~(vec rules))))

;;
(def keep-up-apply
  (rule
   (> commit-index last-applied)
   (-> state
       (update-in [:raft-state] advance-applied-to-commit))
   {:as state
    {:keys [commit-index last-applied]} :raft-state}))

(def jump-to-newer-term
  (rule
   (and message-term
        (> message-term current-term))
   (-> state
       (log-trace "jump-to-newer-term")
       (update-in [:raft-state] merge {:node-type :follower
                                       :current-term message-term
                                       :votes 0
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
   (reject-append-entries state leader-id current-term id)
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
   (reject-append-entries state leader-id current-term id)
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
    (accept-append-entries state leader-id current-term id)
    {:as state
     :keys [id]
     {{prev-log-index :prev-log-index
       prev-log-term :prev-log-term
       message-term :term
       leader-id :leader-id} :message} :io
     {:keys [current-term node-type] :as raft-state} :raft-state})
   {{{message-type :type} :message} :io
    {:keys [node-type]} :raft-state}))

(def follower-respond-to-request-vote
  (guard
   (and (= :follower node-type)
        (= message-type :request-vote))
   (rule
    :success
    (and (or (= voted-for candidate-id)
             (nil? voted-for))
         (log-contains? raft-state last-log-term last-log-index))
    (-> state
        (log-trace "votes for" candidate-id "in" current-term)
        (consume-message)
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
    (or (and (not= voted-for candidate-id)
             (not (nil? voted-for)))
        (not (log-contains? raft-state last-log-term last-log-index)))
    (-> state
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
        (log-trace "received vote from" from "in" current-term)
        (log-trace "becoming leader with" (inc votes) "in" current-term)
        (update-in [:raft-state] merge {:node-type :leader
                                        :votes 0
                                        :voted-for nil
                                        :leader-id id})
        (assoc-in [:timer :next-timeout] (+ now period))
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
     {:keys [votes node-set current-term commit-index] :as raft-state} :raft-state})
   (rule
    (not (enough-votes? (count node-set) (inc votes)))
    (-> state
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
                                       :votes 0})
       (assoc-in [:timer :next-timeout] (+ period now)))
   {:as state
    {{message-type :type} :message} :io
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
       (consume-message)
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

(def leader-receive-command
  (rule
   false
   state
   state))

(def update-followers
  (rule
   (seq (for [[node next-index] next-index
              :when (not= node id)
              :when (>= (last-log-index raft-state) next-index)]
          node))
   (-> state
       (publish (for [[node next-index] next-index
                      :when (not= node id)
                      :when (>= (last-log-index raft-state) next-index)]
                  {:type :append-entries
                   :target node
                   :term current-term
                   :leader-id id
                   :prev-log-index (max 0 (dec next-index))
                   :prev-log-term (or (:term (get (:log raft-state)
                                                  (dec next-index)))
                                      0)
                   :entries (for [index (range (max 0 (dec next-index))
                                               (inc next-index))
                                  :when (not (zero? index))]
                              (get (:log raft-state) index))
                   :leader-commit commit-index
                   :from id})))
   {:as state
    :keys [id]
    {:keys [next-index]} :raft-leader-state
    {:as raft-state
     :keys [commit-index current-term]} :raft-state}))

(def update-commit
  (rule
   (possible-new-commit commit-index raft-state match-index node-set current-term)
   (-> state
       (assoc-in [:raft-state :commit-index]
                 (possible-new-commit
                  commit-index raft-state match-index node-set current-term)))
   {:as state
    {:keys [match-index]} :raft-leader-state
    {:keys [commit-index node-set current-term] :as raft-state} :raft-state}))

(def leader-handle-append-entries-response
  (guard
   (and (= node-type :leader)
        (= message-type :append-entries-response))
   (rule
    (not success?)
    (-> state
        (update-in [:raft-leader-state :next-index from] dec)
        (update-in [:raft-leader-state :next-index from] max 0))
    {:as state
     {{:keys [success? from]} :message} :io})
   (rule
    success?
    (-> state
        (assoc-in [:raft-leader-state :next-index from] (inc last-log-index))
        (assoc-in [:raft-leader-state :match-index from] last-log-index))
    {:as state
     {{:keys [success? from last-log-index]} :message} :io})
   {:as state
    {{message-type :type} :message} :io
    {:keys [node-type]} :raft-state}))

(def rules-of-raft
  (guard
   true
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
