(ns com.thelastcitadel.raft
  (:require [clojure.core.async :refer [alt!! timeout >!! chan sliding-buffer]]
            [clojure.tools.logging :as log]))

(defprotocol Cluster
  (add-node [cluster node arg1])
  (remove-node [cluster node])
  (list-nodes [cluster])
  (send-to [cluster node msg]))

(defprotocol API
  (apply-read [value operation])
  (apply-write [value operation]))

(defn broadcast [cluster msg]
  (doseq [node (list-nodes cluster)]
    (send-to cluster node msg)))

(defrecord RaftLeaderState [next-index match-index])
(defrecord RaftState [current-term voted-for log commit-index last-applied node-type value votes leader-id])
(defrecord State [in-queue out-queue raft-state raft-leader-state id cluster])

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

(defn raft [id cluster]
  (->State clojure.lang.PersistentQueue/EMPTY
           clojure.lang.PersistentQueue/EMPTY
           (->RaftState 0 nil {} 0 0 :follower (->MapValue) 0 nil)
           (->RaftLeaderState {} {})
           id
           cluster))

(defmacro rule [condition change binding]
  `(fn [& args#]
     (let [~binding (first args#)]
       (when ~condition
         ~change))))

(defn advance-applied-to-commit [raft-state]
  (if (> (:commit-index raft-state)
         (:last-applied raft-state))
    (let [new-last (inc (:last-applied raft-state))
          op (get (:log raft-state) new-last)]
      (if (= :read (:operation-type op))
        (let [read-value (apply-read (:value raft-state) op)
              log (assoc-in raft-state [:log new-last :value] read-value)]
          (recur (assoc raft-state
                   :last-applied new-last
                   :log log)))
        (let [new-value (apply-write (:value raft-state) op)]
          (recur (assoc raft-state
                   :last-applied new-last
                   :value new-value)))))
    raft-state))

(defn last-log-index [raft-state]
  (apply max 0 (keys (:log raft-state))))

(defn last-log-term [raft-state]
  (or (:term (get (:log raft-state) (last-log-index raft-state))) 0))

(defn log-contains? [raft-state log-index log-term]
  (and (contains? (:log raft-state) log-index)
       (= log-term (get (:log raft-state) log-index))))

(defn append-log [raft-state entries]
  {:pre [(every? (comp number? :index) entries)]
   :post [(map? (:log %))]}
  (update-in raft-state [:log] merge (into {} (for [entry entries] [(:index entry) entry]))))

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

;;; Raft Rules

(def keep-up-commited
  (rule
   (> commit-index last-applied)
   (update-in state [:raft-state] advance-applied-to-commit)
   {{:keys [commit-index last-applied]} :raft-state :as state}))

(def jump-to-new-term
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
  (rule
   (and (not (empty? in-queue))
        (= :timeout (:type (peek in-queue)))
        (or (= node-type :candidate)
            (= node-type :follower)))
   (do
     (log/info id "a" node-type "requests a vote for" (inc current-term))
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:out-queue] conj {:type :request-vote
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
       (update-in [:raft-state] merge {:voted-for (:candidate-id (peek in-queue))})
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
     (log/info id "doesn't vote for" (:candidate-id (peek in-queue)))
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
     (log/info (:from (peek in-queue)) "voted for" (:id state) "in" (:current-term (:raft-state state)))
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
     (log/info (:id state) "becomes leader in term" current-term "with" votes "votes")
     (-> state
         (update-in [:out-queue] conj {:target :broadcast
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
         (update-in [:raft-leader-state] merge {:next-index (into {} (for [node (list-nodes cluster)]
                                                                       [node (inc (last-log-index raft-state))]))
                                                :next-match (into {} (for [node (list-nodes cluster)]
                                                                       [node 0]))})))
   {:as state
    :keys [cluster id]
    {:keys [votes current-term commit-index node-type] :as raft-state} :raft-state}))

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
   (do
     (log/info "append-entries from" (:from (peek in-queue)))
     (update-in state [:raft-state] merge {:node-type :follower
                                           :voted-for nil
                                           :votes 0}))
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
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:out-queue] conj {:target (:leader-id (peek in-queue))
                                     :type :append-entries-response
                                     :term current-term
                                     :success? false
                                     :from id}))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term]} :raft-state}))

(def accept-append-entries
  (rule
   (and (= node-type :follower)
        (not (empty? in-queue))
        (= :append-entries (:type (peek in-queue)))
        (or (zero? (:prev-log-term (peek in-queue)))
            (log-contains? raft-state
                           (:prev-log-term (peek in-queue))
                           (:prev-log-index (peek in-queue)))))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:out-queue] conj {:target (:leader-id (peek in-queue))
                                     :type :append-entries-response
                                     :term current-term
                                     :success? true
                                     :last-index (last-log-index raft-state)
                                     :from id})
       (assoc-in [:raft-state :voted-for] nil)
       (assoc-in [:raft-state :leader-id] (:leader-id (peek in-queue)))
       (update-in [:raft-state] clear-log-after (:prev-log-term (peek in-queue)))
       (update-in [:raft-state] append-log (:entries (peek in-queue)))
       (update-in [:raft-state] set-commit (:leader-commit (peek in-queue)))
       (as-> state
             (update-in state [:out-queue] conj {:target (:leader-id (peek in-queue))
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
        (not (:success? (peek in-queue))))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:raft-leader-state :next-index (:from (peek in-queue))] dec))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def handle-successful-append-entries
  (rule
   (and (= node-type :leader)
        (not (empty? in-queue))
        (= :append-entries-response (:type (peek in-queue)))
        (:success? (peek in-queue)))
   (do
     (when (not= (:last-index (peek in-queue))
                 (get-in state [:raft-leader-state :next-index (:from (peek in-queue))]))
       (log/info (:from (peek in-queue)) "has accepted up to" (:last-index (peek in-queue))))
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
        (let [min-next (apply min (vals next-index))]
          (and (>= (last-log-index raft-state) min-next)
               (not (some #(some (fn [entry] (= min-next (:index entry))) (:entries %)) out-queue)))))
   (let [min-next (apply min (vals next-index))]
     (log/info "advance laggy followers to" min-next)
     (-> state
         (update-in [:out-queue] conj {:target :broadcast
                                       :type :append-entries
                                       :term current-term
                                       :leader-id id
                                       :prev-log-index (dec min-next)
                                       :prev-log-term (dec min-next)
                                       :entries [(get log min-next)]
                                       :from id
                                       :leader-commit commit-index})))
   {:as state
    :keys [out-queue id]
    {:keys [node-type commit-index log current-term] :as raft-state} :raft-state
    {:keys [next-index]} :raft-leader-state}))

(def advance-commit
  (rule
   (and (= :leader node-type)
        (let [ns (sort (map second match-index))
              quorum-count (Math/ceil (/ (count match-index) 2.0))
              _ (log/info "quorum-count" quorum-count)
              n (apply max 0 (take quorum-count ns))]
          (and (> n commit-index)
               (= current-term (:term (get log n))))))
   (do
     (log/info "advance-commit")
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
     (log/info "removing" (:node (peek in-queue)) "from" id "'s cluster")
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
   (let [s (-> state
               (update-in [:in-queue] pop)
               (update-in [:raft-state] append-log [(assoc (peek in-queue)
                                                      :term current-term
                                                      :index (inc (last-log-index raft-state)))]))]
     (log/info "accepting operations")
     (log/info s)
     s)
   {:as state
    :keys [in-queue id raft-state]
    {:keys [node-type current-term]} :raft-state}))

;;;

(def raft-rules
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
   #'accept-append-entries
   #'handle-unsuccessful-append-entries
   #'handle-successful-append-entries
   #'advance-laggy-followers
   #'advance-commit
   #'drop-append-entry-responses-from-previous-terms
   #'raft-remove-node
   #'leader-accept-operations])

;; (defn step [raft-state]
;;   (loop [[rule & rules] raft-rules]
;;     (if (nil? rule)
;;       raft-state
;;       (if-let [r (rule raft-state)]
;;         (do
;;           (locking #'*out*
;;             (println "rule matched" rule))
;;           r)
;;         (recur rules)))))

(defn step [raft-state]
  (let [new-raft-state (reduce
                        (fn [raft-state rule]
                          (if-let [r (rule raft-state)]
                            r
                            raft-state))
                        raft-state
                        raft-rules)]
    (if (= raft-state new-raft-state)
      new-raft-state
      (recur new-raft-state))))

(defn run-one [state message]
  (let [cluster (:cluster state)]
    (if (= :stop (:type message))
      (do
        (log/info "stopping" (:id state))
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
          (log/info (:id state) "changed terms"(:current-term (:raft-state state)) "=>"
                    (:current-term (:raft-state new-state))))
        (when (not= (:commit-index (:raft-state state))
                    (:commit-index (:raft-state new-state)))
          (assert (> (:commit-index (:raft-state new-state))
                     (:commit-index (:raft-state state))))
          (log/info (:id state) "changed commits from"
                    (:commit-index (:raft-state state))
                    "to"
                    (:commit-index (:raft-state new-state))))
        new-state))))
