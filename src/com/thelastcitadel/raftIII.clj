(ns com.thelastcitadel.raftIII
  (:require [clojure.core.async :refer [alt!! timeout >!! chan]]))

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
    (>!! (get cluster node) msg)))

(defn broadcast [cluster msg]
  (doseq [node (list-nodes cluster)]
    (send-to cluster node msg)))

(defrecord RaftLeaderState [next-index match-index])
(defrecord RaftState [current-term voted-for log commit-index last-applied node-type value votes])
(defrecord State [in-queue out-queue raft-state raft-leader-state id cluster])

(defn raft [id cluster]
  (->State clojure.lang.PersistentQueue/EMPTY
           clojure.lang.PersistentQueue/EMPTY
           (->RaftState 0 nil {} 0 0 :follower {} 0)
           (->RaftLeaderState {} {})
           id
           cluster))

(defmacro rule [condition change binding]
  `(fn [& args#]
     (let [~binding (first args#)]
       (when ~condition
         ~change))))

(defn apply-op [op value]
  (case (:op op)
    :read value
    :write (assoc value
             (:key op) (:value op))))

(defn advance-applied-to-commit [raft-state]
  (if (> (:commit-index raft-state)
         (:last-applied raft-state))
    (let [new-last (inc (:last-applied raft-state))
          op (get (:log raft-state) new-last)
          new-value (apply-op op (:value raft-state))]
      (recur (assoc raft-state
               :last-applied new-last
               :value new-value)))
    raft-state))

(defn last-log-index [raft-state]
  (apply max 0 (keys (:log raft-state))))

(defn last-log-term [raft-state]
  (:term (get (:log raft-state) (last-log-index raft-state))))

(defn log-contains? [raft-state log-index log-term]
  (and (contains? (:log raft-state) log-index)
       (= log-term (get (:log raft-state) log-index))))

(defn append-log [raft-state entries]
  (update-in raft-state [:log] (merge (into {} (for [entry entries] [(:index entry) entry])))))

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
        (> (:term (peek in-queue)) current-term))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:raft-state] merge {:node-type :follower
                                       :vote-for nil
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
                                       :vote-for id
                                       :votes 0}))
   {:keys [id in-queue raft-state]
    {:keys [current-term node-type]} :raft-state
    :as state}))

(def respond-to-vote-request
  (rule
   (and (not (empty? in-queue))
        (= :request-vote (:type (peek in-queue)))
        (or (nil? voted-for)
            (= voted-for (:candidate-id (peek in-queue))))
        (or (= node-type :candidate)
            (= node-type :follower)))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:out-queue] conj {:type :request-vote-response
                                     :target (:candidate-id (peek in-queue))
                                     :term current-term
                                     :vote? true
                                     :from id}))
   {:as state
    :keys [in-queue]
    {:keys [voted-for node-type current-term]} :raft-state}))

(def receive-vote
  (rule
   (and (not (empty? in-queue))
        (= :request-vote-response (:type (peek in-queue)))
        (:vote? (peek in-queue))
        (or (= node-type :candidate)
            (= node-type :follower)))
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:raft-state :votes] inc))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(defmacro LOG [form]
  `(let [f# ~form]
     (locking #'*out*
       (when (= 1 (:id ~'state))
         (prn (:id ~'state) '~form f#)))
     f#))

(def become-leader
  (rule
   (and (LOG node-type)
        (LOG (number? votes))
        (LOG (> votes 0))
        (LOG (> 2 (/ (inc (count (list-nodes cluster))) votes)))
        (LOG (= node-type :candidate)))
   (do
     (locking #'*out*
       (println "leadering" state))
     (-> state
         (update-in [:out-queue] conj {:target :broadcast
                                       :type :append-entries
                                       :term (inc current-term)
                                       :leader-id id
                                       :prev-log-index (last-log-index raft-state)
                                       :prev-log-term (last-log-term raft-state)
                                       :entries []
                                       :from id
                                       :leader-commit commit-index})
         (update-in [:raft-state] merge {:node-type :leader
                                         :vote-for nil
                                         :votes 0})
         (update-in [:raft-leader-state] merge {:next-index (into {} (for [node (list-nodes cluster)]
                                                                       [node (inc (last-log-index))]))
                                                :next-match (into {} (for [node (list-nodes cluster)]
                                                                       [node 0]))})))
   {:as state
    :keys [cluster id]
    {:keys [votes current-term commit-index node-type] :as raft-state} :raft-state}))

(def follow-the-leader
  (rule
   (and (= node-type :candidate)
        (= :append-entries (:type (peek in-queue))))
   (update-in state [:raft-state] merge {:node-type :follower
                                         :vote-for nil
                                         :votes 0})
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def reject-append-entries-from-old-leaders
  (rule
   (and (= node-type :follower)
        (not (empty? in-queue))
        (= :append-entries (:type (peek in-queue)))
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
        (not (log-contains? raft-state
                            (:prev-log-term (peek in-queue))
                            (:prev-log-index (peek in-queue)))))
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
        (log-contains? raft-state
                       (:prev-log-term (peek in-queue))
                       (:prev-log-index (peek in-queue))))
   (do
     #_(locking #'*out*
       (println (:leader-id (peek in-queue)) "is the leader"))
     (-> state
         (update-in [:in-queue] pop)
         (update-in [:out-queue] conj {:target (:leader-id (peek in-queue))
                                       :type :append-entries-response
                                       :term current-term
                                       :success? true
                                       :last-index (apply max (map :index (:entries (peek in-queue))))
                                       :from id})
         (update-in [:raft-state] clear-log-after (:prev-log-term (peek in-queue)))
         (update-in [:raft-state] append-log (:entries (peek in-queue)))
         (update-in [:raft-state] set-commit (:leader-commit (peek in-queue)))))
   {:as state
    :keys [in-queue raft-state id]
    {:keys [node-type current-term]} :raft-state}))

(def handle-unsuccessful-append-entries
  (rule
   (and (= node-type :leader)
        (not (empty? in-queue))
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
   (-> state
       (update-in [:in-queue] pop)
       (update-in [:raft-leader-state :next-index (:from (peek in-queue))] (inc (:last-index (peek in-queue)))))
   {:as state
    :keys [in-queue]
    {:keys [node-type]} :raft-state}))

(def advance-laggy-followers
  (rule
   (and (= node-type :leader)
        (let [min-next (apply min (vals next-index))]
          (and (>= (last-log-index raft-state) min-next)
               (not (some #(some (fn [entry] (= min-next (:index entry))) (:entries %)) out-queue)))))
   (let [min-next (apply min (vals next-index))]
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
   (let [ns (sort (map second match-index))
         n (apply max 0 (take (dec (/ (count match-index) 2)) ns))]
     (and (> n commit-index)
          (= current-term (:term (get log n)))))
   (let [ns (sort (map second match-index))
         n (apply max 0 (take (dec (/ (count match-index) 2)) ns))]
     #_(locking #'*out*
       (println "advancing commit to" n))
     (update-in state [:raft-state] assoc :commit-index n))
   {:as state
    {:keys [match-index]} :raft-leader-state
    {:keys [commit-index current-term log]} :raft-state}))


;;;

(def raft-rules
  [#'keep-up-commited
   #'jump-to-new-term
   #'new-election-on-timeout
   #'respond-to-vote-request
   #'receive-vote
   #'become-leader
   #'follow-the-leader
   #'reject-append-entries-from-old-leaders
   #'reject-append-entries-with-unknown-prevs
   #'accept-append-entries
   #'handle-unsuccessful-append-entries
   #'handle-successful-append-entries
   #'advance-laggy-followers
   #'advance-commit])

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
                            (do
                              (locking #'*out*
                                (when (= 1 (:id raft-state))
                                  (println "applied" rule)))
                              r)
                            raft-state))
                        raft-state
                        raft-rules)]
    (if (= raft-state new-raft-state)
      new-raft-state
      (recur new-raft-state))))

(defn run [in id cluster]
  (loop [state (raft id cluster)]
    (locking #'*out*
      (println id (:current-term (:raft-state state)))
      (when (= 1 id)
        (println "node-type" (:node-type (:raft-state state))))
      #_(println id "has" (:votes (:raft-state state)) "votes"
               (:node-type (:raft-state state))
               (when-not (zero? (:votes (:raft-state state)))
                 [(/ (inc (count (list-nodes cluster)))
                     (:votes (:raft-state state)))
                  (> 2 (/ (inc (count (list-nodes cluster)))
                          (:votes (:raft-state state))))])))
    (let [message (alt!!
                   in ([message] message)
                   (timeout (if (= :candidate (:node-type (:raft-state state)))
                              (rand-int 10)
                              4000))
                   ([_] {:type :timeout
                         :term 0}))
          state (update-in state [:in-queue] conj message)
          new-state (step state)]
      #_(locking #'*out*
        (println id
                 (:node-type (:raft-state state))
                 "=>"
                 (:node-type (:raft-state new-state))))
      (doseq [msg (:out-queue new-state)]
        (if (= :broadcast (:target msg))
          (broadcast cluster msg)
          (send-to cluster (:target msg) msg)))
      (recur (update-in new-state [:out-queue] empty)))))

(comment

  (def in1 (chan 10))
  (def id1 1)
  (def in2 (chan 10))
  (def id2 2)
  (def in3 (chan 10))
  (def id3 3)
  ;; (def in4 (chan 10))
  ;; (def id4 3)

  (def cluster1
    (-> (->ChannelCluster)
        (add-node id2 in2)
        (add-node id3 in3)))

  (def cluster2
    (-> (->ChannelCluster)
        (add-node id1 in1)
        (add-node id3 in3)))

  (def cluster3
    (-> (->ChannelCluster)
        (add-node id1 in1)
        (add-node id2 in2)))
  

  (def fut2
    (future
      (run in2 id2 cluster2)))

  (def fut3
    (future
      (run in3 id3 cluster3)))

  ;; (def fut4
  ;;   (future
  ;;     (run in4 id4 cluster4)))

  (run in1 id1 cluster1)

  )
