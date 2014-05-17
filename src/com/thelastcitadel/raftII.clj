(ns com.thelastcitadel.raft
  (:require [clojure.core.async :refer [alt!! timeout >!!]]
            [clojure.core.match :refer [match]])
  (:import (java.util UUID)))

(def valid-states #{:follower :candidate :leader})

(defn state [id broadcast-channel receive-channel servers]
  {:me id
   :term 0
   :broadcast-channel broadcast-channel
   :receive-channel receive-channel
   :state :follower
   :commit-index 0
   :last-applied 0
   :log {}
   :voted-for nil
   :next-index 0
   :match-index 0
   :value {}
   :servers servers
   :votes 0})

(def heart-beat-time-out 1000)

(declare receive-message)

;; {:op :delete :args []}
;; {:op :write :args ["foo" "bar"]}
;; {:op :read}
;; {:op :write-if :args ["foo" "bar"]}

(defn main-loop [state]
  (if (> (:commit-index state)
         (:last-applied state))
    (let [op-index (:last-applied state)
          op (get (:log state) op-index)
          value (reduce
                 (fn [value {:keys [op args]}]
                   (case op
                     :delete (dissoc value (first args))
                     :write (assoc value (first args) (second args))
                     ;; TODO: nop?
                     :read value
                     :write-if (if (contains? state (first args))
                                 state
                                 (assoc value (first args) (second args)))
                     :nop state))
                 (:value state)
                 (:entries op))
          state (assoc state
                  :value value)
          state (update-in state [:last-applied] inc)]
      #(main-loop state))
    (if (= :leader (:state state))
      (let [last-seen (apply min (vals (:match-index state)))]
        (if (> (:last-applied state) last-seen)
          (>!! (:broadcast-channel state)
               (dissoc (get (:log state) last-seen)
                       :index)))
        #(receive-message state))
      #(receive-message state))))

(defn receive-message [state]
  (let [message (alt!!
                 (:receive-channel state) ([m] m)
                 ;; randomize timeout
                 ;; TODO: shorter timeout if you are a leader?
                 (timeout heart-beat-time-out) ([_] {:type :timeout}))]
    (if (> (:term message) (:term state))
      #(main-loop (assoc state
                    :state :follower))
      (match [(:type message) (:state state)]
             [:timeout :follower] (let [new-term (inc (:term state))]
                                    (>!! (:broadcast-channel state) {:type :request-vote
                                                                     :term new-term
                                                                     :from (:me state)
                                                                     :last-log-index (:index (last (:log state)))
                                                                     :last-log-term (:index (term (:log state)))
                                                                     :ts (System/currentTimeMillis)})
                                    #(main-loop (assoc state
                                                  :term new-term
                                                  :votes 0
                                                  :state :candidate)))
             [:timeout :candidate] (assert nil)
             [:timeout :leader] (assert nil)
             ;;
             [:request-vote-result :follower] #(main-loop state)
             [:request-vote-result :candidate] (if (and (= (:term state) (:term message))
                                                        (= (:me state) (:vote-for message)))
                                                 (let [votes (inc (:votes state))]
                                                   (if (> 2 (/ (count (:servers state))
                                                               votes))
                                                     (let [new-state (assoc state
                                                                       :state :leader
                                                                       ;; TODO: ?
                                                                       :next-index (inc (count (:log state)))
                                                                       :match-index
                                                                       (into {} (for [server (:servers state)]
                                                                                  [server 0])))]
                                                       (>!! (:broadcast-channel state)
                                                            {:type :append-entries
                                                             :term (:term state)
                                                             :from (:me state)
                                                             :prev-log-index (:index (last (:log state)))
                                                             :prev-log-term (:term (last (:log state)))
                                                             :commit-index (:commit-index state)
                                                             :entries [{:op :nop :args []}]}))
                                                     #(main-loop (assoc state
                                                                   :votes votes))))
                                                 #(main-loop state))
             [:request-vote-result :leader] (do
                                              (>!! (:broadcast-channel state)
                                                   {:type :append-entries
                                                    :term (:term state)
                                                    :from (:me state)
                                                    :prev-log-index (:index (last (:log state)))
                                                    :prev-log-term (:term (last (:log state)))
                                                    :commit-index (:commit-index state)
                                                    :entries [{:op :nop :args []}]})
                                              #(main-loop state))
             ;;
             [:request-vote :follower] (cond
                                        (> (:term state) (:term message))
                                        (do
                                          ;; TODO:
                                          (>!! (:broadcast-channel state) {:type :request-vote-result
                                                                           :vote-for nil
                                                                           :from (:me state)
                                                                           :ts (System/currentTimeMillis)})
                                          #(main-loop state))
                                        (and (nil? (:voted-for state))
                                             (>= (:last-log-index message)
                                                 (:index (last (:log state)))))
                                        (do
                                          (>!! (:broadcast-channel state) {:type :request-vote-result
                                                                           :vote-for (:from message)
                                                                           :from (:me state)
                                                                           :ts (System/currentTimeMillis)})
                                          #(main-loop (assoc state
                                                        :state :follower
                                                        :voted-for (:from message))))
                                        :else
                                        (do
                                          (>!! (:broadcast-channel state) {:type :request-vote-result
                                                                           :vote-for nil
                                                                           :from (:me state)
                                                                           :ts (System/currentTimeMillis)})
                                          #(main-loop state)))
             [:request-vote-result :candidate] (cond
                                                (> (:term state) (:term message))
                                                (do
                                                  ;; TODO:
                                                  (>!! (:broadcast-channel state) {:type :request-vote-result
                                                                                   :vote-for nil
                                                                                   :from (:me state)
                                                                                   :term (:term state)
                                                                                   :ts (System/currentTimeMillis)})
                                                  #(main-loop state))
                                                (and (nil? (:voted-for state))
                                                     (>= (:last-log-index message)
                                                         (:index (last (:log state)))))
                                                (do
                                                  (>!! (:broadcast-channel state) {:type :request-vote-result
                                                                                   :vote-for (:from message)
                                                                                   :from (:me state)
                                                                                   :ts (System/currentTimeMillis)})
                                                  #(main-loop (assoc state
                                                                :state :follower
                                                                :voted-for (:from message))))
                                                :else
                                                (do
                                                  (>!! (:broadcast-channel state) {:type :request-vote-result
                                                                                   :vote-for nil
                                                                                   :from (:me state)
                                                                                   :ts (System/currentTimeMillis)})
                                                  #(main-loop state)))
             [:request-vote :leader] (do
                                       (>!! (:broadcast-channel state)
                                            {:type :append-entries
                                             :term (:term state)
                                             :from (:me state)
                                             :prev-log-index (:index (last (:log state)))
                                             :prev-log-term (:term (last (:log state)))
                                             :commit-index (:commit-index state)
                                             :entries [{:op :nop :args []}]})
                                       #(main-loop state))
             ;;
             [:append-entries :follower] (if
                                          (or (< (:term message) (:term state))
                                              (= (:prev-log-term message)
                                                 (:term (get (:log state) (:prev-log-index message)))))
                                          (do
                                            (>!! (:broadcast-channel state)
                                                 {:type :append-entries-response
                                                  :term (:term state)
                                                  :from (:me state)
                                                  :success false})
                                            #(main-loop state))
                                          )
             [:append-entries :candidate] ...
             [:append-entries :leader] (assert nil)
             )))
  )
