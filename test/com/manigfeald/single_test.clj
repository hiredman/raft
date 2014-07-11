(ns com.manigfeald.single-test
  (:require [clojure.test :refer :all]
            [com.manigfeald.raft :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]))

(defn n-rafts [n delays]
  (for [i (range n)]
    {:id i
     :in-queue clojure.lang.PersistentQueue/EMPTY
     :raft (raft i (set (range n)) (->Timer 0 0 (get delays i)))}))

(defn step-n-rafts [rafts instructions]
  (let [messages (group-by :target (mapcat (comp :out-queue :io :raft) rafts))
        rafts (for [{:keys [id in-queue raft]} rafts]
                {:id id
                 :raft (update-in raft [:io :out-queue] empty)
                 :in-queue (into in-queue (get messages id))})]
    (for [{:keys [id in-queue raft]} rafts
          :let [inst (get instructions id)
                raft (update-in raft [:timer :now] inc)]]
      (case inst
        :drop-message {:id id
                       :raft (run-one (assoc-in raft [:io :message] nil))
                       :in-queue (if (empty? in-queue)
                                   in-queue
                                   (pop in-queue))}
        :receive-message (let [message (when-not (empty? in-queue)
                                         (peek in-queue))
                               in-queue (if (empty? in-queue)
                                          in-queue
                                          (pop in-queue))]
                           {:id id
                            :raft (run-one (assoc-in raft
                                                     [:io :message]
                                                     message))
                            :in-queue in-queue})
        :stall {:id id
                :raft raft
                :in-queue in-queue}
        :no-message {:id id
                     :raft (run-one (assoc-in raft [:io :message] nil))
                     :in-queue in-queue}))))

(def instructions
  (gen/elements [:drop-message :receive-message :stall :no-message]))

(defn programs [machines]
  (fn [program-size]
    (gen/vector (apply gen/tuple (repeat machines instructions))
                program-size)))

(def zero-or-one-leader-per-term
  (prop/for-all
   [delays (gen/tuple gen/s-pos-int
                      gen/s-pos-int
                      gen/s-pos-int)
    program (gen/bind gen/nat (programs 3))]
   (every?
    #(or (= % 0) (= % 1))
    (for [{:keys [rafts]} (take-while
                           identity
                           (iterate
                            (fn [{:keys [rafts program]}]
                              (when (seq program)
                                {:program (rest program)
                                 :rafts (step-n-rafts rafts
                                                      (first program))}))
                            {:program program
                             :rafts (n-rafts 3 delays)}))
          n (vals (frequencies
                   (for [{{{:keys [node-type current-term]} :raft-state} :raft}
                         rafts
                       :let [_ (assert node-type node-type)]
                       :when (= :leader node-type)]
                   current-term)))]
      n))))

(defspec zero-or-one-leader-per-term-spec
  10000
  zero-or-one-leader-per-term)

(comment


  (tc/quick-check 10 zero-or-one-leader-per-term)


  )
