(ns com.manigfeald.single-test
  (:require [clojure.test :refer :all]
            [com.manigfeald.raft :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))

(defn n-rafts [n]
  (for [i (range n)]
    {:id i
     :in-queue clojure.lang.PersistentQueue/EMPTY
     :raft (raft i (set (range n)) (->Timer 0 0 (+ 50 (rand-int 50))))}))

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
  (gen/vector (apply gen/tuple (repeat machines instructions))
              5000))

;; (def instructions-for-three-machines
;;   (gen/vector (gen/tuple instructions instructions instructions)
;;               5000))

(def zero-or-one-leader
  (prop/for-all
   [program (programs 3)]
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
                             :rafts (n-rafts 3)}))]
      (count (for [{{{:keys [node-type]} :raft-state} :raft} rafts
                   :let [_ (assert node-type node-type)]
                   :when (= :leader node-type)]
               nil))))))

(comment


(tc/quick-check 200 zero-or-one-leader)

  )
