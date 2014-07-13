(ns com.manigfeald.raft.log
  "extends RaftLog and Counted to ISeq and IPersistentMap because you
  need at least two implementations for an abstraction"
  (:require [clojure.set :as set]))

(defprotocol RaftLog
  (log-contains? [log log-term log-index]
    "does the log contain an entry with the given term and index")
  (last-log-index [log]
    "what is the index of the last entry in the log")
  (last-log-term [log]
    "what is the term of the last entry in the log")
  (indices-and-terms [log]
    "a seq of [index term] pairs from the log")
  (add-to-log [log index entry]
    "add an entry to the log with a given index")
  (log-entry-of [log index]
    "return the log entry with the given index or nil")
  (entry-with-serial [log serial]
    "return the log entry associated with the given serial or nil")
  (delete-from [log index]
    "delete log entries from the given index onwards"))

(defprotocol Counted
  (log-count [log]
    "return the count of entries in the log"))

(extend-type clojure.lang.ISeq
  Counted
  (log-count [log]
    (count log))
  RaftLog
  (log-contains? [log log-term log-index]
    (loop [[head & tail] log]
      (cond
       (nil? head)
       false
       (and (= log-term (:term head))
            (= log-index (:index head)))
       true
       :else
       (recur tail))))
  (last-log-index [log]
    (or (:index (first log)) 0))
  (last-log-term [log]
    (or (:term (first log)) 0))
  (indices-and-terms [log]
    (for [entry log]
      [(:index entry) (:term entry)]))
  (add-to-log [log index entry]
    (let [r (sort-by #(- 0 (:index %))
                     (conj (for [entry log
                                 :when (not= index (:index entry))]
                             entry)
                           (assoc entry
                             :index index)))]
      (assert (some #{(assoc entry
                        :index index)}
                    r))
      r))
  (log-entry-of [log needle-index]
    (first
     (for [{:keys [index] :as entry} log
           :when (= index needle-index)]
       entry)))
  (entry-with-serial [log needle-serial]
    (first
     (for [{:keys [serial] :as entry} log
           :when (= serial needle-serial)]
       entry)))
  (delete-from [log index]
    (doall (for [item log
                 :when (not (>= (:index item) index))]
             item))))

(extend-type clojure.lang.IPersistentMap
  Counted
  (log-count [this]
    (count this))
  RaftLog
  (log-contains? [log log-term log-index]
    (boolean (seq (for [[index {:keys [term]}] log
                        :when (= index log-index)
                        :when (= term log-term)]
                    index))))
  (last-log-index [log]
    (apply max 0 (keys log)))
  (last-log-term [log]
    (or (first (for [[index {:keys [term]}] log
                     :when (= index (last-log-index log))]
                 term))
        0))
  (indices-and-terms [log]
    (for [[index {:keys [term]}] log]
      [index term]))
  (add-to-log [log index entry]
    (assert (map? log))
    (assert (number? index))
    (assert (map? entry))
    (let [l (assoc log
              index (assoc entry :index index))]
      (assert (every? number? (keys log)))
      l))
  (log-entry-of [log index]
    (get log index))
  (entry-with-serial [log needle-serial]
    (first (for [[index {:keys [serial] :as entry}] log
                 :when (= serial needle-serial)]
             entry)))
  (delete-from [log index]
    (loop [log log
           index index]
      (if (contains? log index)
        (recur (dissoc log index) (inc index))
        log))))

(deftype LogChecker [log1 log2]
  Counted
  (log-count [_]
    (let [r1 (log-count log1)
          r2 (log-count log2)]
      (assert (= r1 r2) ["count" r1 r2])
      r1))
  RaftLog
  (log-contains? [log log-term log-index]
    (let [r1 (log-contains? log1 log-term log-index)
          r2 (log-contains? log2 log-term log-index)]
      (assert (= r1 r2) ["log-contains?" r1 r2])
      r1))
  (last-log-index [log]
    (let [r1 (last-log-index log1)
          r2 (last-log-index log2)]
      (assert (= r1 r2) ["last-log-index" r1 r2])
      r1))
  (last-log-term [log]
    (let [r1 (last-log-term log1)
          r2 (last-log-term log2)]
      (assert (= r1 r2) ["last-log-term" r1 r2])
      r1))
  (indices-and-terms [log]
    (let [r1 (indices-and-terms log1)
          r2 (indices-and-terms log2)]
      (assert (= (set r1) (set r2)) ["indices-and-terms" r1 r2])
      r1))
  (add-to-log [log index entry]
    (assert (map? entry) entry)
    (assert (number? index) index)
    (let [r1 (add-to-log log1 index entry)
          r2 (add-to-log log2 index entry)]
      (assert (= (set (indices-and-terms r1))
                 (set (indices-and-terms r2)))
              ["add-to-log"
               (set (indices-and-terms r1))
               (set (indices-and-terms r2))])
      (LogChecker. r1 r2)))
  (log-entry-of [log index]
    (let [r1 (log-entry-of log1 index)
          r2 (log-entry-of log2 index)]
      (assert (= r1 r2) ["log-entry-of" r1 r2])
      r1))
  (entry-with-serial [log serial]
    (let [r1 (entry-with-serial log1 serial)
          r2 (entry-with-serial log2 serial)]
      (assert (= r1 r2) ["entry-with-serial" r1 r2])
      r1))
  (delete-from [log index]
    (let [r1 (delete-from log1 index)
          r2 (delete-from log2 index)]
      (assert (= (set (indices-and-terms r1))
                 (set (indices-and-terms r2)))
              ["delete-from"
               (set/difference (set (indices-and-terms r1))
                               (set (indices-and-terms r2)))
               (set/difference (set (indices-and-terms r2))
                               (set (indices-and-terms r1)))])
      (LogChecker. r1 r2))))

(alter-meta! #'->LogChecker assoc :doc
             "satisfies RaftLog, takes two things that satisfy RaftLog
             and runs all operations against both, checking one
             against the other")

;; document log entry format
(defrecord LogEntry [return index term payload operation-type serial])
