(ns com.manigfeald.raft.log)

(defprotocol RaftLog
  (log-contains? [log log-term log-index])
  (last-log-index [log])
  (last-log-term [log])
  (indices-and-terms [log])
  (serial-exists? [log serial])
  (add-to-log [log index entry])
  ;; for strict "raft" make this operation a no op
  (rewrite-terms-after [log index new-term])
  (log-entry-of [log index])
  ;; don't need this?
  )

(defprotocol Counted
  (log-count [_]))

(defrecord ListLog [lst serials]
  Counted
  (log-count [_]
    (count lst))
  RaftLog
  (log-contains? [log log-term log-index]
    (loop [[head & tail] lst]
      (cond
       (nil? head)
       false
       (and (= log-term (:term head))
            (= log-index (:index head)))
       true
       :else
       (recur tail))))
  (last-log-index [log]
    (or (:index (first lst)) 0))
  (last-log-term [log]
    (or (:term (first lst)) 0))
  (indices-and-terms [log]
    (for [entry lst]
      [(:index entry) (:term entry)]))
  (serial-exists? [log serial]
    (contains? serials serial))
  (add-to-log [log index entry]
    (->ListLog (conj (doall (for [entry lst
                                  :when (not= index (:index entry))]
                              entry))
                     (assoc entry
                       :index index))
               (conj serials (:serial entry))))
  (rewrite-terms-after [log index new-term]
    (->ListLog (for [entry lst]
                 (if (> (:index entry) index)
                   (assoc entry :term new-term)
                   entry))
               serials))
  (log-entry-of [log needle-index]
    (first
     (for [{:keys [index] :as entry} lst
           :when (= index needle-index)]
       entry))))

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
  (serial-exists? [log needle-serial]
    (boolean (seq (for [[index {:keys [serial]}] log
                        :when (= serial needle-serial)]
                    index))))
  (add-to-log [log index entry]
    (assoc log
      index (assoc entry :index index)))
  (rewrite-terms-after [log target-index new-term]
    (into log (for [[index entry] log]
                (if (> index target-index)
                  [index (assoc entry :term new-term)]
                  [index entry]))))
  (log-entry-of [log index]
    (get log index)))

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
      (assert (= r1 r2) ["indices-and-terms" r1 r2])
      r1))
  (serial-exists? [log serial]
    (let [r1 (serial-exists? log1 serial)
          r2 (serial-exists? log2 serial)]
      (assert (= r1 r2) ["serial-exists?" r1 r2])
      r1))
  (add-to-log [log index entry]
    (let [r1 (add-to-log log1 index entry)
          r2 (add-to-log log2 index entry)]
      (assert (= (set (indices-and-terms r1))
                 (set (indices-and-terms r2))) ["add-to-log" r1 r2])
      (LogChecker. r1 r2)))
  (rewrite-terms-after [log index new-term]
    (let [r1 (rewrite-terms-after log1 index new-term)
          r2 (rewrite-terms-after log2 index new-term)]
      (assert (= (set (indices-and-terms r1))
                 (set (indices-and-terms r2)))
              ["rewrite-terms-after" r1 r2])
      (LogChecker. r1 r2)))
  (log-entry-of [log index]
    (let [r1 (log-entry-of log1 index)
          r2 (log-entry-of log2 index)]
      (assert (= r1 r2) ["log-entry-of" r1 r2])
      r1)))
