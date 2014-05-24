(ns com.manigfeald.raft-test
  (:require [clojure.test :refer :all]
            [com.manigfeald.raft :refer :all]
            [clojure.core.async :refer [alt!! timeout <!! >!! chan
                                        sliding-buffer dropping-buffer
                                        close!]]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]))

(in-ns 'clojure.test)

(def original-test-var  test-var)

(defn test-var [v]
  (clojure.tools.logging/trace "testing" v)
  (let [start (System/currentTimeMillis)
        result (original-test-var v)]
    (clojure.tools.logging/trace
     "testing" v "took"
     (/ (- (System/currentTimeMillis) start) 1000.0)
     "seconds")
    result))

(def original-test-ns test-ns)

(defn test-ns [ns]
  (clojure.tools.logging/trace "testing namespace" ns)
  (let [start (System/currentTimeMillis)
        result (original-test-ns ns)]
    (clojure.tools.logging/trace
     "testing namespace" ns "took"
     (/ (- (System/currentTimeMillis) start) 1000.0)
     "seconds")
    result))

(in-ns 'com.manigfeald.raft-test)

;; stop sf4j or whatever from printing out nonsense when you run tests
(log/info "logging is terrible")

(defprotocol Cluster
  (add-node [cluster node arg1])
  (remove-node [cluster node])
  (list-nodes [cluster])
  (send-to [cluster node msg]))

(defn v [channel to r]
  (alt!!
   channel ([message] message)
   (timeout to)
   ([_] r)))



(defn run
  ([in id cluster]
     (run in id cluster identity))
  ([in id cluster callback]
     (loop [state (raft id (conj (set (list-nodes cluster)) id))
            callbacks {}]
       (let [message (alt!!
                      in ([message] message)
                      (timeout
                       (if (= :leader (:node-type (:raft-state state)))
                         300
                         (+ 1000 (* 1000 (rand-int 3)))))
                      ([_] {:type :timeout
                            :term 0}))]
         (cond
          (= :await (:type message))
          (recur state (update-in callbacks [(:serial message)]
                                  conj (:callback message)))
          (= :leader? (:type message))
          (do
            ((:callback message) (:leader-id (:raft-state state)))
            (recur state callbacks))
          :else
          (let [new-state (run-one state message)
                _ (callback state new-state)
                _ (doseq [msg (:out-queue new-state)]
                    (assert (not= :broadcast (:target msg)))
                    (send-to cluster (:target msg) msg))
                _ (doseq [{:keys [level message] :as m}
                          (:running-log new-state)]
                    (case level
                      :trace (log/trace message)))
                new-state (update-in new-state [:out-queue] empty)
                new-state (update-in new-state [:running-log] empty)
                callbacks (reduce
                           (fn [cbs fun] (fun cbs))
                           callbacks
                           (for [{:keys [serial] :as entry}
                                 (:applied new-state)
                                 :when (contains? callbacks serial)
                                 callback (get callbacks serial)]
                             (fn [callbacks]
                               (callback entry)
                               (dissoc callbacks serial))))]
            (if (:stopped? new-state)
              new-state
              (recur new-state callbacks))))))))

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

(defn f [n l]
  (let [leaders (atom {})
        commited (atom {})
        value (atom {})
        m (into {} (for [i (range n)]
                     [i (chan (sliding-buffer 100))]))
        cluster (reduce #(add-node % (key %2) (val %2)) (->ChannelCluster) m)
        dump (atom {})]
    [leaders
     (doall (for [[id in] m
                  :let [lock (java.util.concurrent.Semaphore. 1)
                        s (atom nil)]]
              (assoc {}
                :lock lock
                :id id
                :in in
                :cluster cluster
                :future
                (future
                  (try
                    (run in id cluster
                         (fn [old-state state]
                           (.acquire lock)
                           (reset! s state)
                           (when (get @dump id)
                             (log/trace state)
                             (swap! dump assoc id false))
                           (try
                             (when (not= (:leader-id (:raft-state old-state))
                                         (:leader-id (:raft-state state)))
                               (swap! leaders assoc
                                      (:id state)
                                      (:leader-id (:raft-state state)))
                               (>!! l (:leader-id (:raft-state state))))
                             (swap! value assoc
                                    (:id state)
                                    (:value (:raft-state state)))
                             (doseq [entry (:log (:raft-state state))
                                     :when (:serial entry)
                                     :when
                                     (>= (:commit-index (:raft-state state))
                                         (:index entry))]
                               (swap! commited assoc-in
                                      [(:id state) (:serial entry)] entry))

                             (catch Throwable t
                               (prn t))
                             (finally
                               (.release lock)))))
                    (catch Throwable e
                      (log/error @s)
                      (doseq [n (keys m)]
                        (swap! dump assoc n true))
                      (log/error (type e) (.getMessage e))))))))
     commited
     value]))

(def n 1000)

(defn shut-it-down! [nodes]
  (doseq [i nodes]
    (future-cancel (:future i))))

(defn leaders-of [nodes n]
  (loop []
    (let [lead (seq (for [node nodes
                          :let [c (chan 1)
                                _ (>!! (:in node)
                                       {:type :leader?
                                        :callback
                                        (fn [leader-id]
                                          (>!! c (or leader-id :none)))})
                                r (alt!!
                                   c ([m] m)
                                   (timeout 1000) ([_] :none))]
                          :when (not= r :none)]
                      r))]
      (if (>= (count lead) n)
        lead
        (do
          (Thread/sleep 1000)
          (recur))))))

(defn stable-leader? [nodes n]
  (loop [i 100]
    (if (zero? i)
      false
      (let [n (leaders-of nodes n)]
        (if (apply = n)
          true
          (recur (dec i)))))))

(deftest test-election
  (let [leader (chan (dropping-buffer 10))
        [leaders nodes] (f 3 leader)]
    (try
      (is (apply = (leaders-of nodes 3)) @leaders)
      (finally
        (shut-it-down! nodes)))))

(deftest test-remove-node
  (let [leader (chan (dropping-buffer 10))
        [leaders nodes] (f 5 leader)]
    (try
      (testing "elect leader"
        (is (stable-leader? nodes 5)))
      (testing "kill leader and elect a new one"
        (let [[leader'] (leaders-of nodes 1)
              c (chan (sliding-buffer 10))]
          (doseq [node nodes
                  :when (= leader' (:id node))]
            (>!! (:in node) {:type :operation
                             :op :write
                             :node leader'
                             :operation-type :remove-node
                             :serial 1})
            (doseq [node nodes
                    :when (not (future-done? (:future node)))]
              (let [c (chan)]
                (>!! (:in node) {:type :await
                                 :callback (fn [_]
                                             (close! c))
                                 :serial 1})
                (v c (* 1000 60) :timeout)))
            (>!! (:in node) {:type :stop :term 0}))
          (let [nodes (for [node nodes
                            :when (not= (:id node) leader')]
                        node)]
            (is (stable-leader? (for [node nodes
                                      :when (not (future-done? (:future node)))]
                                  node)
                                4))
            (testing "kill leader again and elect a new one"
              (let [[leader] (leaders-of nodes 1)]
                (doseq [node nodes
                        :when (= leader (:id node))]
                  (>!! (:in node) {:type :operation
                                   :op :write
                                   :node leader
                                   :operation-type :remove-node
                                   :serial 2})
                  (doseq [node nodes
                          :when (not (future-done? (:future node)))]
                    (let [c (chan)]
                      (>!! (:in node) {:type :await
                                       :callback (fn [_]
                                                   (close! c))
                                       :serial 2})
                      (v c (* 1000 60) :timeout)))
                  (>!! (:in node) {:type :stop})))
              (is (stable-leader? (for [node nodes
                                        :when (not (future-done? (:future node)))]
                                    node)
                                  3))))))
      (finally
        (shut-it-down! nodes)))))

(deftest test-operations
  (let [leader (chan (dropping-buffer 10))
        [leaders nodes commited value] (f 5 leader)]
    (try
      (testing "elect leader"
        (is (stable-leader? nodes 5)))
      (let [[leader] (leaders-of nodes 5)
            c (chan 2)]
        (doseq [node nodes
                :when (= leader (:id node))]
          (>!! (:in node) {:type :operation
                           :op :write
                           :key "hello"
                           :value "world"
                           :operation-type :write
                           :serial 1})
          (>!! (:in node) {:type :await
                           :serial 1
                           :callback (fn [e]
                                       (close! c))}))
        (is (not= :timeout (v c (* 60 1000) :timeout))))
      (Thread/sleep 1000)
      (doseq [[node commited-entries] @commited]
        (is (contains? commited-entries 1)))
      (is (apply = (vals @value)))
      (doseq [[node value] @value]
        (is (= (get value "hello") "world")))
      (finally
        (shut-it-down! nodes)))))

(deftest test-read-operations
  (let [leader (chan (dropping-buffer 2))
        [leaders nodes commited value] (f 3 leader)]
    (try
      (testing "elect leader"
        (is (stable-leader? nodes 3)))
      (let [leader (first (vals @leaders))
            c (chan 1)]
        (doseq [node nodes
                :when (= leader (:id node))]
          (>!! (:in node) {:type :operation
                           :op :write
                           :key "hello"
                           :value "world"
                           :operation-type :write
                           :serial 1})
          (>!! (:in node) {:type :operation
                           :op :write
                           :key "hello"
                           :value "bob"
                           :operation-type :write
                           :serial 2})
          (>!! (:in node) {:type :operation
                           :op :read
                           :key "hello"
                           :operation-type :read
                           :serial 3})
          (>!! (:in node) {:type :await
                           :callback (fn [{:keys [value]}]
                                       (>!! c value))
                           :serial 3}))
        (is (= (v c (* 1000 60) :timeout) "bob")))
      (finally
        (shut-it-down! nodes)))))

(deftest test-kill-node
  (dotimes [i 8]
    (let [n (inc (* (inc i) 2))
          leader (chan (dropping-buffer 10))
          [leaders nodes] (f n leader)]
      (try
        (testing "elect leader"
          (is (stable-leader? nodes n)))
        (dotimes [ii (Math/floor (/ n 2))]
          (testing "kill leader and elect a new one"
            (let [[leader'] (leaders-of nodes 1)]
              (doseq [node nodes
                      :when (= leader' (:id node))]
                (future-cancel (:future node)))
              (is (stable-leader? (for [node nodes
                                        :when (not (future-done? (:future node)))]
                                    node)
                                  (- n (inc ii)))))))
        (finally
          (shut-it-down! nodes))))))

(defn raft-obj [in id cluster]
  (let [s (atom nil)
        lock (java.util.concurrent.Semaphore. 1)
        f (future
            (loop [state (raft id (conj (set (list-nodes cluster)) id))
                   callbacks {}]
              (let [to (if (= :leader (:node-type (:raft-state state)))
                         300
                         (+ 500 (* 100 (rand-int 100))))
                    ;; to (+ 300 (rand-int 1000))
                    ;; _ (log/trace id "timeout" to)
                    message (alt!!
                             in
                             ([message] message)
                             (timeout to)
                             ([_] {:type :timeout
                                   :term 0}))]
                (let [_ (.acquire lock)
                      new-state (try
                                  (run-one state message)
                                  (finally
                                    (.release lock)))
                      _ (doseq [msg (:out-queue new-state)]
                          (assert (not= :broadcast (:target msg)))
                          (send-to cluster (:target msg) msg))
                      _ (doseq [{:keys [level message] :as m}
                                (:running-log new-state)]
                          (case level
                            :trace (log/trace message)))
                      new-state (update-in new-state [:out-queue] empty)
                      new-state (update-in new-state [:running-log] empty)
                      callbacks (reduce
                                 (fn [cbs fun] (fun cbs))
                                 callbacks
                                 (for [{:keys [serial] :as entry}
                                       (:applied new-state)
                                       :when (contains? callbacks serial)
                                       callback (get callbacks serial)]
                                   (fn [callbacks]
                                     (callback entry)
                                     (dissoc callbacks serial))))]
                  (reset! s new-state)
                  (if (:stopped? new-state)
                    new-state
                    (recur new-state callbacks))))))]
    {:in in
     :id id
     :cluster cluster
     :raft s
     :lock lock
     :future f}))


(defn await-applied [nodes serial-w i dunno]
  (loop [i 100]
    (if (zero? i)
      dunno
      (if-let [v (seq (for [node nodes
                            :let [{:keys [raft]} node
                                  v (deref raft)
                                  {{:keys [last-applied log]} :raft-state} v]
                            [_ {:keys [index serial value]}] log
                            :when (= serial serial-w)
                            :when (>= last-applied index)]
                        value))]
        (first v)
        (do
          (Thread/sleep 100)
          (recur (dec i)))))))

(defn stable-leader?* [nodes n]
  (loop [i 100]
    (Thread/sleep 1000)
    (if (zero? i)
      false
      (let [lead (for [node nodes
                       :let [{:keys [raft]} node
                             v (deref raft)
                             {{:keys [leader-id]} :raft-state} v]
                       :when leader-id
                       leader nodes
                       :when (= leader-id (:id node))]
                   node)]
        (if (and (>= (count lead) n)
                 (apply = lead))
          (first lead)
          (recur (dec i)))))))

(defn raft-write [nodes key value]
  (log/trace "raft-write")
  (let [id (java.util.UUID/randomUUID)
        leader (stable-leader?* nodes 1)]
    (>!! (:in leader) {:type :operation
                       :op :write
                       :key key
                       :value value
                       :operation-type :write
                       :serial id})
    (when (= :dunno (await-applied nodes id 100 :dunno))
      (recur nodes key value))))

(defn raft-read [nodes key]
  (log/trace "raft-read")
  (let [id (java.util.UUID/randomUUID)
        leader (stable-leader?* nodes 1)]
    (if (not leader)
      (recur nodes key)
      (do
        (>!! (:in leader) {:type :operation
                           :op :read
                           :key key
                           :operation-type :read
                           :serial id})
        (let [r (await-applied nodes id 100 :dunno)]
          (if (= r :dunno)
            (recur nodes key)
            r))))))

(deftest test-stress
  (let [node-ids-and-channels (into {} (for [i (range 3)]
                                         [i (chan (sliding-buffer 10))]))
        cluster (reduce #(add-node % (key %2) (val %2)) (->ChannelCluster) node-ids-and-channels)
        nodes (doall (for [[node-id in] node-ids-and-channels]
                       (raft-obj in node-id cluster)))]
    (try
      (is (stable-leader?* nodes 3))
      (raft-write nodes :key 0)
      (dotimes [i 10]
        (log/trace "writing" i)
        (let [leader (stable-leader?* nodes 1)
              victim (rand-nth nodes)
              write-id (java.util.UUID/randomUUID)
              read-id (java.util.UUID/randomUUID)]
          (.acquire (:lock victim))
          (try
            (let [ii (raft-read nodes :key)]
              (raft-write nodes :key (inc ii))
              (is (= (inc ii) (raft-read nodes :key)))
              (is (= i ii)))
            (finally
              (.release (:lock victim))))
          (raft-read nodes :key)))
      (finally
        (shut-it-down! nodes)))))
