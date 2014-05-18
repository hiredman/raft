(ns com.thelastcitadel.raftII
  (:refer-clojure :exclude [== record?])
  (:require [clojure.core.logic :refer :all]
            [clojure.core.logic.arithmetic :as math]))


(defn go [current-term voted-for log commit-index last-applied next-index match-index message state output id
          last-log-index last-log-term votes]
  (run 1 [output]
       (fresh [out-output
               out-current-term
               out-voted-for
               out-log
               out-commit-index
               out-last-applied
               out-next-index
               out-match-index
               out-state
               out-last-log-index
               out-last-log-term
               out-votes]
              (== output
                  {:last-log-index out-last-log-index
                   :last-log-term out-last-log-term
                   :current-term out-current-term
                   :voted-for out-voted-for
                   :log out-log
                   :commit-index out-commit-index
                   :last-applied out-last-applied
                   :next-index out-next-index
                   :match-index out-match-index
                   :state out-state
                   :out out-output
                   :id id
                   :votes out-votes})
              (conde
               ;; [;; TODO: message term > current-term
               ;;  ]
               [(featurec message {:type :timeout})
                (conde
                 [(== state :follower)]
                 [(== state :candidate)])
                (conso {:target :broadcast
                        :type :request-vote
                        :term out-current-term
                        :candidate-id id
                        :last-log-index last-log-index
                        :last-log-term last-log-term}
                       output out-output)
                (== out-last-log-term last-log-term)
                (== out-last-log-index last-log-index)
                (== out-current-term (inc current-term))
                (== out-voted-for nil)
                (== out-log log)
                (== out-commit-index commit-index)
                (== out-last-applied last-applied)
                (== out-match-index match-index)
                (== out-state :candidate)]
               [(featurec message {:type :request-vote})
                (conde
                 [(== state :follower)]
                 [(== state :candidate)])
                (conde
                 [(== voted-for nil)
                  (== voted-for id)])
                (== out-current-term current-term)
                (fresh [grant-vote? message-term candidate-id m-last-log-index
                        m-last-log-term foo]
                       (featurec message {:term message-term
                                          :candidate-id candidate-id})
                       (conso {:type :request-vote-response
                               :target candidate-id
                               :term out-current-term
                               :vote? grant-vote?}
                              output out-output)
                       (conde
                        [(math/= out-current-term message-term)
                         (== out-voted-for candidate-id)
                         (== grant-vote? true)
                         (featurec foo {:index m-last-log-index
                                        :term m-last-log-term})
                         (membero foo log)]
                        [(== out-voted-for voted-for)
                         (== grant-vote? false)]))
                (== out-log log)
                (== out-commit-index commit-index)
                (== out-last-applied last-applied)
                (== out-match-index match-index)
                (== out-state state)]
               [(fresh [vote?]
                       (featurec message {:type :request-vote-response
                                          :vote? vote?}))
                ]))))
