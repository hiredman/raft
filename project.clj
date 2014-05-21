(defproject com.thelastcitadel/raft "0.1.0-SNAPSHOT"
  :description "abstract raft algorithm written in clojure"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :profiles {:dev {:dependencies [[org.clojure/core.async "0.1.303.0-886421-alpha"]
                                  [org.clojure/tools.logging "0.2.6"]
                                  ;;[org.slf4j/slf4j-nop "1.7.2"]
                                  [org.easytesting/fest-assert "1.4"]
                                  [junit "4.8.2"]
                                  [ch.qos.logback/logback-classic "1.0.9"]
                                  [ch.qos.logback/logback-core "1.0.9"]
                                  [org.slf4j/jcl-over-slf4j "1.7.2"]]}})
