(ns rtengine.service-connection.core-test
  (:require [clojure.test :refer :all]
            [rtengine.service-connection.core :refer :all])
  (:import [java.util.concurrent Executors]))

(deftest execute-inc
  (testing "executing #'inc through a server connection"
    (let [port 11000]
      (with-open [ss (service-server #'inc
                                   "rtengine.test-service.increment"
                                   port)]
        (is (= 2 (execute port 1)))))))

(deftest ^:performance execute-latency-parallel
  (testing "executing #'inc in parallel, asserting < 2 ms latency"
    (let [port 11000]
      (with-open [ss (service-server #'inc
                                   "rtengine.test-service.increment"
                                   port)]
        (let [pool-size 4
              tp (Executors/newFixedThreadPool pool-size)
              tasks-count 10000
              tasks (for [n (range tasks-count)]
                      #(execute port n))
              start (System/currentTimeMillis)
              futures (map deref (.invokeAll tp tasks))
              duration (- (System/currentTimeMillis) start)
              latency-per-task (* pool-size (/ duration tasks-count))]
          (println (format "latency-per-task: %3s ms" (double latency-per-task)))
          (is (< latency-per-task 2)
              "less than 2 milliseconds latency per task in parallel"))))))
