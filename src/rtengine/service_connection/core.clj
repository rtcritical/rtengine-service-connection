(ns rtengine.service-connection.core
  (:require [clojure.java.io :as io])
  (:import [java.util.concurrent Executors ThreadFactory]
           [java.net ServerSocket Socket SocketException InetAddress]
           [java.io BufferedReader InputStreamReader BufferedWriter OutputStreamWriter Closeable]))

;; designed as one request per connection
(defn accept [^Socket conn single-arg-processing-fn]
  (try
    (with-open [rd (BufferedReader. (InputStreamReader. (.getInputStream conn)))
                wr (BufferedWriter. (OutputStreamWriter. (.getOutputStream conn)))]
      (let [line (.readLine rd)
            arg (read-string line)
            result (single-arg-processing-fn arg)]
        (doto wr
          (.write (str (pr-str result) "\n"))
          (.flush))))
    (finally
      (.close conn))))

(defn counted-thread-factory
  "Create a ThreadFactory that maintains a counter for naming Threads.
     name-format specifies thread names - use %d to include counter
     daemon is a flag for whether threads are daemons or not"
  ([name-format daemon]
   (let [counter (atom 0)]
     (reify
       ThreadFactory
       (newThread [this runnable]
         (doto (Thread. ^Runnable runnable)
           (.setName (format name-format (swap! counter inc)))
           (.setDaemon daemon)))))))

(defn accept-thread-pool [name]
  ;; threads should be user/non-daemon threads, so jvm does not kill these off.
  ;; they need to finish before jvm shutdown
  (let [daemon false]
    (Executors/newCachedThreadPool
     (counted-thread-factory (str "fn-socket-server-accept-" name "-%d")
                             daemon))))

(defrecord ServiceServer
    [server-socket accept-thread-pool service-fn fqsn bind-address port]
  java.io.Closeable
  (close [this]
    (.close server-socket)
    (.shutdown accept-thread-pool)))

(defmacro ^:private thread
  [^String name daemon & body]
  `(doto (Thread. (fn [] ~@body) ~name)
    (.setDaemon ~daemon)
    (.start)))

;; current design - one thread per client, one request per connection
;; roughly 1.0 ms latency, with parallel capability up to # of cores
(defn service-server [service-fn fqsn port
                      & {:keys [bind-address]}]
  (let [server-daemon true
        address (InetAddress/getByName bind-address) ; nil returns loopback
        server-socket (ServerSocket. port 0 bind-address)
        accept-thread-pool (accept-thread-pool name)]
    (thread
      (str "Fn-Socket-Server " fqsn) server-daemon
      (while (not (.isClosed server-socket))
        (try
          (let [conn (.accept server-socket)]
            (.execute accept-thread-pool
                      #(accept conn service-fn)))
          (catch SocketException _disconnect))))
    (->ServiceServer server-socket accept-thread-pool service-fn fqsn bind-address port)))

(defn execute [port form & {:keys [host]
                            :or {host "127.0.0.1"}}]
  (with-open [socket (Socket. host port)
              rd (io/reader socket)
              wr (io/writer socket)]
    (doto wr
      (.write (str (pr-str form) "\n"))
      (.flush))
    (read-string (.readLine rd))))
