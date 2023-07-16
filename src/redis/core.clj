(ns redis.core 
  (:require [clojure.java.io :as io])
  (:import [java.net ServerSocket])
  (:gen-class))

(defn receive-message
  "Read a line of textual data from the given socket"
  [socket]
  (let [reader (io/reader socket)
        first-line (.readLine reader)]
    (when first-line
      (loop [line first-line
             res []]
        (if (.ready reader)
          (recur (.readLine reader) (conj res line))
          (conj res line))))))

(defn send-message
  "Send the given string message out over the given socket"
  [socket msg]
  (let [writer (io/writer socket)]
    (.write writer msg)
    (.flush writer)))

(defn handle-client [socket handler]
  (with-open [sock socket]
    (while true
      (let [msg-in (receive-message socket)
            msg-out (handler msg-in)]
        (send-message sock msg-out)))))

(defn serve [port handler]
  (with-open [server-sock (ServerSocket. port)]
    (. server-sock (setReuseAddress true))
    (while true
      (let [sock (.accept server-sock)]
        (future (handle-client sock handler))))))

(def storage (atom {}))

(defn handler
  [msg-in]
  (println "msg-in: " msg-in)
  (case (get msg-in 2)
    "ping" "+PONG\r\n"
    "echo" (format "+%s\r\n" (get msg-in 4))
    "set"  (do 
             (let [k (get msg-in 4)
                   v (get msg-in 6)]
               (println "adding [k,v]" [k v])
               (swap! storage assoc k v))
             "+OK\r\n")
    "get"  (format "+%s\r\n" (@storage (get msg-in 4)))
    :else "+ERROR\r\n"))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;; You can use print statements as follows for debugging, they'll be visible when running tests.
  (println "Logs from your program will appear here!")
  ;; Uncomment this block to pass the first stage
  (serve 6379 handler)
  )

(comment
  (serve 6379 handler)
  ;;
  )
