(ns metabase.middleware.async
  (:require [cheshire.core :as json]
            [clojure.core.async :as a]
            [compojure.response :refer [Sendable]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [metabase.middleware.exceptions :as mw.exceptions]
            [metabase.util :as u]
            compojure.response
            [metabase.util.i18n :as ui18n :refer [trs]]
            [ring.core.protocols :as ring.protocols]
            [ring.util.response :as response])
  (:import clojure.core.async.impl.channels.ManyToManyChannel
           org.eclipse.jetty.io.EofException
           java.io.OutputStream
           java.io.Writer))

(def ^:private ^:const streaming-response-keep-alive-interval-ms
  "Interval between sending newline characters to keep Heroku from terminating requests like queries that take a long
  time to complete."
  (* 1 1000))

;; Handle ring response maps that contain a core.async chan in the :body key:
;;
;; {:status 200
;;  :body (a/chan)}
;;
;; and send strings (presumibly \n) as heartbeats to the client until the real results (a seq) is received, then
;; stream that to the client
(defn- write-keepalive-char!
  "Write a single newline keep-alive char to `out`. Returns `true` if a character was successfully written, `false` if
  writing failed because the request has been canceled or some other unknown reason. Does not throw Exceptions."
  [^Writer out]
  (try
    (.write out (str \newline))
    (.flush out)
    true
    (catch EofException e
      (log/info e (u/format-color 'yellow (trs "connection closed, canceling request")))
      false)
    (catch Throwable e
      (log/error e (trs "Unexepected error writing keep-alive char"))
      false)))

(defn- write-other! [chunk, ^Writer out]
  (with-open [out out]
    (cond
      ;; An error has occurred, let the user know
      (instance? Exception chunk)
      (json/generate-stream (mw.exceptions/api-exception-response chunk) out)

      ;; We've recevied the response, write it to the output stream and we're done
      (seqable? chunk)
      (do
        (println "generate-stream!")    ; NOCOMMIT
        (json/generate-stream chunk out)))))

(defn- with-open-async [])

(defn- write-channel-to-output-stream [chan, ^OutputStream output-stream]
  ;; Yes, this is not using `with-open`, but we're closing it in the go loop instead below.
  ;;
  ;; We have to be very careful to make sure there are no possibile situations where something can throw an Exception
  ;; without closing the writer
  (let [out (io/writer output-stream)]
    (a/go-loop [chunk (a/<! chan), keep-open-chan (a/chan 1)]
      (try
        (cond
          ;; not a keepalive message?
          (not= chunk ::keepalive)
          (write-other! chunk out)

          ;; If the latest chunk is `::keepalive`, attempt to write a char. If successful, `write-keepalive-char!`
          ;; will return truthy, and we can send a message to `keep-open-chan` to keep the writer open and recur
          (write-keepalive-char! chan output-stream)
          (a/>!! keep-open-chan ::keep-open)

          ;; otherwise if the keepalive char write failed (probably because request was canceled) then close our input
          ;; channel so it can cancel pending requests such as QP responses
          )

        (if
          (if

            (a/close! done-chan))

          )
        (finally
          (if (a/poll! keep-open-chan)
            (do
              (.close out)
              (a/close! keep-open-chan))
            (recur (a/<! chan) keep-open-chan)))))))

(extend-protocol ring.protocols/StreamableResponseBody
  ManyToManyChannel
  (write-body-to-stream [chan _ ^OutputStream output-stream]
    ;; Write the actual results on a separate thread because we don't want to waste precious core.async threads doing
    ;; I/O. Future threadpool is unbounded but this is only being triggered by QP responses [at the time of this
    ;; writing] so it is ultimately bounded by the number of concurrent QP requests
    (future
      (println #_log/debug (u/format-color 'green (trs "starting streaming response"))) ; NOCOMMIT
      (try
        (write-channel-to-output-stream chan output-stream)
        (catch Exception e
          (log/error e (trs "Unexpected exception writing streaming response"))
          (.close output-stream))))))

(defn- start-async-keepalive-loop [input-chan output-chan]
  ;; Start the async loop to wait for the response/write messages to the output
  (a/go-loop []
    (if-let [response (first (a/alts! [input-chan (a/timeout streaming-response-keep-alive-interval-ms)]))]
      ;; We have a response since it's non-nil, write the results and close up, we're done
      (do
        (println "response:" response)  ; NOCOMMIT
        ;; BTW if output-chan is closed, it's already too late, nothing else we need to do
        (a/>! output-chan response)
        (println #_log/debug (u/format-color 'blue (trs "Async response finished."))) ; NOCOMMIT
        (a/close! input-chan)
        (a/close! output-chan))
      ;; when we timeout...
      (do
        ;; a newline padding character as it's harmless and will allow us to check if the client is connected. If
        ;; sending this character fails because the connection is closed, the chan will then close. Newlines are
        ;; no-ops when reading JSON which this depends upon.
        (println #_log/debug (u/format-color 'blue (trs "Response not ready, writing one byte & sleeping..."))) ; NOCOMMIT
        (if (a/>! output-chan ::keepalive)
          ;; Successfully put the message channel, wait and see if we get the response next time
          (recur)
          ;; If we couldn't put the message, output-chan is closed, so go ahead and close input-chan too, which will
          ;; trigger query-cancelation in the QP (etc.)
          (a/close! input-chan))))))

(defn- async-keepalive-chan [input-chan]
  ;; Output chan only needs to hold on to the last message it got, for example no point in writing multiple `\n`
  ;; characters if the consumer didn't get a chance to consume them, and no point writing `\n` before writing the
  ;; actual response
  (let [output-chan (a/chan (a/sliding-buffer 1))]
    (start-async-keepalive-loop input-chan output-chan)
    output-chan))

(defn- async-keepalive-response [input-chan]
  (assoc (response/response (async-keepalive-chan input-chan))
    :content-type "applicaton/json"))

(extend-protocol Sendable
  ManyToManyChannel
  (send* [input-chan _ respond _]
    (respond (async-keepalive-response input-chan))))

(defn- wow-route [_ respond raise]
  (try
    (let [response-chan (a/chan 1)]
      (respond (async-keepalive-response response-chan))
      (a/go
        (loop [remaining-seconds 5]
          (a/<! (a/timeout 1000))
          (cond
            (!= ::proceed (first (a/alts!! [response-chan] :default ::proceed)))
            (println "Oop, channel closed! Bye!")

            (pos? remaining-seconds)
            (recur (dec remaining-seconds))

            :else
            (a/>! c {:wow "AMAZING"})))))
    (catch Throwable e
      (println "e:" e)                  ; NOCOMMIT
      (raise e))))

(def routes
  (let [handler (compojure.core/routes
                 (compojure.core/GET "/" [] "OK")
                 (compojure.core/GET "/wow" [] wow-route)
                 (compojure.core/GET "*" [] "NOT FOUND"))]
    (fn [{:keys [uri], :as request} respond raise]
      (handler
       request
       (fn [response]
         (println uri "response:" response) ; NOCOMMIT
         (respond response))
       raise))))

#_(def routes wow-route)

(defonce server
  (#'metabase.server/create-server #'routes {:port 5000, :async? true}))
