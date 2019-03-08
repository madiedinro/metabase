(ns metabase.async.api-response
  (:require [cheshire.core :as json]
            [clojure.core.async :as a]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [compojure.response :refer [Sendable]]
            [metabase.middleware.exceptions :as mw.exceptions]
            [metabase.util :as u]
            [metabase.util.i18n :as ui18n :refer [trs]]
            [ring.core.protocols :as ring.protocols]
            [ring.util.response :as response])
  (:import clojure.core.async.impl.channels.ManyToManyChannel
           java.io.OutputStream
           org.eclipse.jetty.io.EofException))

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
(defn- write-channel-to-output-stream [chan, ^OutputStream output-stream]
  (with-open [out (io/writer output-stream)]
    (try
      (loop [chunk (a/<!! chan)]
        (cond
          (= chunk ::keepalive)
          (do
            (try
              ;; a newline padding character as it's harmless and will allow us to check if the client is connected.
              ;; If sending this character fails because the connection is closed, the chan will then close. Newlines
              ;; are no-ops when reading JSON which this depends upon.
              (.write out (str \newline))
              (.flush out)
              (catch EofException e
                (log/debug e (u/format-color 'yellow (trs "connection closed, canceling request")))
                ;; close the channel so producers like the QP know they can stop what they're doing
                (a/close! chan)))
            (recur (a/<!! chan)))

          ;; An error has occurred, let the user know
          (instance? Exception chunk)
          (json/generate-stream (mw.exceptions/api-exception-response chunk) out)

          ;; We've recevied the response, write it to the output stream and we're done
          (seqable? chunk)
          (json/generate-stream chunk out))))))


(extend-protocol ring.protocols/StreamableResponseBody
  ManyToManyChannel
  (write-body-to-stream [chan _ ^OutputStream output-stream]
    ;; Write the actual results on a separate thread because we don't want to waste precious core.async threads doing
    ;; I/O. Future threadpool is unbounded but this is only being triggered by QP responses [at the time of this
    ;; writing] so it is ultimately bounded by the number of concurrent QP requests
    (future
      (log/debug (u/format-color 'green (trs "starting streaming response")))
      (write-channel-to-output-stream chan output-stream))))

(defn- start-async-keepalive-loop
  "Starts a go-loop that will send `::keepalive` messages to `output-chan` every second until `input-chan` either
  produces a response or one of the two channels is closed. If `output-chan` is closed (because there's no longer
  anywhere to write to -- the connection was canceled), closes `input-chan`; this can and is used by producers such as
  the async QP to cancel whatever they're doing."
  [input-chan output-chan]
  ;; Start the async loop to wait for the response/write messages to the output
  (a/go-loop []
    ;; check whether input-chan is closed or has produced a value, or time out after a second
    (let [[response chan] (a/alts! [input-chan (a/timeout streaming-response-keep-alive-interval-ms)])]
      (cond
        ;; We have a response since it's non-nil, write the results and close up, we're done
        (some? response)
        (do
          ;; BTW if output-chan is closed, it's already too late, nothing else we need to do
          (a/>! output-chan response)
          (log/debug (u/format-color 'blue (trs "Async response finished, closing channels.")))
          (a/close! input-chan)
          (a/close! output-chan))

        ;; if response was produced by `input-chan` but was `nil`, that means `input-chan` was unexpectedly closed. Go
        ;; ahead and write out an Exception rather than letting it sit around forever waiting for a response that will
        ;; never come
        (= chan input-chan)
        (do
          (log/error (trs "Input channel unexpectedly closed."))
          (a/>! output-chan (Exception. (str (trs "Input channel unexpectedly closed."))))
          (a/close! output-chan))

        ;; otherwise we've hit the one second timeout again. Write our byte & recur if output-channel is still open,
        ;; or close `input-chan` if it's not
        (log/debug (u/format-color 'blue (trs "Response not ready, writing one byte & sleeping...")))
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
