(ns metabase.async.semaphore-channel
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [metabase.util.i18n :refer [trs]])
  (:import java.io.Closeable
           java.lang.ref.WeakReference))

(defn- permit [id return!]
  (let [closed? (atom false)]
    (reify
      Object
      (toString [this]
        (format "Permit #%d" id))

      Closeable
      (close [this]
        (when-not @closed?
          (when (compare-and-set! closed? false true)
            (return! id)))
        nil))))

(defn semaphore-channel
  "Create a new channel that acts as a counting semaphore, containing `n` permits. Use `<!` or the like to get a permit
  from the semaphore channel. The permit should be returned by calling `.close` or by using `with-open`."
  [n]
  (let [weak-refs  (atom {})
        permits    (a/chan n)
        id-counter (atom 0)
        in         (a/chan n)
        out        (a/chan 1)

        create-new-permit!
        (fn []
          (let [id       (swap! id-counter inc)
                permit   (permit id #(a/>!! in %))
                weak-ref (WeakReference. permit)]
            (log/debug (trs "Created {0}" permit))
            (swap! weak-refs assoc id weak-ref)
            (a/>!! permits permit)
            nil))]


    (dotimes [_ n]
      (create-new-permit!))

    ;; loop to handle returned permits
    (a/go-loop [id (a/<! in)]
      (log/debug (trs "Permit {0} returned nicely" id))
      (let [[old] (swap-vals! weak-refs dissoc id)]
        (when (get old id)
          (create-new-permit!)))
      (recur (a/<! in)))

    ;; loop to take returned permits and make them available to the `out` channel consumed elsewhere, or to clean up
    (a/go-loop [permit (a/poll! permits)]
      (if permit
        (do
          (a/>! out permit)
          (recur (a/poll! permits)))

        ;; Out of permits. Cleanup time!
        (do
          (log/debug (trs "Initiating clean up of abandoned permits. Total weak refs: {0}/{1}" (count @weak-refs) n))
          (doseq [[id, ^WeakReference weak-ref] @weak-refs]
            (log/debug (trs "Check permit {0} abandoned? {1}" id (nil? (.get weak-ref))))
            (when-not (.get weak-ref)
              (log/warn (trs "Warning: abandoned Permit {0} recovered. Make sure you're closing permits when done." id))
              (let [[old] (swap-vals! weak-refs dissoc id)]
                (when (get old id)
                  (create-new-permit!)))))
          (recur (a/<! permits)))))

    out))

(defn- closed-or-has-message?
  "True if a core.async channel is open and doesn't have any messages ready."
  ;; Check whether the channel is closed by attempting to take a value from it. If channel is already closed `alts!!`
  ;; will return `nil`. Otherwise if it already has a value, unless you are trying to intentionally break things, it
  ;; won't be `::open`.
  [chan]
  (let [[v _] (a/alts!! [chan] :default ::open)]
    (not= v ::open)))

(defn do-f-after-receiving-permit-fn
  "Return a function to call once we get a permit. Runs `f` on a background thread and returns the token when finished.
  If `result-chan` is already closed, or already has a message written to it, returns permit immediately and skips
  running `f`. "
  [semaphore-chan result-chan f args]
  (fn [permit]
    (if (closed-or-has-message? semaphore-chan)
      ;; channel closed, return the permit
      (.close permit)
      ;; If results channel is *not* yet closed...
      ;;
      ;; Create a new channel we'll use to keep track of whether we finished things on our own terms. If this channel
      ;; closes/gets a message before the result-channel we'll know we finished things on our terms
      (let [finished-chan (a/chan 1)
            ;; and fire off a future to run the query on a separate thread.
            futur         (future
                            (with-open [permit permit]
                              (try
                                (let [result (apply f args)]
                                  ;; if query completes, close the finished chan to release the go block below, and
                                  ;; write the result to the result-chan
                                  (a/close! finished-chan)
                                  (a/put! result-chan result))
                                ;; if we catch an Exception (shouldn't happen in a QP query, but just in case), still
                                ;; close the finished chan, but write the Exception to the result-chan. It's ok, our
                                ;; IMPL of Ring StreamableResponseBody will do the right thing with it.
                                (catch Throwable e
                                  (a/close! finished-chan)
                                  (a/put! result-chan e)))))]
        ;; meanwhile, schedule a go block to wait for either finished-chan or result-chan to close or get a message
        (a/go
          ;; chan is whichever one completes first
          (let [[_ chan] (a/alts! [finished-chan result-chan])]
            ;; if result-chan completes before finished-chan, either it was closed, or someone besides us sent it a
            ;; message. Cancel the future running the request.
            (when (= chan result-chan)
              (future-cancel futur))
            ;; Close the channels if they're not already closed.
            (a/close! finished-chan)
            (a/close! result-chan)))))))
