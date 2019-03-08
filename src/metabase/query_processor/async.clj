(ns metabase.query-processor.async
  "Async versions of the usual public query processor functions. Instead of blocking while the query is ran, these
  functions all return a `core.async` channel that can be used to fetch the results when they become available.

  Each connected database is limited to a maximum of 15 simultaneous queries (configurable) using these methods; any
  additional queries will park the thread. Super-useful for writing high-performance API endpoints. Prefer these
  methods to the old-school synchronous versions.

  How is this achieved? For each Database, we'll maintain a channel that acts as a counting semaphore; the channel
  will initially contain 15 permits. Each incoming request will asynchronously read from the channel until it acquires
  a permit, then put it back when it finishes."
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :as async.protocols]
            [metabase.async.semaphore-channel :as semaphore-channel]
            [metabase.models.setting :refer [defsetting]]
            [metabase.util.i18n :refer [trs]]
            [metabase
             [query-processor :as qp]
             [util :as u]]))

(defsetting max-simultaneous-queries-per-db
  (trs "Maximum number of simultaneous queries to allow per connected Database.")
  :type    :integer
  :default 15)



(defonce ^:private db-semaphore-channels (atom {}))

(defn- db-semaphore-channel
  "Fetch the counting semaphore channel for a Database, creating it if not already created."
  [database-or-id]
  (let [id (u/get-id database-or-id)]
    (or
     ;; channel already exists
     (@db-semaphore-channels id)
     ;; channel does not exist, Create a channel and stick it in the atom
     (let [ch     (semaphore-channel/semaphore-channel (max-simultaneous-queries-per-db))
           new-ch ((swap! db-semaphore-channels update id #(or % ch)) id)]
       ;; ok, if the value swapped into the atom was a different channel (another thread beat us to it) then close our
       ;; newly created channel
       (when-not (= ch new-ch)
         (.close ch))
       ;; return the newly created channel
       new-ch))))




(defn- do-async
  "Execute `f` asynchronously, waiting to receive a permit from `db`'s semaphore channel before proceeding. Returns the
  results in a channel."
  [db f & args]
  (let [semaphore-chan (db-semaphore-channel db)
        ;; open a channel to receive the result
        result-chan    (a/chan 1)
        do-f           (semaphore-channel/do-f-after-receiving-permit-fn semaphore-chan result-chan f args)]
    (a/go
      (do-f (a/<! semaphore-chan)))
    ;; return a channel that can be used to get the response, and one that can be used to cancel the request
    result-chan))

(defn process-query
  "Async version of `metabase.query-processor/process-query`. Runs query asynchronously, and returns a `core.async`
  channel that can be used to fetch the results once the query finishes running. Closing the channel will cancel the
  query."
  [query]
  (do-async (:database query) qp/process-query query))

(defn process-query-and-save-execution!
  "Async version of `metabase.query-processor/process-query-and-save-execution!`. Runs query asynchronously, and returns
  a `core.async` channel that can be used to fetch the results once the query finishes running. Closing the channel
  will cancel the query."
  [query options]
  (do-async (:database query) qp/process-query-and-save-execution! query options))

(defn process-query-and-save-with-max!
  "Async version of `metabase.query-processor/process-query-and-save-with-max!`. Runs query asynchronously, and returns
  a `core.async` channel that can be used to fetch the results once the query finishes running. Closing the channel
  will cancel the query."
  [query options]
  (do-async (:database query) qp/process-query-and-save-with-max! query options))

(defn process-query-without-save!
  "Async version of `metabase.query-processor/process-query-without-save!`. Runs query asynchronously, and returns a
  `core.async` channel that can be used to fetch the results once the query finishes running. Closing the channel will
  cancel the query."
  [user query]
  (do-async (:database query) qp/process-query-without-save! user query))
