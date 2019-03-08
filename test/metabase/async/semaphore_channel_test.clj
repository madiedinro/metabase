(ns metabase.async.semaphore-channel-test)


;; TODO

;; NOCOMMIT
#_(defn- y [c]
  (println "[1] in another function" (a/<!! c)))

#_(defn- x []
  (let [c (semaphore-channel 3)]
    (y c)
    (try
      (with-open [permit (a/<!! c)]
        (println "[2] with-open" permit) ; NOCOMMIT
        )
      (with-open [permit (a/<!! c)]
        (println "[3] with-open" permit) ; NOCOMMIT
        )
      (println "[4] in same fn" (a/<!! c))
      (println "[5] in same fn" (a/<!! c))
      (let [[result] (a/alts!! [c] :default ::not-available)]
        (println "[6] in same fn [poll]" result)
        (assert (= result ::not-available)))
      (System/gc)
      (let [[result] (a/alts!! [c] :default ::not-available)]
        (println "[7] in same fn [poll]" result)
        (assert (not= result ::not-available)))
      (let [p8 (a/<!! c)
            _ (println "[8] let-bound" p8)
            p9 (a/<!! c)
            _ (println "[9] let-bound" p9)]
        (System/gc)
        (let [[result] (a/alts!! [c] :default ::not-available)]
          (println "[10] in same fn [poll]" result)
          (assert (not= result ::not-available))))


      (finally (a/close! c)))))
