(ns streams-clj.core)

(defmacro stream [n]
  (range n))

(def stream-nil (range 0))
(def stream-1 (range 1))
(def stream-2 (range 2))
(def stream-3 (range 3))

;;for now assume streams are simple singleton sequences
;;so windows are defined in terms of the number of tuples

;difference here is that we would consume the chunk based on real-time or timestamps in the tuple
(defn- default-stream-consumer [n-tuples] (partial take n-tuples))

(println () (default-stream-consumer 2) (range 10))

(defn- stream-take-chunk
  ([stream] (apply (default-stream-consumer 1) stream)) 
  ([stream stream-consumer] (apply stream-consumer stream))) ;;  (take emit-freq-tuples stream)

(defn- stream-advance-window [window steps]
  (drop-last steps window))

(defn- compute-over-window [window f] (reduce f [] window))

(defn- compute-over-window-collection [windows f] (reduce f [] windows))

(defn- max-collection-len
  ([c1] (count c1))
  ([c1 & cols] (apply max (map count (reduce conj [c1] cols))))
  )
;tests
(def x (max-collection-len [1]))
(println x)
(def x (max-collection-len [1] [1 2]))
(println x)
(def x (max-collection-len [1] [1 2] [1 2 3]))
(println x)

(defn- coll-pad-end [c1 len elem]
  (let [pad-len (- len (count c1))]
    ;; TODO if it's negative that means we have to trim should probably throw an exception
    (if (> pad-len 0)
      (apply (partial conj c1) (repeat pad-len elem))
      c1
      )
    )
  )
;;tests
(println (= 10 (count (coll-pad-end [1] 10 nil))))
(println (= 1 (count (coll-pad-end [1] 1 nil))))

(defn- cross-product
  ([c1 c2] (for [row c1 col c2] (vec [row col])))
  ([c1 c2 & cols] nil)
  )
;;tests
;;equal sized lists
(println (cross-product [1 2] [3 4]))
;;un-equal sized lists
(println (cross-product [1 2] [3]))

(defn- cross-product-full-outer
  ([c1 c2] (
            let [pad-len (max-collection-len c1 c2)
                 c1-pad (coll-pad-end c1 pad-len nil)
                 c2-pad (coll-pad-end c2 pad-len nil)]
             (println pad-len c1-pad c2-pad)
;;             (def c1-pad (coll-pad-end c1 pad-len nil))
;;             (def c2-pad ())
;;            for [row c1 col c2] (vec [row col])
             ))
  ([c1 c2 & cols] nil)
  )
;;tests
;;equal sized lists
(println (cross-product-full-outer [1 2] [3 4]))
;;un-equal sized lists
(println (cross-product-full-outer [1 2] [3]))


;;(def next-chunk (stream-take-chunk stream-3 2))
;;(println next-chunk)

                                        ;; Psueode Code for 1:1 Bolt
                                        ; start:
                                        ; get next_chunk
                                        ; if chunk_list < window_size:
                                        ;      append chunk to chunk_list
                                        ; else:
                                        ;       drop oldest chunk
                                        ;       subtract last_chunk from last_result
                                        ; add next_chunk + last_result => last_result
                                        ; if chunk list >= window size:
                                        ;       emit last_result
                                        ; goto start:


                                        ;; Psueode Code for N:1 Bolt
                                        ; start:
                                        ; for ith_stream in streams:
                                        ;    get next_chunk => ith_window 
                                        ;
                                        ;    if ith_chunk_list < ith_window_size:
                                        ;      append ith_chunk to ith_chunk_list
                                        ;    else:
                                        ;       drop ith_oldest_chunk
                                        ;       subtract ith_oldest_chunk from last_result => last_result
                                        ;
                                        ; for ith_next_chunk in next_chunks:
                                        ;     add chunk_0 chunk_1 chunk_N last_result => last_result
                                        ; if chunk list >= window size:
                                        ;       emit last_result
                                        ; goto start:


