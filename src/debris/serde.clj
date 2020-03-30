(ns debris.serde
  (:require [clojure.string :as str])
  (:gen-class))

;; Debris is *not* intendend to do space-efficient or speed-efficient serialization.
;; It's main goal is to write *deterministic* binary data.
;; Debris -> Deterministic Binary Serializer.

;; For 0x0* types, the serialization is:
;; {type}{size in bytes}{value in bytes}

;; Important assumptions:
;; * All numeric values are decimals. Some can be coerced to more specific types:
;;   * 0x01|0x01|0x30 == (Int) 0
;;   * 0x01|0x03|0x30,0x2E,0x31 == (Double) 0.1
;;   * Note that coercion can and probably will hurt determinism. Avoid it.

;; 0x11,0x0C,0x01,0x01,0x01,0x01,0x01,0x00,0x03,0x01,0x2E,0x02,0x01,0x30 == {".": 0, true: false}

;; Collections are heterogeneous and inform their size in elements:

;; Bytes
(def ^:private byte-prefix (byte 0x00))
;; Boolean
(def ^:private bool-prefix (byte 0x01))
;; Short, Int, Long, Double, Decimal
(def ^:private num-prefix (byte 0x02))
;; Char, string
(def ^:private text-prefix (byte 0x03))

;;Set, bag, unordered
(def ^:private unordered-seq-prefix (byte 0x10))

;; Object, hash-map, dictionary, key-value pair
(def ^:private unordered-map-prefix (byte 0x11))

;; List, vector, array, tuple
(def ^:private ordered-seq-prefix (byte 0x20))

;; Ordered map
(def ^:private ordered-map-prefix (byte 0x20))

(def max-size 4294967295)

(defmulti serialize type)
(defmulti deserialize-value first)

(defn size-in-bytes [integer]
  (cond
    (neg? integer) (throw (ex-info "Invalid negative size"
                                   {:reason ::invalid-negative-size
                                    :supplied-size integer
                                    :max-size max-size}))
    (> integer max-size) (throw (ex-info "Unsupported size"
                                         {:reason ::unsupported-size
                                          :supplied-size integer
                                          :max-size max-size}))
    :else (byte-array [(unchecked-byte (bit-and 0xFF (bit-shift-right integer 24)))
                       (unchecked-byte (bit-and 0xFF (bit-shift-right integer 16)))
                       (unchecked-byte (bit-and 0xFF (bit-shift-right integer 8)))
                       (unchecked-byte (bit-and 0xFF integer))])))

(defn deserialize-size [^bytes barr] (BigInteger. 1 barr))

(comment
  (vec (.toByteArray (.toBigInteger 256N)))
  (BigInteger. 1 (byte-array (repeat 4 (unchecked-byte 0xFF))))
  )

(defn- serialize-byte [raw]
  (byte-array [byte-prefix (byte 0x00) (byte 0x00) (byte 0x00) (byte 0x1) raw]))

(defn- serialize-bytes [^bytes raw]
  (byte-array (concat
                [byte-prefix]
                (size-in-bytes (count raw))
                (vec raw))))

(defn- serialize-numbers [number]
  (let [bin (.getBytes (str number) "UTF8")]
    (byte-array (concat
                  [num-prefix]
                  (size-in-bytes (count bin))
                  bin))))

(defn- serialize-text [str-]
  (let [bin (.getBytes str- "UTF8")]
    (byte-array (concat
                [text-prefix]
                (size-in-bytes (count bin))
                bin))))

(defn- serialize-named [symbol-or-kw]
  (let [ns- (namespace symbol-or-kw)
        str-repr (str/join "/"
                           (cond->> [(name symbol-or-kw)]
                             (some? ns-) (cons ns-)))]
    (serialize-text str-repr)))

(defn- serialize-bool [bool]
  (byte-array [bool-prefix (byte 0x0) (byte 0x0) (byte 0x0) (byte 0x1) (byte (cond-> 0 bool inc))]))

(defn- serialize-maps [map-]
  (let [body (sort-by (comp vec first)
                      compare
                      (map (fn [[k v]] [(serialize k) (serialize v)]) map-))
        body (apply concat [] body)
        size (transduce (map count) + body)]
    (byte-array (apply concat [unordered-map-prefix] (size-in-bytes size) body))))

(defn- serialize-sets [set-]
  (let [body (sort-by vec
                      compare
                      (map serialize set-))
        size (transduce (map count) + body)]
    (byte-array (apply concat [unordered-seq-prefix] (size-in-bytes size) body))))

(defn- serialize-vecs [seq-]
  (let [body (map serialize seq-)
        size (transduce (map count) + body)]
    (byte-array (apply concat [ordered-seq-prefix] (size-in-bytes size) body))))

(defn- serialize-ordered-kvps [seq-]
  (let [body (mapcat (fn [[k v]] [(serialize k) (serialize v)]) seq-)
        size (transduce (map count) + body)]
    (byte-array (apply concat [ordered-seq-prefix] (size-in-bytes size) body))))

(defmethod serialize java.lang.Byte [data] (serialize-byte data))
(defmethod serialize (type (byte-array [])) [data] (serialize-bytes data))
(defmethod serialize java.lang.String [data] (serialize-text data))
(defmethod serialize clojure.lang.Keyword [data] (serialize-named data))
(defmethod serialize clojure.lang.Symbol [data] (serialize-named data))

(defmethod serialize clojure.lang.PersistentArrayMap [data] (serialize-maps data))
(defmethod serialize clojure.lang.PersistentHashMap [data] (serialize-maps data))
(defmethod serialize clojure.lang.PersistentStructMap [data] (serialize-maps data))

(defmethod serialize clojure.lang.PersistentHashSet [data] (serialize-sets data))
(defmethod serialize clojure.lang.PersistentTreeSet [data] (serialize-sets data))

(defmethod serialize clojure.lang.PersistentList [data] (serialize-vecs data))
(defmethod serialize clojure.lang.PersistentVector [data] (serialize-vecs data))
(defmethod serialize clojure.lang.PersistentQueue [data] (serialize-vecs data))

(defmethod serialize clojure.lang.PersistentTreeMap [data] (serialize-ordered-kvps data))

(defmethod serialize java.lang.Integer [data] (serialize-numbers data))
(defmethod serialize java.lang.Long [data] (serialize-numbers data))
(defmethod serialize java.lang.Double [data] (serialize-numbers data))
(defmethod serialize java.lang.Float [data] (serialize-numbers data))
(defmethod serialize java.math.BigDecimal [data] (serialize-numbers data))
(defmethod serialize java.math.BigInteger [data] (serialize-numbers data))

(defmethod serialize java.lang.Boolean [data] (serialize-bool data))

(defn deserialize-chunk [chunk-]
  (if (seq chunk-)
    (let [[tp & data] chunk-
          sz (deserialize-size (byte-array (take 4 data)))
          [payload next-] (split-at sz (drop 4 data))]
      [(deserialize-value [tp sz payload]) next-])
    []))

(defn deserialize [data]
  (first (deserialize-chunk data)))

(defmethod deserialize-value byte-prefix [[_ _ this]] this)

(defmethod deserialize-value num-prefix [[_ _ this]]
  (BigDecimal. (char-array (map char this))))

(defmethod deserialize-value bool-prefix [[_ _ this]]
  (= (byte 0x01) (first this)))

(defmethod deserialize-value unordered-map-prefix [[_ _ this]]
  (into {}
        (map vec)
        (partition 2 (loop [kvps []
                            [des nxt] (deserialize-chunk this)]
                       (cond-> kvps
                         (seq des) (conj des)
                         (seq nxt) (recur (deserialize-chunk nxt)))))))

(defmethod deserialize-value ordered-map-prefix [[_ _ this]]
  (into (sorted-map)
        (map vec)
        (partition 2 (loop [kvps []
                            [des nxt] (deserialize-chunk this)]
                       (cond-> kvps
                         (seq des) (conj des)
                         (seq nxt) (recur (deserialize-chunk nxt)))))))

(defmethod deserialize-value unordered-seq-prefix [[_ _ this]]
  (loop [vs #{}
         [des nxt] (deserialize-chunk this)]
    (cond-> vs
      (seq des) (conj des)
      (seq nxt) (recur (deserialize-chunk nxt)))))


(defmethod deserialize-value ordered-seq-prefix [[_ _ this]]
  (loop [vs []
         [des nxt] (deserialize-chunk this)]
    (cond-> vs
      (seq des) (conj des)
      (seq nxt) (recur (deserialize-chunk nxt)))))

(defmethod deserialize-value text-prefix [[_ _ this]]
  (String. (into-array Byte/TYPE this) "UTF8"))
