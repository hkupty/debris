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

(defmulti serialize type)
(defmulti deserialize-value first)

(defn- serialize-byte [raw]
  (byte-array [byte-prefix (byte 0x1) raw]))

(defn- serialize-bytes [^bytes raw]
  (byte-array (into
                  [byte-prefix
                   (byte (count raw))]
                  (vec raw))))

(defn- serialize-numbers [number]
 (let [str-repr (str number)]
    (byte-array (into
                  [num-prefix
                   (byte (count str-repr))]
                  (map byte)
                  str-repr))))

(defn- serialize-text [str-]
  (byte-array (into
                [text-prefix
                 (byte (count str-))]
                (map byte)
                str-)))

(defn- serialize-named [symbol-or-kw]
  (let [ns- (namespace symbol-or-kw)
        str-repr (str/join "/"
                           (cond->> [(name symbol-or-kw)]
                             (some? ns-) (cons ns-)))]
    (byte-array (into
                  [text-prefix
                   (byte (count str-repr))]
                  (map byte)
                  str-repr))))

(defn- serialize-bool [bool]
  (byte-array [bool-prefix (byte 0x1) (byte (cond-> 0 bool inc))]))

(defn- serialize-maps [map-]
  (let [body (sort-by (comp vec first)
                      compare
                      (map (fn [[k v]] [(serialize k) (serialize v)]) map-))
        body (apply concat [] body)
        size (transduce (map count) + body)
        header (byte-array [unordered-map-prefix (byte size)])]
    (byte-array (apply concat [] header body))))

(defn- serialize-sets [set-]
  (let [body (sort-by vec
                      compare
                      (map serialize set-))
        size (transduce (map count) + body)
        header (byte-array [unordered-seq-prefix (byte size)])]
    (byte-array (apply concat [] header body))))

(defn- serialize-vecs [seq-]
  (let [body (map serialize seq-)
        size (transduce (map count) + body)
        header (byte-array [ordered-seq-prefix (byte size)])]
    (byte-array (apply concat [] header body))))

(defn- serialize-ordered-kvps [seq-]
  (let [body (mapcat (fn [[k v]] [(serialize k) (serialize v)]) seq-)
        size (transduce (map count) + body)
        header (byte-array [ordered-seq-prefix (byte size)])]
    (byte-array (apply concat [] header body))))

(defmethod serialize java.lang.Byte [data] (serialize-byte data))
(defmethod serialize  (type (byte-array [])) [data] (serialize-bytes data))
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
                          (cond-> (conj kvps des)
                            (seq nxt) (recur (deserialize-chunk nxt)))))))

(defmethod deserialize-value ordered-map-prefix [[_ _ this]]
  (into (sorted-map)
        (map vec)
        (partition 2 (loop [kvps []
                               [des nxt] (deserialize-chunk this)]
                          (cond-> (conj kvps des)
                            (seq nxt) (recur (deserialize-chunk nxt)))))))

(defmethod deserialize-value unordered-seq-prefix [[_ _ this]]
  (loop [vs #{}
         [des nxt] (deserialize-chunk this)]
    (cond-> (conj vs des)
      (seq nxt) (recur (deserialize-chunk nxt)))))


(defmethod deserialize-value ordered-seq-prefix [[_ _ this]]
  (loop [vs []
         [des nxt] (deserialize-chunk this)]
    (cond-> (conj vs des)
      (seq nxt) (recur (deserialize-chunk nxt)))))

(defmethod deserialize-value text-prefix [[_ _ this]]
  (String. (into-array Byte/TYPE this)))

(defn deserialize-chunk [chunk-]
  (let [[tp sz & data] chunk-
        [payload next-] (split-at sz data)]
    [(deserialize-value [tp sz payload]) next-]))

(defn deserialize [data]
  (first (deserialize-chunk data)))

(comment
  (into (byte-array [(byte 1)])
        (byte-array [(byte 2)]))
  (deserialize (serialize true))
  (= 0 (compare (vec (serialize {1 2, 3 4, 0 5, 2 3}))
                (vec (serialize {0 5, 3 4, 2 3, 1 2}))))

  (deserialize (serialize 30.1))

  (deserialize (vec (serialize {::stuff 1 3 true false #{1 2 3 5 0}})))

  (compare [(byte 0x2) (byte 0x8) (byte 0x48) (byte 0x48)]
           [(byte 0x2) (byte 0x1) (byte 0x50)])
  )
