(ns debris.serde-test
  (:require [clojure.test :as t]
            [matcher-combinators.test]
            [debris.serde :as debris])
  (:import (clojure.lang ExceptionInfo)))

(t/deftest size-in-bytes-test
  ;; TODO Add generative testing
  (t/testing "All byte values can be encoded"
    (t/is (thrown-match?
            ExceptionInfo
            {:reason ::debris/invalid-negative-size}
            (BigInteger. 1 (debris/size-in-bytes -1))))
    (t/is (thrown-match?
            ExceptionInfo
            {:reason ::debris/unsupported-size}
            (BigInteger. 1 (debris/size-in-bytes (inc debris/max-size)))))
    (t/is (match? 32874 (BigInteger. 1 (debris/size-in-bytes 32874))))
    (t/is (match? 100 (BigInteger. 1 (debris/size-in-bytes 100))))
    (t/is (match? 0 (BigInteger. 1 (debris/size-in-bytes 0))))
    (t/is (match? debris/max-size (BigInteger. 1 (debris/size-in-bytes debris/max-size))))))


(t/deftest serde-test
  (t/testing "roundtrip"
    ;; Not truly roundtrip, since it widens some types
    (t/is (match? 30.1M (debris/deserialize (debris/serialize 30.1))))
    (t/is (match? true (debris/deserialize (debris/serialize true))))
    ;; UTF-8 text
    (t/is (match? "ação" (debris/deserialize (debris/serialize "ação"))))

    (t/is (match? "something" (debris/deserialize (debris/serialize 'something))))
    (t/is (match? "something" (debris/deserialize (debris/serialize :something))))
    (t/is (match? "le-ns/something" (debris/deserialize (debris/serialize :le-ns/something))))
    (t/is (match? [] (debris/deserialize (debris/serialize []))))
    (t/is (match? {} (debris/deserialize (debris/serialize {}))))
    (t/is (match? #{} (debris/deserialize (debris/serialize #{}))))
    )
  )
