(ns auto-qc.test-generators
  (:require [clojure.test.check.generators :as gen]))

(def char-uppercase
  (gen/such-that #(or (Character/isUpperCase %) (Character/isDigit %)) gen/char-alphanumeric))


(def miseq-flowcell-id
  (gen/fmap #(apply str "000000000-" %) (gen/vector char-uppercase 5)))


(defn make-zero-padded-num
  [min max digits]
  (let [zero-pad (fn [n] (format (str "%0" digits "d") n))]
    (gen/fmap zero-pad (gen/choose min max))))


(def zero-padded-month
  (let [pad-zero #(if (> % 9) (str %) (str 0 %))]
    (gen/fmap pad-zero (gen/choose 1 12))))


(def two-digit-year
  (gen/fmap str (gen/choose 16 32)))


(def four-digit-date
  (let [num-days {"01" 31
                  "02" 28
                  "03" 31
                  "04" 30
                  "05" 31
                  "06" 30
                  "07" 31
                  "08" 31
                  "09" 30
                  "10" 31
                  "11" 30
                  "12" 31}]
    (gen/bind zero-padded-month (fn [month] (gen/fmap #(str month %) (make-zero-padded-num 1 (num-days month) 2))))))


(def six-digit-date
  (gen/bind two-digit-year
            (fn [year] (gen/fmap #(str year %) four-digit-date))))


(def miseq-instrument-id
  (gen/fmap #(str "M" %) (make-zero-padded-num 1 9999 5)))


(def miseq-run-id
  (-> six-digit-date
      (gen/bind (fn [x] (gen/fmap #(str x "_" %) miseq-instrument-id)))
      (gen/bind (fn [x] (gen/fmap #(str x "_" %) (make-zero-padded-num 1 9999 4))))
      (gen/bind (fn [x] (gen/fmap #(str x "_" %) miseq-flowcell-id)))))
