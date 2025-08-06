;; # API Reference
;;
;; Complete reference documentation for all functions in the `ppi.api` namespace.
;;

^:kindly/hide-code
(ns ppi.docs.api-reference
  (:require [scicloj.kindly.v4.kind :as kind]
            [scicloj.kindly.v4.api :as kindly]
            [clojure.string :as str]
            [tablecloth.api :as tc]
            [java-time.api :as java-time]
            [ppi.api :as ppi]
            [babashka.fs :as fs]))

^:kindly/hide-code
(defn include-fnvar-as-section [fnvar]
  (-> (let [{:keys [name arglists doc]} (meta fnvar)]
        (str (format "## `%s`\n\n" name)
             (->> arglists
                  (map (fn [l]
                         (->> l
                              pr-str
                              (format "`%s`\n\n"))))
                  (str/join ""))
             doc))
      kind/md
      kindly/hide-code))

(include-fnvar-as-section #'ppi/standardize-csv-line)

;; ### Examples

(ppi/standardize-csv-line "\"hello,world\"")

(ppi/standardize-csv-line "hello,\"\"\"\"world")

(ppi/standardize-csv-line "hello,\"\"world\"\"")

(ppi/standardize-csv-line "\"field1,\"\"value with quotes\"\",field3\"")

(include-fnvar-as-section #'ppi/prepare-standard-csv!)

;; ### Example

;; Process a malformed CSV file
(def raw-csv-path
  "data/query_result_2025-05-30T07_52_48.720548159Z.csv.gz")

;; Create a standardized version with cleaned quotes:
(def standard-csv-path
  (str/replace raw-csv-path #"\.csv\.gz" ".standard.csv.gz"))

(when-not (fs/exists? standard-csv-path)
  (ppi/prepare-standard-csv! raw-csv-path standard-csv-path))

;; Creates clean-data.csv.gz with fixed quote formatting

(include-fnvar-as-section #'ppi/prepare-raw-data)

;; ### Example

(let [sample-raw-data (tc/dataset {"Query Results - Device UUID" ["device-1" "device-2"]
                                   "Query Results - PpInMs" ["1,200" "1,150"]
                                   "Query Results - PpErrorEstimate" ["5,000" "6,000"]
                                   "Other Column" ["data1" "data2"]})]

  ;; Show the transformation
  (kind/hiccup
   [:div
    [:h4 "Before:"]
    sample-raw-data
    [:h4 "After:"]
    (ppi/prepare-raw-data sample-raw-data "Query Results - ")]))

(include-fnvar-as-section #'ppi/filter-recent-data)

;; ### Example

(let [sample-data (tc/dataset {:Client-Timestamp [(java-time/local-date-time 2024 12 31)
                                                  (java-time/local-date-time 2025 1 15)
                                                  (java-time/local-date-time 2025 2 1)]
                               :value [1 2 3]})
      cutoff-date (java-time/local-date-time 2025 1 1)]

  (kind/hiccup
   [:div
    [:h4 "Original data:"]
    sample-data
    [:h4 "After filtering (keeping only records after 2025-01-01):"]
    (ppi/filter-recent-data sample-data cutoff-date)]))

(include-fnvar-as-section #'ppi/add-timestamps)

;; ### Example

(let [sample-data (tc/dataset {:Device-UUID [#uuid "550e8400-e29b-41d4-a716-446655440000"
                                             #uuid "550e8400-e29b-41d4-a716-446655440000"]
                               :Client-Timestamp [(java-time/local-date-time 2025 1 1 12 0)
                                                  (java-time/local-date-time 2025 1 1 12 0)]
                               :PpInMs [800 820]})]

  (kind/hiccup
   [:div
    [:h4 "Before (client timestamps only):"]
    sample-data
    [:h4 "After (with precise measurement timestamps):"]
    (ppi/add-timestamps sample-data)]))

(include-fnvar-as-section #'ppi/recognize-jumps)

;; ### Example

(let [sample-data (tc/dataset {:Device-UUID [#uuid "550e8400-e29b-41d4-a716-446655440000"
                                             #uuid "550e8400-e29b-41d4-a716-446655440000"
                                             #uuid "550e8400-e29b-41d4-a716-446655440000"]
                               :timestamp [(java-time/local-date-time 2025 1 1 12 0 0)
                                           (java-time/local-date-time 2025 1 1 12 0 1)
                                           (java-time/local-date-time 2025 1 1 12 0 8)]}) ; 7 second gap
      params {:jump-threshold 5000}] ; 5 second threshold

  (kind/hiccup
   [:div
    [:h4 "Original data:"]
    sample-data
    [:h4 "After jump detection:"]
    (ppi/recognize-jumps sample-data params)]))
