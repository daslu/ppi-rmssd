;; # API Reference
;;
;; Complete reference documentation for all functions in the `ppi.api` namespace.
;;

^:kindly/hide-code
(ns ppi-docs.api-reference
  (:require [scicloj.kindly.v4.kind :as kind]
            [scicloj.kindly.v4.api :as kindly]
            [clojure.string :as str]
            [tablecloth.api :as tc]
            [java-time.api :as java-time]
            [clojure.math :as math]
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

(include-fnvar-as-section #'ppi/prepare-timestamped-ppi-data)

;; ### Example

(when (fs/exists? standard-csv-path)
  ;; Process the standardized CSV to create timestamped PPI data
  (def timestamped-data (ppi/prepare-timestamped-ppi-data standard-csv-path))

  ;; Show a sample of the processed data
  (kind/hiccup
   [:div
    [:h4 "Sample of timestamped PPI data:"]
    (-> timestamped-data
        (tc/head 5)
        (tc/select-columns [:Device-UUID :Client-Timestamp :PpInMs :timestamp :accumulated-pp]))]))

(include-fnvar-as-section #'ppi/calculate-coefficient-of-variation)

;; ### Examples

;; Low variability (healthy, stable heart rate)
(let [stable-intervals [800 810 805 815 820]]
  (printf "Stable intervals %s -> CV: %.2f%%\n"
          stable-intervals
          (ppi/calculate-coefficient-of-variation stable-intervals)))

;; High variability (irregular heart rate)
(let [variable-intervals [700 900 750 1000 650]]
  (printf "Variable intervals %s -> CV: %.2f%%\n"
          variable-intervals
          (ppi/calculate-coefficient-of-variation variable-intervals)))

(include-fnvar-as-section #'ppi/calculate-successive-changes)

;; ### Examples

;; Calculate successive percentage changes
(let [ppi-intervals [800 850 820 880 810]]
  (kind/hiccup
   [:div
    [:h4 "PPI intervals:"] [:code (pr-str ppi-intervals)]
    [:h4 "Successive percentage changes:"]
    [:code (pr-str (vec (ppi/calculate-successive-changes ppi-intervals)))]]))

;; Example with larger changes
(let [volatile-intervals [800 1200 600 1000 500]]
  (printf "Volatile intervals %s -> Max change: %.1f%%\n"
          volatile-intervals
          (apply max (ppi/calculate-successive-changes volatile-intervals))))

(include-fnvar-as-section #'ppi/clean-segment?)

;; ### Example

(let [;; Create sample segment data
      base-time (java-time/local-date-time 2025 1 1 12 0 0)
      clean-segment (tc/dataset {:timestamp [(java-time/plus base-time (java-time/seconds 0))
                                             (java-time/plus base-time (java-time/seconds 1))
                                             (java-time/plus base-time (java-time/seconds 2))
                                             (java-time/plus base-time (java-time/seconds 3))
                                             (java-time/plus base-time (java-time/seconds 4))]
                                 :PpInMs [800 810 805 815 820] ; Low variability
                                 :PpErrorEstimate [5 4 6 5 4]}) ; Low error
      noisy-segment (tc/dataset {:timestamp [(java-time/plus base-time (java-time/seconds 0))
                                             (java-time/plus base-time (java-time/seconds 1))
                                             (java-time/plus base-time (java-time/seconds 2))
                                             (java-time/plus base-time (java-time/seconds 3))
                                             (java-time/plus base-time (java-time/seconds 4))]
                                 :PpInMs [800 1200 600 1400 500] ; High variability
                                 :PpErrorEstimate [50 60 55 45 65]}) ; High error
      quality-params {:max-error-estimate 10
                      :max-heart-rate-cv 5.0
                      :max-successive-change 10.0
                      :min-clean-duration 3000
                      :min-clean-samples 5}]

  (kind/hiccup
   [:div
    [:h4 "Quality Assessment Results:"]
    [:p [:strong "Clean segment (stable intervals, low error): "]
     (if (ppi/clean-segment? clean-segment quality-params) "✓ CLEAN" "✗ REJECTED")]
    [:p [:strong "Noisy segment (variable intervals, high error): "]
     (if (ppi/clean-segment? noisy-segment quality-params) "✓ CLEAN" "✗ REJECTED")]]))

;; ## Windowed Dataset Functions

;; The windowed dataset functionality provides efficient circular buffer operations for streaming HRV analysis.

;; ### WindowedDataset Record

;; The `WindowedDataset` record implements a circular buffer data structure optimized for time-series analysis:

;; ```clojure
;; (defrecord WindowedDataset
;;           [dataset           ; tech.v3.dataset containing the actual data
;;            column-types      ; map of column names to data types
;;            max-size         ; maximum number of rows the buffer can hold
;;            current-size     ; current number of rows (0 to max-size)
;;            current-position ; current write position (circular index)])
;; ```

;; **Key Characteristics:**

;; - **Fixed Memory**: Pre-allocates arrays for maximum performance
;; - **Circular Buffer**: New data overwrites oldest when buffer is full
;; - **Chronological Access**: Functions provide data in insertion order
;; - **Zero-Copy Views**: Time windows are extracted without data copying
;; - **Type Safety**: Column types are enforced at creation time

;; **Usage Pattern:**

;; 1. Create with `make-windowed-dataset` specifying column types and buffer size
;; 2. Insert streaming data with `insert-to-windowed-dataset!` (❗**Caution: mutating the internal dataset.**)
;; 3. Extract time windows with `windowed-dataset->time-window-dataset`
;; 4. Compute HRV metrics like RMSSD over specific time periods

;; This design enables real-time HRV analysis with consistent memory usage and sub-millisecond response times.

;; ### WindowedDataset Structure Example

(let [;; Create a windowed dataset to examine its structure
      wd (ppi/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 3)
      base-time (java-time/local-date-time 2025 1 1 12 0 0)

      ;; Add one data point to see the structure
      wd-with-data (ppi/insert-to-windowed-dataset! wd {:timestamp base-time :PpInMs 800})]

  (kind/hiccup
   [:div
    [:h4 "WindowedDataset Record Fields:"]
    [:table
     [:thead [:tr [:th "Field"] [:th "Value"] [:th "Description"]]]
     [:tbody
      [:tr [:td [:code ":dataset"]] [:td "tech.v3.dataset"] [:td "Internal data storage"]]
      [:tr [:td [:code ":column-types"]] [:td [:code (pr-str (:column-types wd-with-data))]] [:td "Column type specifications"]]
      [:tr [:td [:code ":max-size"]] [:td (:max-size wd-with-data)] [:td "Buffer capacity"]]
      [:tr [:td [:code ":current-size"]] [:td (:current-size wd-with-data)] [:td "Current number of rows"]]
      [:tr [:td [:code ":current-position"]] [:td (:current-position wd-with-data)] [:td "Next write position"]]]]]))

;; ### Circular Buffer Behavior

(let [;; Demonstrate circular buffer behavior
      small-wd (ppi/make-windowed-dataset {:value :int32} 3)

      ;; Fill beyond capacity to show circular behavior
      test-data (map (fn [i] {:value i}) (range 5))
      final-wd (reduce ppi/insert-to-windowed-dataset! small-wd test-data)]

  (kind/hiccup
   [:div
    [:h4 "Circular Buffer Example (capacity: 3, inserted: 5 values):"]
    [:p [:strong "Final state: "]
     (format "size=%d, position=%d (values 0,1 were overwritten by 3,4)"
             (:current-size final-wd)
             (:current-position final-wd))]
    [:p [:strong "Data in chronological order: "]]
    (ppi/windowed-dataset->dataset final-wd)]))

(include-fnvar-as-section #'ppi/make-windowed-dataset)

;; ### Example

(let [;; Create a windowed dataset for HRV data with 10-sample capacity
      column-spec {:timestamp :local-date-time
                   :PpInMs :int32
                   :heartbeat-id :int32}
      wd (ppi/make-windowed-dataset column-spec 10)]

  (kind/hiccup
   [:div
    [:h4 "Created windowed dataset:"]
    [:p [:strong "Max size: "] (:max-size wd)]
    [:p [:strong "Current size: "] (:current-size wd)]
    [:p [:strong "Current position: "] (:current-position wd)]
    [:p [:strong "Column types: "] [:code (pr-str (:column-types wd))]]]))

(include-fnvar-as-section #'ppi/insert-to-windowed-dataset!)

;; ### Example

(let [;; Create windowed dataset
      wd (ppi/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32 :value :int32} 5)
      base-time (java-time/local-date-time 2025 1 1 12 0 0)

      ;; Insert some data points
      sample-data [{:timestamp base-time :PpInMs 800 :value 1}
                   {:timestamp (java-time/plus base-time (java-time/millis 1000)) :PpInMs 850 :value 2}
                   {:timestamp (java-time/plus base-time (java-time/millis 2000)) :PpInMs 820 :value 3}]

      ;; Insert data step by step
      wd-step1 (ppi/insert-to-windowed-dataset! wd (first sample-data))
      wd-step2 (ppi/insert-to-windowed-dataset! wd-step1 (second sample-data))
      final-wd (ppi/insert-to-windowed-dataset! wd-step2 (last sample-data))]

  (kind/hiccup
   [:div
    [:h4 "Windowed dataset after inserting 3 records:"]
    [:p [:strong "Current size: "] (:current-size final-wd)]
    [:p [:strong "Data view: "]]
    (ppi/windowed-dataset->dataset final-wd)]))

(include-fnvar-as-section #'ppi/windowed-dataset-indices)

;; ### Example

(let [;; Create and populate a small windowed dataset
      wd (ppi/make-windowed-dataset {:value :int32} 4)
      ;; Insert 6 items (will wrap around)
      final-wd (reduce ppi/insert-to-windowed-dataset! wd
                       (map (fn [i] {:value i}) (range 6)))]

  (kind/hiccup
   [:div
    [:h4 "Windowed dataset with circular buffer behavior:"]
    [:p [:strong "Dataset state: "] (format "size=%d, position=%d, max=%d"
                                            (:current-size final-wd)
                                            (:current-position final-wd)
                                            (:max-size final-wd))]
    [:p [:strong "Index order for chronological access: "]
     [:code (pr-str (ppi/windowed-dataset-indices final-wd))]]
    [:p [:strong "Data in insertion order: "]]
    (ppi/windowed-dataset->dataset final-wd)]))

(include-fnvar-as-section #'ppi/windowed-dataset->dataset)

;; ### Example

(let [;; Create windowed dataset with sample HRV data
      base-time (java-time/local-date-time 2025 1 1 12 0 0)
      hrv-data (map (fn [i interval]
                      {:timestamp (java-time/plus base-time (java-time/millis (* i 1000)))
                       :PpInMs interval
                       :heartbeat-id i})
                    (range 8)
                    [800 850 820 880 810 840 795 825])
      wd (ppi/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32 :heartbeat-id :int32} 5)
      final-wd (reduce ppi/insert-to-windowed-dataset! wd hrv-data)]

  (kind/hiccup
   [:div
    [:h4 "Converting windowed dataset to regular dataset:"]
    [:p "Inserted 8 heartbeats into 5-capacity window (last 5 retained):"]
    (ppi/windowed-dataset->dataset final-wd)]))

(include-fnvar-as-section #'ppi/binary-search-timestamp-start)

;; ### Example

(let [;; Create sample timestamp data
      base-time (java-time/local-date-time 2025 1 1 12 0 0)
      timestamps (map #(java-time/plus base-time (java-time/millis (* % 2000))) (range 5))
      timestamp-col (vec timestamps)
      indices (vec (range 5))

      ;; Search for different target times
      search-cases [[(java-time/plus base-time (java-time/millis 3000)) "Between timestamps"]
                    [(java-time/plus base-time (java-time/millis 4000)) "Exact match"]
                    [(java-time/minus base-time (java-time/millis 1000)) "Before all timestamps"]
                    [(java-time/plus base-time (java-time/millis 10000)) "After all timestamps"]]]

  (kind/hiccup
   [:div
    [:h4 "Binary search examples:"]
    [:p [:strong "Timestamps: "] (map #(java-time/format "HH:mm:ss" %) timestamps)]
    [:table
     [:thead [:tr [:th "Target Time"] [:th "Description"] [:th "Found Position"]]]
     [:tbody
      (for [[target-time description] search-cases]
        [:tr
         [:td (java-time/format "HH:mm:ss" target-time)]
         [:td description]
         [:td (ppi/binary-search-timestamp-start timestamp-col indices target-time)]])]]]))

(include-fnvar-as-section #'ppi/windowed-dataset->time-window-dataset)

;; ### Example

(let [;; Create realistic HRV scenario with timestamps
      base-time (java-time/local-date-time 2025 1 1 12 0 0)
      hrv-intervals [800 820 790 830 810 840 795 825 815 805 820 800 830 810 840]

      ;; Create timestamped data (each measurement advances by its interval)
      hrv-data (loop [i 0, current-time base-time, intervals hrv-intervals, result []]
                 (if (empty? intervals)
                   result
                   (let [interval (first intervals)]
                     (recur (inc i)
                            (java-time/plus current-time (java-time/millis interval))
                            (rest intervals)
                            (conj result {:timestamp current-time
                                          :PpInMs interval
                                          :heartbeat-id i})))))

      wd (ppi/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32 :heartbeat-id :int32} 20)
      final-wd (reduce ppi/insert-to-windowed-dataset! wd hrv-data)]

  (kind/hiccup
   [:div
    [:h4 "Time window extraction examples:"]
    [:p (format "Created %d heartbeats over ~%.1f seconds"
                (count hrv-data)
                (/ (reduce + hrv-intervals) 1000.0))]

    [:h5 "Last 5 seconds of data:"]
    (ppi/windowed-dataset->time-window-dataset final-wd :timestamp 5000)

    [:h5 "Last 10 seconds of data:"]
    (ppi/windowed-dataset->time-window-dataset final-wd :timestamp 10000)

    [:h5 "All data (30-second window):"]
    (-> (ppi/windowed-dataset->time-window-dataset final-wd :timestamp 30000)
        (tc/select-columns [:heartbeat-id :PpInMs]))]))

(include-fnvar-as-section #'ppi/windowed-dataset->rmssd)

;; ### Examples

(let [;; Create sample HRV data for RMSSD calculation
      base-time (java-time/local-date-time 2025 1 1 12 0 0)

      ;; Test case 1: Known values for verification
      test-intervals [800 850 820 880 810] ; Expected RMSSD ≈ 54.54 ms
      test-data (map (fn [i interval]
                       {:timestamp (java-time/plus base-time (java-time/millis (* i 1000)))
                        :PpInMs interval})
                     (range 5)
                     test-intervals)

      wd (ppi/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 10)
      test-wd (reduce ppi/insert-to-windowed-dataset! wd test-data)]

  (kind/hiccup
   [:div
    [:h4 "RMSSD Calculation Examples:"]

    [:h5 "Test case: Known intervals"]
    [:p [:strong "PPI intervals: "] [:code (pr-str test-intervals)]]
    [:p [:strong "Expected successive differences: "] [:code "[50, -30, 60, -70]"]]
    [:p [:strong "RMSSD result: "] (format "%.2f ms" (ppi/windowed-dataset->rmssd test-wd :timestamp 10000))]

    [:h5 "Time window examples:"]
    [:table
     [:thead [:tr [:th "Window Size"] [:th "RMSSD (ms)"] [:th "Data Points Used"]]]
     [:tbody
      (for [[window-name window-ms] [["3 seconds" 3000] ["5 seconds" 5000] ["10 seconds" 10000]]]
        (let [window-data (ppi/windowed-dataset->time-window-dataset test-wd :timestamp window-ms)
              rmssd (ppi/windowed-dataset->rmssd test-wd :timestamp window-ms)]
          [:tr
           [:td window-name]
           [:td (if rmssd (format "%.2f" rmssd) "nil")]
           [:td (tc/row-count window-data)]]))]]]))

;; ### Custom Column Name Example

(let [;; Example with custom column name
      base-time (java-time/local-date-time 2025 1 1 12 0 0)
      custom-data [{:timestamp base-time :HeartInterval 800}
                   {:timestamp (java-time/plus base-time (java-time/millis 1000)) :HeartInterval 850}
                   {:timestamp (java-time/plus base-time (java-time/millis 2000)) :HeartInterval 820}]

      custom-wd (ppi/make-windowed-dataset {:timestamp :local-date-time :HeartInterval :int32} 5)
      custom-final-wd (reduce ppi/insert-to-windowed-dataset! custom-wd custom-data)]

  (kind/hiccup
   [:div
    [:h5 "Custom column name support:"]
    [:p "Using column name " [:code ":HeartInterval"] " instead of default " [:code ":PpInMs"]]
    [:p [:strong "RMSSD: "]
     (format "%.2f ms"
             (ppi/windowed-dataset->rmssd custom-final-wd :timestamp 5000 :HeartInterval))]]))

;; ### Performance Characteristics

(let [;; Create larger dataset for performance demonstration
      base-time (java-time/local-date-time 2025 1 1 12 0 0)
      large-intervals (cycle [800 820 790 830 810 840 795 825 815 805])
      large-data (map (fn [i interval]
                        {:timestamp (java-time/plus base-time (java-time/millis (* i interval)))
                         :PpInMs interval})
                      (range 1000)
                      (take 1000 large-intervals))
      large-wd (ppi/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 1200)
      large-final-wd (reduce ppi/insert-to-windowed-dataset! large-wd large-data)]

  ;; Time the RMSSD calculation
  (let [start-time (System/nanoTime)
        rmssd-result (ppi/windowed-dataset->rmssd large-final-wd :timestamp 60000)
        end-time (System/nanoTime)
        duration-ms (/ (- end-time start-time) 1000000.0)]

    (kind/hiccup
     [:div
      [:h5 "Performance with 1000 data points:"]
      [:p [:strong "Dataset size: "] "1000 heartbeats"]
      [:p [:strong "Time window: "] "60 seconds"]
      [:p [:strong "RMSSD result: "] (format "%.2f ms" rmssd-result)]
      [:p [:strong "Computation time: "] (format "%.3f ms" duration-ms)]
      [:p [:em "Uses dtype-next operations for optimal performance"]]])))

(include-fnvar-as-section #'ppi/copy-windowed-dataset)

;; ### Example

(let [;; Create and populate a windowed dataset
      base-time (java-time/local-date-time 2025 1 1 12 0 0)
      original-data [{:timestamp base-time :PpInMs 800}
                     {:timestamp (java-time/plus base-time (java-time/millis 1000)) :PpInMs 850}
                     {:timestamp (java-time/plus base-time (java-time/millis 2000)) :PpInMs 820}]

      wd (ppi/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 5)
      populated-wd (reduce ppi/insert-to-windowed-dataset! wd original-data)

      ;; Create a deep copy
      copied-wd (ppi/copy-windowed-dataset populated-wd)]

  (kind/hiccup
   [:div
    [:h4 "Deep copy windowed dataset example:"]
    [:p [:strong "Original dataset state: "]
     (format "size=%d, position=%d" (:current-size populated-wd) (:current-position populated-wd))]
    [:p [:strong "Copied dataset state: "]
     (format "size=%d, position=%d" (:current-size copied-wd) (:current-position copied-wd))]
    [:p [:strong "Data identical: "]
     (= (tc/rows (ppi/windowed-dataset->dataset populated-wd))
        (tc/rows (ppi/windowed-dataset->dataset copied-wd)))]]))

;; ## Data Distortion Functions

;; The following functions simulate realistic artifacts commonly found in HRV data from wearable devices. These are essential for:
;; 
;; - **Algorithm Testing**: Evaluate smoothing and cleaning algorithms against known artifacts
;; - **Synthetic Data Generation**: Create realistic test datasets when clean reference data is available
;; - **Research Validation**: Compare algorithm performance across different artifact types and severities
;; - **Quality Benchmarking**: Establish baseline performance metrics for HRV processing pipelines
;;
;; Each function can be used independently or combined via `distort-segment` for comprehensive artifact simulation.

(include-fnvar-as-section #'ppi/add-gaussian-noise)

;; ### Example

(let [;; Create clean sample data
      clean-data (tc/dataset {:PpInMs [800 810 805 815 820]})
      noisy-data (ppi/add-gaussian-noise clean-data :PpInMs 5.0)]

  (kind/hiccup
   [:div
    [:h4 "Gaussian noise example:"]
    [:p [:strong "Original: "] [:code (pr-str (vec (tc/column clean-data :PpInMs)))]]
    [:p [:strong "With 5ms noise: "] [:code (pr-str (mapv math/round (tc/column noisy-data :PpInMs)))]]]))

(include-fnvar-as-section #'ppi/add-outliers)

;; ### Example

(let [;; Create clean sample data
      clean-data (tc/dataset {:PpInMs [800 810 805 815 820]})
      outlier-data (ppi/add-outliers clean-data :PpInMs 0.4 2.5)] ; High probability for demo

  (kind/hiccup
   [:div
    [:h4 "Outlier example:"]
    [:p [:strong "Original: "] [:code (pr-str (vec (tc/column clean-data :PpInMs)))]]
    [:p [:strong "With outliers: "] [:code (pr-str (mapv math/round (tc/column outlier-data :PpInMs)))]]]))

(include-fnvar-as-section #'ppi/add-missing-beats)

;; ### Example

(let [;; Create clean sample data
      clean-data (tc/dataset {:PpInMs [800 810 805 815 820]})
      missing-data (ppi/add-missing-beats clean-data :PpInMs 0.6)] ; High probability for demo

  (kind/hiccup
   [:div
    [:h4 "Missing beats example:"]
    [:p [:strong "Original: "] [:code (pr-str (vec (tc/column clean-data :PpInMs)))]]
    [:p [:strong "With missing beats: "] [:code (pr-str (mapv int (tc/column missing-data :PpInMs)))]]
    [:p [:em "Doubled intervals show where beats were \"missed\""]]]))

(include-fnvar-as-section #'ppi/add-extra-beats)

;; ### Example

(let [;; Create clean sample data
      clean-data (tc/dataset {:PpInMs [800 810 805 815] :id (range 4)})
      extra-data (ppi/add-extra-beats clean-data :PpInMs 0.5)] ; High probability for demo

  (kind/hiccup
   [:div
    [:h4 "Extra beats example:"]
    [:p [:strong "Original (4 beats): "] [:code (pr-str (vec (tc/column clean-data :PpInMs)))]]
    [:p [:strong "With extra beats ("] (tc/row-count extra-data) " beats): "
     [:code (pr-str (mapv int (tc/column extra-data :PpInMs)))]]
    [:p [:em "Halved intervals appear where extra beats were inserted"]]]))

(include-fnvar-as-section #'ppi/add-trend-drift)

;; ### Example

(let [;; Create clean sample data
      clean-data (tc/dataset {:PpInMs [800 800 800 800 800]})
      drift-data (ppi/add-trend-drift clean-data :PpInMs 50.0 :increase)]

  (kind/hiccup
   [:div
    [:h4 "Trend drift example:"]
    [:p [:strong "Original: "] [:code (pr-str (vec (tc/column clean-data :PpInMs)))]]
    [:p [:strong "With 50ms increasing drift: "] [:code (pr-str (mapv math/round (tc/column drift-data :PpInMs)))]]
    [:p [:em "Gradual increase simulates heart rate slowing over time"]]]))

(include-fnvar-as-section #'ppi/distort-segment)

;; ### Example

(let [;; Create clean sample data
      clean-data (tc/dataset {:PpInMs [800 800 800 800 800]})
      distorted-data (ppi/distort-segment clean-data {})] ; Default parameters

  (kind/hiccup
   [:div
    [:h4 "Comprehensive distortion example:"]
    [:p [:strong "Original: "] [:code (pr-str (vec (tc/column clean-data :PpInMs)))]]
    [:p [:strong "Row count: "] (format "%d → %d" (tc/row-count clean-data) (tc/row-count distorted-data))]
    [:p [:strong "Distorted: "] [:code (pr-str (mapv math/round (tc/column distorted-data :PpInMs)))]]
    [:p [:em "Combines noise, outliers, missing/extra beats, and drift"]]]))

;; ## Smoothing Functions

(include-fnvar-as-section #'ppi/moving-average)

;; ### Example

(let [wd (ppi/make-windowed-dataset {:PpInMs :int32} 10)
      data [{:PpInMs 800} {:PpInMs 850} {:PpInMs 820}]
      populated-wd (reduce ppi/insert-to-windowed-dataset! wd data)]
  (double (ppi/moving-average populated-wd 3)))

(include-fnvar-as-section #'ppi/median-filter)

;; ### Example

(let [wd (ppi/make-windowed-dataset {:PpInMs :int32} 10)
      data [{:PpInMs 800} {:PpInMs 1200} {:PpInMs 820}] ; middle value is outlier
      populated-wd (reduce ppi/insert-to-windowed-dataset! wd data)]
  (ppi/median-filter populated-wd 3))

(include-fnvar-as-section #'ppi/cascaded-median-filter)

;; ### Example

(let [wd (ppi/make-windowed-dataset {:PpInMs :int32} 10)
      data [{:PpInMs 800} {:PpInMs 1200} {:PpInMs 820} {:PpInMs 1100} {:PpInMs 810}]
      populated-wd (reduce ppi/insert-to-windowed-dataset! wd data)]
  (ppi/cascaded-median-filter populated-wd))

(include-fnvar-as-section #'ppi/exponential-moving-average)

;; ### Example

(let [wd (ppi/make-windowed-dataset {:PpInMs :int32} 10)
      data [{:PpInMs 800} {:PpInMs 850} {:PpInMs 820}]
      populated-wd (reduce ppi/insert-to-windowed-dataset! wd data)]
  (ppi/exponential-moving-average populated-wd 0.3))


