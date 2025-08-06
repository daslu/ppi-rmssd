(ns ppi.api
  (:require ;; Data manipulation and analysis
   [tech.v3.dataset :as ds] ; Efficient dataset constructs
   [tablecloth.api :as tc] ; Dataset manipulation and analysis
   [tablecloth.column.api :as tcc] ; Column-level operations and statistics

   ;; Standard library utilities
   [clojure.string :as str] ; String manipulation functions
   [clojure.java.io :as io] ; File I/O operations

   ;; File system operations
   [babashka.fs :as fs] ; Cross-platform file system utilities

   ;; Date and time handling
   [java-time.api :as java-time] ; Modern Java time API wrapper
   [tech.v3.datatype.datetime :as datetime] ; High-performance datetime operations

   ;; High-performance array programming with dtype-next
   [tech.v3.datatype :as dtype] ; Core dtype-next functionality
   [tech.v3.datatype.functional :as dfn] ; Functions applied to arrays
   [tech.v3.datatype.rolling :as rolling] ; Rolling windows functionality

   ;; Random number generation for artifact simulation
   [fastmath.random :as random]) ; Fast mathematical random number generation
  (:import (java.util.zip GZIPInputStream ; For reading compressed CSV files
                          GZIPOutputStream))) ; For writing compressed CSV files



(defn standardize-csv-line
  "Cleans up malformed CSV line by removing redundant quotes.
  
  Fixes common CSV parsing issues where fields have excessive quotes:
  
  - Removes leading and trailing quotes from the entire line
  - Removes quadruple quotes (`\"\"\"\"` -> empty)
  - Converts double quotes to single quotes (`\"\"` -> `\"`)
  
  **Args:**
  - `csv-line` - String containing a single CSV line with quote issues
    
  **Returns:**
  String with cleaned quotes"
  [csv-line]
  (-> csv-line
      (str/replace #"^\"" "")
      (str/replace #"\"$" "")
      (str/replace #"\"\"\"\"" "")
      (str/replace #"\"\"" "\"")))

(defn prepare-standard-csv!
  "Processes a gzipped CSV file to fix quote formatting issues.
  
  Reads the raw CSV file, applies quote standardization to each line,
  and writes a cleaned version. Only processes if the standard file
  doesn't already exist.
  
  **Args:**
  - `raw-csv-path` - String path to the input gzipped CSV file
  - `standard-csv-path` - String path for the output standardized gzipped CSV
    
  **Side effects:**
  Creates a new standardized CSV file on disk"
  [raw-csv-path standard-csv-path]
  (when-not (fs/exists? standard-csv-path)
    (with-open [in (-> raw-csv-path
                       io/input-stream
                       GZIPInputStream.)
                out (-> standard-csv-path
                        clojure.java.io/output-stream
                        GZIPOutputStream.)]
      (->> in
           slurp
           str/split-lines
           (map standardize-csv-line)
           (str/join "\n")
           (spit out)))))

(defn prepare-raw-data
  "Prepares raw CSV data by cleaning column names and parsing numeric fields.
  
  Transforms the raw dataset by:
  
  - Removing specified column name prefix
  - Converting spaces to hyphens in column names  
  - Converting column names to keywords
  - Parsing comma-separated numeric strings in `:PpInMs` and `:PpErrorEstimate` columns
  
  **Args:**
  - `raw-data` - Tablecloth dataset from CSV with string columns
  - `colname-prefix` - String prefix to remove from column names
  
  **Returns:**
  Dataset with cleaned column names and parsed numeric values"
  [raw-data colname-prefix]
  (-> raw-data
      (tc/rename-columns (fn [colname]
                           (-> colname
                               (str/replace colname-prefix "")
                               (str/replace " " "-")
                               keyword)))
      (tc/map-columns :PpInMs :PpInMs (fn [s]
                                        (-> s
                                            (str/replace "," "")
                                            Integer/parseInt)))
      (tc/map-columns :PpErrorEstimate :PpErrorEstimate (fn [s]
                                                          (-> s
                                                              (str/replace "," "")
                                                              Integer/parseInt)))))

(defn filter-recent-data
  "Filters dataset to include only records after a specified date.
  
  **Args:**
  - `prepared-data` - Dataset with parsed timestamps
  - `cutoff-date` - `java-time` LocalDateTime, records after this date are kept
  
  **Returns:**
  Filtered dataset containing only recent records"
  [prepared-data cutoff-date]
  (-> prepared-data
      (tc/select-rows (fn [row]
                        (-> row
                            :Client-Timestamp
                            (java-time/after? cutoff-date))))))


(defn add-timestamps
  "Computes actual timestamps for pulse-to-pulse measurements.
  
  Takes a dataset with `:Device-UUID`, `:Client-Timestamp`, and `:PpInMs` columns
  and calculates precise timestamps for each measurement. Groups by device
  and client timestamp, accumulates pulse-to-pulse intervals, then adds
  them to the client timestamp to get actual measurement times.
  
  **Args:**
  - `data` - Tablecloth dataset containing columns:
    - `:Device-UUID` - device identifier  
    - `:Client-Timestamp` - base timestamp from client
    - `:PpInMs` - pulse-to-pulse interval in milliseconds
          
  **Returns:**
  Dataset with additional columns:
  
  - `:accumulated-pp` - cumulative sum of pulse intervals
  - `:timestamp` - precise measurement timestamp (`:Client-Timestamp` + accumulated intervals)"
  [data]
  (-> data
      (tc/group-by [:Device-UUID :Client-Timestamp])
      (tc/add-column :accumulated-pp
                     #(reductions + (:PpInMs %)))
      (tc/map-columns :timestamp
                      [:Client-Timestamp :accumulated-pp]
                      (fn [client-timestamp accumulated-pp]
                        (datetime/plus-temporal-amount
                         client-timestamp
                         accumulated-pp
                         :milliseconds)))
      tc/ungroup))

(defn recognize-jumps
  "Identifies temporal discontinuities in time series data.
  
  Analyzes timestamps to detect gaps that exceed a threshold, indicating
  potential data collection interruptions or device resets. For each device,
  calculates time differences between consecutive measurements and marks
  jumps when gaps exceed the threshold.
  
  **Args:**
  - `data` - Tablecloth dataset with `:Device-UUID` and `:timestamp` columns
  - `params` - Map containing:
    - `:jump-threshold` - minimum gap in milliseconds to consider a jump
            
  **Returns:**
  Dataset with an additional column:
  - `:jump-count` - cumulative count of jumps per device (creates segments)"
  [data {:keys [jump-threshold]}]
  (-> data
      (tc/group-by [:Device-UUID])
      (tc/order-by [:timestamp])
      (tc/add-column :delta-timestamp
                     (fn [ds]
                       (let [timestamps (:timestamp ds)
                             n (count timestamps)]
                         (if (< n 2)
                           [0]
                           (cons 0
                                 (map #(datetime/between %1 %2 :milliseconds)
                                      (take (dec n) timestamps)
                                      (drop 1 timestamps)))))))
      (tc/add-column :jump
                     #(map (fn [delta] (if (> delta jump-threshold) 1 0)) (:delta-timestamp %)))
      (tc/add-column :jump-count
                     #(reductions + (:jump %)))
      (tc/drop-columns [:delta-timestamp :jump])
      tc/ungroup))


(defn prepare-timestamped-ppi-data
  "Prepares a continous PPI dataset from the raw data.
  This is the main dataset to be used in the analysis.

  **Args:**
  - `standard-csv-path` - path to the raw data
            
  **Returns:**
  Dataset with columns: `:Device-UUID :timestamp :PpInMs :PpErrorEstimate`"
  [standard-csv-path]
  (let [date-time-format "yyyy.M.d, HH:mm"
        raw-data      (tc/dataset standard-csv-path
                                  {:parser-fn {"Created At" [:local-date-time date-time-format]
                                               "Client Timestamp" [:local-date-time date-time-format]}})
        colname-prefix-to-remove (-> raw-data
                                     keys
                                     second
                                     (subs 0 14))]
    (-> raw-data
        (prepare-raw-data colname-prefix-to-remove)
        (filter-recent-data (java-time/local-date-time 2024 1 1))
        add-timestamps
        (tc/select-columns [:Device-UUID :timestamp :PpInMs :PpErrorEstimate]))))


(defn calclulcate-coefficient-of-variation
  "Calculate coefficient of variation using dtype-next vectorized operations.
  
  **Args:**
  - `values` - Sequence or array of numeric values
  
  **Returns:**
  Double - CV as percentage (0-100)"
  [values]
  (let [mean-val (tcc/mean values)
        std-val (tcc/standard-deviation values)]
    (if (zero? mean-val)
      0.0
      (* 100.0 (/ std-val mean-val)))))

(defn calculate-successive-changes
  "Calculate percentage changes between successive elements efficiently.
  
  **Args:**
  - `values` - Sequence or array of numeric values
  
  **Returns:**
  Array of successive percentage changes"
  [values]
  (let [n (count values)]
    (if (< n 2)
      (dtype/->reader [] :float64)
      (let [shifted (tcc/shift values 1)
            ;; Calculate differences (skip first element which is meaningless)
            diffs (dtype/sub-buffer (tcc/- values shifted) 1 (dec n))
            ;; Get previous values for percentage calculation
            prev-vals (dtype/sub-buffer values 0 (dec n))
            ;; Calculate percentage changes: 100 * (diff / prev)
            pct-changes (tcc/* 100.0 (tcc// diffs prev-vals))]
        (tcc/abs pct-changes)))))


(defn clean-segment?
  "Identifies high-quality 'clean' segments suitable for ground truth analysis.
  
  These segments serve as reference data for validating cleaning algorithms by
  providing pristine examples before artificial distortion is applied.
  
  Uses dtype-next fast statistical functions for improved performance.
  
  **Args:**
  - `segment-data` - The time series of one segment of one device.
  - `params` - Map containing quality thresholds:
    - `:max-error-estimate` - Maximum acceptable PP error 
    - `:max-heart-rate-cv` - Maximum coefficient of variation for heart rate (%)
    - `:max-successive-change` - Maximum allowed successive PP change (%)
    - `:min-clean-duration` - Minimum duration for clean segments (ms)
    - `:min-clean-samples` - Minimum samples required
    
  **Returns:**
  Dataset containing only segments that meet all cleanliness criteria"
  [segment-data
   {:keys [max-error-estimate max-heart-rate-cv max-successive-change
           min-clean-duration min-clean-samples]}]
  (and
   ;; Basic size and duration requirements
   (>= (tc/row-count segment-data) min-clean-samples)
   (>= (java-time/time-between (-> segment-data :timestamp first)
                               (-> segment-data :timestamp last)
                               :millis)
       min-clean-duration)

   ;; Low error estimate requirement (using dtype-next for speed)
   (< (tcc/mean (:PpErrorEstimate segment-data))
      max-error-estimate)

   ;; Stable heart rate (using fast coefficient of variation)
   (< (calclulcate-coefficient-of-variation (:PpInMs segment-data))
      max-heart-rate-cv)

   ;; No sudden jumps (using fast successive changes)
   (let [successive-changes (calculate-successive-changes (:PpInMs segment-data))]
     (< (if (zero? (count successive-changes))
          0.0
          (tcc/reduce-max successive-changes))
        max-successive-change))))


;; A dataset that holds only the last `max-size` (or less)
;; rows in memory,
;; implemented as a round-robin index structure
;; defined over a tech.ml.dataset structure with mutable columns:
(defrecord WindowedDataset
    [dataset
     column-types
     max-size
     current-size
     current-position])


(defn make-windowed-dataset
  "Create an empty `WindowedDataset` with a given `max-size`
  and given `column-types` (map).

  **Args:**
  - `column-types` - a map from column name to type
  - `max-size` - maximal window size to keep

  **Returns:**
  The specified `WindowedDataset` structure."
  [column-types max-size]
  (-> column-types
      (update-vals
       (fn [datatype]
         (dtype/make-container :jvm-heap
                               datatype
                               max-size)))
      tc/dataset
      (->WindowedDataset column-types max-size 0 0)))


(defn insert-to-windowed-dataset!
  "Insert a new row to a `WindowedDataset`.
  
  **Args:**
  - `windowed-dataset` - a `WindowedDataset`
  - `row` - A row represented as a map structure
  (can be a record or `FastStruct`, etc.)

  **Returns:**
  Updated windowed dataset with its data mutated(!)."
  [{:as windowed-dataset
    :keys [dataset column-types max-size current-position]}
   value]
  (doseq [[colname _] column-types]
    (dtype/set-value! (dataset colname)
                      current-position
                      (value colname)))
  (-> windowed-dataset
      (update :current-size #(min (inc %) max-size))
      (update :current-position #(rem (inc %) max-size))))

(defn windowed-dataset->dataset
  "Return a regular dataset as a view over the content of a windowed dataset.

  **Args:**
  - `windowed-dataset` - a `WindowedDataset`"
  [{:as windowed-dataset
    :keys [dataset column-types max-size current-size current-position]}]
  (ds/select-rows dataset
                  (-> (range (- current-position
                                current-size)
                             current-position)
                      (dfn/rem max-size))))
