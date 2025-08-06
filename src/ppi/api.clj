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
        raw-data (tc/dataset standard-csv-path
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

(defn calculate-coefficient-of-variation
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
   (< (calculate-coefficient-of-variation (:PpInMs segment-data))
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

(defn copy-windowed-dataset
  "Create a deep copy of a windowed dataset.
  
  **Args:**
  
  - `windowed-dataset` - a `WindowedDataset`
  
  **Returns:**
  New `WindowedDataset` with copied data"
  [{:as windowed-dataset :keys [dataset column-types max-size current-size current-position]}]
  (let [new-dataset (-> column-types
                        (update-vals
                         (fn [datatype]
                           (dtype/make-container :jvm-heap
                                                 datatype
                                                 max-size)))
                        tc/dataset)]
    ;; Copy existing data
    (doseq [[colname _] column-types
            i (range current-size)]
      (let [src-idx (if (< current-size max-size)
                      i
                      (rem (+ current-position i) max-size))
            dest-idx (if (< current-size max-size)
                       i
                       (rem (+ current-position i) max-size))]
        (dtype/set-value! (new-dataset colname)
                          dest-idx
                          (dtype/get-value (dataset colname) src-idx))))
    (->WindowedDataset new-dataset column-types max-size current-size current-position)))

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
  ;; Handle edge case: size-0 window does nothing
  (if (zero? max-size)
    windowed-dataset
    (let [;; Create a copy to avoid mutation issues with reductions
          copied-wd (copy-windowed-dataset windowed-dataset)]
      (doseq [[colname _] column-types]
        (dtype/set-value! ((:dataset copied-wd) colname)
                          current-position
                          (value colname)))
      ;; Create a new windowed dataset with the updated copy
      (->WindowedDataset (:dataset copied-wd)
                         column-types
                         max-size
                         (min (inc (:current-size windowed-dataset)) max-size)
                         (rem (inc current-position) max-size)))))

(defn windowed-dataset-indices
  "Extract the row indices for retrieving data from a windowed dataset in insertion order.
  
  This utility function encapsulates the logic for determining which rows to select
  from the underlying dataset to present data in the correct chronological order.
  
  **Args:**
  
  - `windowed-dataset` - a `WindowedDataset`
  
  **Returns:**
  Vector of integer indices in the correct order for data retrieval"
  [{:keys [max-size current-size current-position]}]
  (cond
    ;; Empty dataset
    (zero? current-size) []

    ;; Haven't wrapped around yet: select from 0 to current-size-1
    (< current-size max-size) (vec (range current-size))

    ;; Have wrapped around: select from current-position for max-size elements, wrapping
    :else (vec (map #(rem % max-size)
                    (range current-position (+ current-position max-size))))))

(defn windowed-dataset->dataset
  "Return a regular dataset as a view over the content of a windowed dataset.

  **Args:**
  
  - `windowed-dataset` - a `WindowedDataset`"
  [{:as windowed-dataset
    :keys [dataset]}]
  (let [indices (windowed-dataset-indices windowed-dataset)]
    (if (empty? indices)
      ;; Return empty dataset with same columns
      (ds/select-rows dataset [])
      (ds/select-rows dataset indices))))

(defn binary-search-timestamp-start
  "Find the first index position where timestamp >= target-time using binary search.
  
  **Args:**
  
  - `timestamp-col` - dataset column containing timestamps
  - `indices` - vector of indices in chronological order
  - `target-time` - target timestamp to search for
  
  **Returns:**
  Index position in the indices vector (not the actual dataset index)"
  [timestamp-col indices target-time]
  (loop [left 0
         right (count indices)]
    (if (>= left right)
      left ; Return the insertion point
      (let [mid (quot (+ left right) 2)
            mid-idx (nth indices mid)
            mid-timestamp (nth timestamp-col mid-idx)]
        (if (java-time/before? mid-timestamp target-time)
          (recur (inc mid) right) ; Search right half
          (recur left mid)))))) ; Search left half

(defn windowed-dataset->time-window-dataset
  "Return a regular dataset as a view over the content of a windowed dataset,
  including only a recent time window. Uses binary search for optimal performance.

  **Args:**
  
  - `windowed-dataset` - a `WindowedDataset`
  - `timestamp-colname` - the name of the column that contains timestamps
  - `time-window` - window length in ms (from most recent timestamp backwards)

  **Returns:**
  Dataset containing only data within the specified time window
  
  **Performance:** O(log n) time complexity using binary search"
  [{:as windowed-dataset
    :keys [dataset]}
   timestamp-colname
   time-window]
  (let [indices (windowed-dataset-indices windowed-dataset)]
    (cond
      ;; Handle empty dataset
      (empty? indices)
      (ds/select-rows dataset [])

      ;; Handle invalid time window
      (or (nil? time-window) (neg? time-window))
      (ds/select-rows dataset [])

      ;; Handle zero time window - return only the most recent point
      (zero? time-window)
      (ds/select-rows dataset [(last indices)])

      ;; Normal case - use binary search for optimal performance
      :else
      (let [timestamp-col (dataset timestamp-colname)]
        ;; Check if timestamp column exists
        (when (nil? timestamp-col)
          (throw (IllegalArgumentException. (str "Timestamp column '" timestamp-colname "' not found in dataset"))))

        (let [;; Get the latest timestamp as reference point
              latest-idx (last indices)
              latest-time (nth timestamp-col latest-idx)
              ;; Calculate start time for the window
              start-time (java-time/minus latest-time (java-time/millis time-window))
              ;; Use binary search to find the first timestamp >= start-time
              start-pos (binary-search-timestamp-start timestamp-col indices start-time)
              ;; Take all indices from start position to end (they're already in chronological order)
              filtered-indices (subvec (vec indices) start-pos)]
          (ds/select-rows dataset filtered-indices))))))

(defn windowed-dataset->rmssd
  "Compute RMSSD (Root Mean Square of Successive Differences) from a windowed dataset
  over a specified time window.
  
  RMSSD is a time-domain Heart Rate Variability measure that quantifies the 
  short-term variability in pulse-to-pulse intervals by calculating the root 
  mean square of the differences between successive intervals.
  
  **Args:**
  
  - `windowed-dataset` - a `WindowedDataset` containing PPI data
  - `timestamp-colname` - the name of the column that contains timestamps  
  - `time-window` - window length in ms (from most recent timestamp backwards)
  - `ppi-colname` - column name containing pulse-to-pulse intervals (default: :PpInMs)
  
  **Returns:**
  RMSSD value in milliseconds, or nil if insufficient data (< 2 intervals)
  
  **Performance:** O(log n) time complexity using binary search for time window extraction,
  with high-performance dtype-next operations for RMSSD calculation"
  ([windowed-dataset timestamp-colname time-window]
   (windowed-dataset->rmssd windowed-dataset timestamp-colname time-window :PpInMs))
  ([windowed-dataset timestamp-colname time-window ppi-colname]
   (let [time-window-data (windowed-dataset->time-window-dataset windowed-dataset
                                                                 timestamp-colname
                                                                 time-window)]
     (when (>= (tc/row-count time-window-data) 2)
       ;; Check if the PPI column exists before trying to access it
       (when (contains? (set (tc/column-names time-window-data)) ppi-colname)
         (let [ppi-values (tc/column time-window-data ppi-colname)]
           (when (and ppi-values (>= (count ppi-values) 2))
             ;; Use dtype-next operations for high-performance calculation
             (let [;; Convert to dtype array for efficient operations
                   ppi-array (dtype/->reader ppi-values :float64)
                   n (dtype/ecount ppi-array)]
               (when (>= n 2)
                 ;; Calculate successive differences using dtype-next shift operation
                 ;; This creates: [x1-x0, x2-x1, ..., xn-x(n-1)]
                 (let [current-vals (dtype/sub-buffer ppi-array 1 (dec n)) ; [x1, x2, ..., xn]
                       prev-vals (dtype/sub-buffer ppi-array 0 (dec n)) ; [x0, x1, ..., x(n-1)]
                       ;; Calculate differences: current - previous
                       diffs (dfn/- current-vals prev-vals)
                       ;; Square the differences
                       squared-diffs (dfn/sq diffs)
                       ;; Calculate mean of squared differences
                       mean-squared (dfn/mean squared-diffs)
                       ;; Return root mean square
                       rmssd (Math/sqrt mean-squared)]
                   rmssd))))))))))

(defn add-gaussian-noise
  "Add Gaussian (normal) noise to PPI intervals to simulate measurement variability.
  
  **Args:**
  - `data` - Dataset containing PPI intervals
  - `ppi-colname` - Column name containing PPI intervals (default: :PpInMs) 
  - `noise-std` - Standard deviation of noise in milliseconds (default: 5.0)
  
  **Returns:**
  Dataset with noisy PPI intervals
  
  **Example:**
  ```clojure
  ;; Add 5ms standard deviation noise to clean data
  (add-gaussian-noise clean-data :PpInMs 5.0)
  ```"
  ([data ppi-colname noise-std]
   (let [ppi-values (tc/column data ppi-colname)
         rng (random/rng :mersenne)
         noisy-values (mapv (fn [ppi]
                              (+ ppi (random/grandom rng 0.0 noise-std)))
                            ppi-values)]
     (tc/add-columns data {ppi-colname noisy-values})))
  ([data ppi-colname]
   (add-gaussian-noise data ppi-colname 5.0))
  ([data]
   (add-gaussian-noise data :PpInMs 5.0)))

(defn add-outliers
  "Add outlier artifacts to simulate sensor malfunctions or movement artifacts.
  
  **Args:**
  - `data` - Dataset containing PPI intervals
  - `ppi-colname` - Column name containing PPI intervals (default: :PpInMs)
  - `outlier-probability` - Probability of each sample being an outlier (default: 0.02 = 2%)
  - `outlier-magnitude` - Multiplier for outlier deviation (default: 3.0)
  
  **Returns:**
  Dataset with outlier artifacts added
  
  **Example:**
  ```clojure
  ;; Add outliers to 2% of samples with 3x normal deviation
  (add-outliers clean-data :PpInMs 0.02 3.0)
  ```"
  ([data ppi-colname outlier-probability outlier-magnitude]
   (let [ppi-values (tc/column data ppi-colname)
         mean-ppi (tcc/mean ppi-values)
         std-ppi (tcc/standard-deviation ppi-values)
         rng (random/rng :mersenne)
         outlier-values (mapv (fn [ppi]
                                (if (< (random/drandom rng) outlier-probability)
                                  ;; Create outlier: mean ± (outlier-magnitude * std)
                                  (let [sign (if (< (random/drandom rng) 0.5) -1 1)
                                        deviation (* sign outlier-magnitude std-ppi)]
                                    (+ mean-ppi deviation))
                                  ;; Keep original value
                                  ppi))
                              ppi-values)]
     (tc/add-columns data {ppi-colname outlier-values})))
  ([data ppi-colname outlier-probability]
   (add-outliers data ppi-colname outlier-probability 3.0))
  ([data ppi-colname]
   (add-outliers data ppi-colname 0.02 3.0))
  ([data]
   (add-outliers data :PpInMs 0.02 3.0)))

(defn add-missing-beats
  "Simulate missing heartbeat detections by randomly doubling some intervals.
  
  This simulates the common artifact where one heartbeat is missed, causing
  the next detected interval to be approximately twice as long.
  
  **Args:**
  - `data` - Dataset containing PPI intervals
  - `ppi-colname` - Column name containing PPI intervals (default: :PpInMs)
  - `missing-probability` - Probability of missing beat at each position (default: 0.01 = 1%)
  
  **Returns:**
  Dataset with missing beat artifacts (doubled intervals)
  
  **Example:**
  ```clojure
  ;; Simulate 1% missing beat rate
  (add-missing-beats clean-data :PpInMs 0.01)
  ```"
  ([data ppi-colname missing-probability]
   (let [ppi-values (tc/column data ppi-colname)
         rng (random/rng :mersenne)
         missing-beat-values (mapv (fn [ppi]
                                     (if (< (random/drandom rng) missing-probability)
                                       ;; Double the interval (missing beat)
                                       (* ppi 2)
                                       ;; Keep original value
                                       ppi))
                                   ppi-values)]
     (tc/add-columns data {ppi-colname missing-beat-values})))
  ([data ppi-colname]
   (add-missing-beats data ppi-colname 0.01))
  ([data]
   (add-missing-beats data :PpInMs 0.01)))

(defn add-extra-beats
  "Simulate false positive heartbeat detections by randomly halving some intervals.
  
  This simulates the common artifact where noise is detected as an extra heartbeat,
  causing one interval to be split into approximately two half-length intervals.
  
  **Args:**
  - `data` - Dataset containing PPI intervals
  - `ppi-colname` - Column name containing PPI intervals (default: :PpInMs)
  - `extra-probability` - Probability of extra beat at each position (default: 0.01 = 1%)
  
  **Returns:**
  Dataset with extra beat artifacts (halved intervals followed by normal intervals)
  
  **Note:** This function modifies the dataset length by inserting additional rows.
  
  **Example:**
  ```clojure
  ;; Simulate 1% extra beat rate
  (add-extra-beats clean-data :PpInMs 0.01)
  ```"
  ([data ppi-colname extra-probability]
   (let [rows (tc/rows data :as-maps)
         rng (random/rng :mersenne)
         modified-rows (reduce (fn [acc row]
                                 (let [ppi-val (get row ppi-colname)]
                                   (if (and ppi-val (< (random/drandom rng) extra-probability))
                                     ;; Add extra beat: split interval in half
                                     (let [half-interval (/ ppi-val 2)
                                           first-row (assoc row ppi-colname half-interval)
                                           second-row (assoc row ppi-colname half-interval)]
                                       (conj acc first-row second-row))
                                     ;; Keep original row
                                     (conj acc row))))
                               []
                               rows)]
     (tc/dataset modified-rows)))
  ([data ppi-colname]
   (add-extra-beats data ppi-colname 0.01))
  ([data]
   (add-extra-beats data :PpInMs 0.01)))

(defn add-trend-drift
  "Add gradual trend drift to simulate changes in autonomic state during measurement.
  
  This simulates the natural drift in heart rate that occurs during longer
  measurements due to postural changes, breathing patterns, or autonomic shifts.
  
  **Args:**
  - `data` - Dataset containing PPI intervals
  - `ppi-colname` - Column name containing PPI intervals (default: :PpInMs)
  - `drift-magnitude` - Maximum drift amount in milliseconds (default: 50.0)
  - `drift-direction` - Drift direction: :increase, :decrease, or :random (default: :random)
  
  **Returns:**
  Dataset with gradual trend drift added
  
  **Example:**
  ```clojure
  ;; Add gradual 50ms increase over the measurement period
  (add-trend-drift clean-data :PpInMs 50.0 :increase)
  ```"
  ([data ppi-colname drift-magnitude drift-direction]
   (let [ppi-values (tc/column data ppi-colname)
         n (count ppi-values)
         rng (random/rng :mersenne)
         ;; Determine drift direction
         direction-multiplier (case drift-direction
                                :increase 1.0
                                :decrease -1.0
                                :random (if (< (random/drandom rng) 0.5) -1.0 1.0)
                                1.0)
         ;; Create linear drift over time
         drift-values (mapv (fn [i ppi]
                              (let [progress (/ (double i) (double (dec n)))
                                    drift-amount (* direction-multiplier drift-magnitude progress)]
                                (+ ppi drift-amount)))
                            (range n)
                            ppi-values)]
     (tc/add-columns data {ppi-colname drift-values})))
  ([data ppi-colname drift-magnitude]
   (add-trend-drift data ppi-colname drift-magnitude :random))
  ([data ppi-colname]
   (add-trend-drift data ppi-colname 50.0 :random))
  ([data]
   (add-trend-drift data :PpInMs 50.0 :random)))

(defn distort-segment
  "Apply multiple realistic distortions to clean HRV data for algorithm evaluation.
  
  Combines multiple types of artifacts commonly seen in real HRV measurements
  to create realistic test data for evaluating cleaning and smoothing algorithms.
  
  **Args:**
  - `clean-data` - Clean dataset containing PPI intervals
  - `distortion-params` - Map containing distortion parameters:
    - `:noise-std` - Gaussian noise standard deviation (ms, default: 3.0)
    - `:outlier-prob` - Outlier probability (default: 0.015 = 1.5%)
    - `:outlier-magnitude` - Outlier magnitude multiplier (default: 2.5)
    - `:missing-prob` - Missing beat probability (default: 0.008 = 0.8%)
    - `:extra-prob` - Extra beat probability (default: 0.005 = 0.5%)
    - `:drift-magnitude` - Trend drift magnitude in ms (default: 30.0)
    - `:ppi-colname` - PPI column name (default: :PpInMs)
  
  **Returns:**
  Dataset with realistic distortions applied
  
  **Example:**
  ```clojure
  ;; Apply moderate distortions with default parameters
  (distort-segment clean-data {})
  
  ;; Apply heavy distortions
  (distort-segment clean-data {:noise-std 8.0
                               :outlier-prob 0.03
                               :missing-prob 0.02})
  ```"
  ([clean-data distortion-params]
   (let [params (merge {:noise-std 3.0
                        :outlier-prob 0.015
                        :outlier-magnitude 2.5
                        :missing-prob 0.008
                        :extra-prob 0.005
                        :drift-magnitude 30.0
                        :ppi-colname :PpInMs}
                       distortion-params)
         {:keys [noise-std outlier-prob outlier-magnitude missing-prob
                 extra-prob drift-magnitude ppi-colname]} params]

     ;; Apply distortions in sequence
     (-> clean-data
         ;; Add baseline noise
         (add-gaussian-noise ppi-colname noise-std)
         ;; Add occasional outliers
         (add-outliers ppi-colname outlier-prob outlier-magnitude)
         ;; Add missing beats (doubles intervals)
         (add-missing-beats ppi-colname missing-prob)
         ;; Add extra beats (splits intervals, increases row count)
         (add-extra-beats ppi-colname extra-prob)
         ;; Add gradual drift
         (add-trend-drift ppi-colname drift-magnitude))))
  ([clean-data]
   (distort-segment clean-data {})))


