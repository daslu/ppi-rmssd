(ns ppi.api
  (:require ;; Data manipulation and analysis
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
  Dataset with additional columns:
  
  - `:delta-timestamp` - time difference from previous measurement (ms)
  - `:jump` - binary flag (`1` if jump detected, `0` otherwise)  
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
      tc/ungroup))

