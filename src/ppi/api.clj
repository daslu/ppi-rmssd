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

