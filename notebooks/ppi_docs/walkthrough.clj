(ns ppi-docs.walkthrough
  (:require ;; Data manipulation and analysis  
   [tablecloth.api :as tc] ; Dataset manipulation and analysis
   [tablecloth.column.api :as tcc] ; Column-level operations and statistics

   ;; Dataset printing and display
   [tech.v3.dataset.print :as print] ; Pretty printing datasets with formatting

   ;; Standard library utilities
   [clojure.string :as str] ; String manipulation functions
   [clojure.java.io :as io] ; File I/O operations

   ;; File system operations  
   [babashka.fs :as fs] ; Cross-platform file system utilities

   ;; Date and time handling
   [java-time.api :as java-time] ; Modern Java time API wrapper
   [tech.v3.datatype.datetime :as datetime] ; High-performance datetime operations

   ;; Visualization and notebook display
   [scicloj.tableplot.v1.plotly :as plotly] ; Plotly-based data visualization
   [scicloj.kindly.v4.kind :as kind] ; Clay notebook display utilities

   ;; Project-specific functionality
   [ppi.api :as ppi]))


(set! *warn-on-reflection* true)

;; Define paths to our data files:
(def raw-csv-path
  "data/query_result_2025-05-30T07_52_48.720548159Z.csv.gz")

;; Create a standardized version with cleaned quotes:
(def standard-csv-path
  (str/replace raw-csv-path #"\.csv\.gz" ".standard.csv.gz"))

(when-not (fs/exists? standard-csv-path)
  (ppi/prepare-standard-csv! raw-csv-path standard-csv-path))

;; Custom date-time format used in the CSV:
(def date-time-format
  "yyyy.M.d, HH:mm")

;; Load the dataset with proper datetime parsing for timestamp columns:
(def raw-data
  (-> standard-csv-path
      (tc/dataset {:parser-fn {"Created At" [:local-date-time date-time-format]
                               "Client Timestamp" [:local-date-time date-time-format]}})))

;; ## Initial Data Exploration
;;
;; Let's examine the structure and content of our PPI dataset to understand
;; what we're working with. This includes column types, data formats, and
;; sample records.

;; Check column types to understand what we're working with:
(into
 (sorted-map)
 (update-vals
  raw-data
  tcc/typeof))

;; Look at a sample row to understand the data format:
(-> raw-data
    (tc/rows :as-maps)
    first)

;; ## Additional preprocessing

;; Simplify column names and parse relevant numerical columns:

(def colname-prefix-to-remove
  (-> raw-data
      keys
      second
      (subs 0 14)))

(def prepared-data
  (ppi/prepare-raw-data raw-data colname-prefix-to-remove))

(-> prepared-data
    (tc/select-columns [:Device-UUID :Client-Timestamp :PpInMs :PpErrorEstimate]))

;; ## Understanding Temporal Patterns
;;
;; PPI data is fundamentally a time series. To work effectively with it, we need to:

;; 1. Understand the time range of our data collection
;; 2. Filter to relevant time periods  
;; 3. Examine data density and distribution patterns
;;
;; This temporal analysis helps us identify the best data segments for our research.

;; Find the earliest timestamp in our dataset:
(->> prepared-data
     :Client-Timestamp
     (reduce java-time/min))

;; Find the latest timestamp in our dataset:
(->> prepared-data
     :Client-Timestamp
     (reduce java-time/max))

;; ## Data Filtering
;;
;; Focus on recent data (2025 onwards) for our analysis:
(def recent-data
  (ppi/filter-recent-data prepared-data (java-time/local-date-time 2025 1 1)))

;; ## Exploratory Data Analysis
;;
;; Let's understand our dataset: how many devices, what the data looks like,
;; and how the pulse-to-pulse intervals are distributed.

;; ### Device Overview
;;
;; Count how many measurements we have per device to understand data density

;; Show device IDs ranked by number of measurements:
(-> recent-data
    (tc/group-by [:Device-UUID])
    (tc/aggregate {:n tc/row-count})
    (tc/order-by [:n] :desc))

;; ### Raw Time Series Visualization
;;
;; Plot pulse-to-pulse intervals over client timestamps for each device.
;; This shows the raw data before timestamp correction.

;; Create a line plot of PP intervals vs client timestamps, for one of the
;; devices.
(-> recent-data
    (tc/select-rows #(= (:Device-UUID %)
                        #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f"))
    (tc/order-by [:Client-Timestamp])
    (tc/select-columns [:Client-Timestamp :PpInMs])
    (plotly/layer-line {:=x :Client-Timestamp
                        :=y :PpInMs}))

;; Zoom in:
(-> recent-data
    (tc/select-rows #(and (= (:Device-UUID %)
                             #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f")
                          (java-time/after? (:Client-Timestamp %)
                                            (java-time/local-date-time 2025 5 27 8 30))
                          (java-time/before? (:Client-Timestamp %)
                                             (java-time/local-date-time 2025 5 27 9))))
    (tc/order-by [:Client-Timestamp])
    (tc/select-columns [:Client-Timestamp :PpInMs])
    (plotly/layer-line {:=x :Client-Timestamp
                        :=y :PpInMs}))

;; We see that the data samples are aggregated by minute (you can hover over the plot),
;; and that measurements are discontinuous (see the gap of about 3 days).

;; Later, we will generate more precise timestamps by accumulating
;; the durations between pulses, and we will partition the time
;; series into more contiuous segments.

;; **Distribution Analysis**
;; Histogram showing the distribution of pulse-to-pulse intervals:
(-> recent-data
    (plotly/layer-histogram {:=x :PpInMs
                             :=histogram-nbins 100}))


;; ## Precise Timestamp Calculation
;;
;; The client timestamps show when measurements were sent, but we need the actual
;; measurement times. We calculate precise timestamps by adding accumulated
;; pulse-to-pulse intervals to the client timestamp.

(def data-with-timestamps
  (ppi/add-timestamps recent-data))

;; **Corrected Time Series Visualization**
;; Now plot using the corrected timestamps - this shows the actual measurement timing

(-> data-with-timestamps
    (tc/select-rows #(= (:Device-UUID %)
                        #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f"))
    (tc/order-by [:timestamp])
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; Zoom in to see that now we have higher-resolution timestamps:

(-> data-with-timestamps
    (tc/select-rows #(and (= (:Device-UUID %)
                             #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f")
                          (java-time/after? (:timestamp %)
                                            (java-time/local-date-time 2025 5 27 8 30))
                          (java-time/before? (:timestamp %)
                                             (java-time/local-date-time 2025 5 27 9))))
    (tc/order-by [:timestamp])
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; The relationship between the minute-resolution original `:Client-Timestamp`
;; to the refined, higher-resolution, `:timestamp`:

(-> data-with-timestamps
    (tc/select-rows #(and (= (:Device-UUID %)
                             #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f")
                          (java-time/after? (:timestamp %)
                                            (java-time/local-date-time 2025 5 27 8 30))
                          (java-time/before? (:timestamp %)
                                             (java-time/local-date-time 2025 5 27 9))))
    (tc/order-by [:timestamp])
    (plotly/layer-point {:=x :timestamp
                         :=y :Client-Timestamp}))

;; ## Time Series Segmentation
;;
;; Medical devices can have interruptions in data collection (battery changes, 
;; device resets, etc.). We detect these gaps and segment the continuous periods.

;; **Jump Detection: The 5-Second Rule**
;; 
;; **Parameter Selection Rationale**: We use a 5000ms (5-second) jump threshold based on:
;; - Normal PPI intervals: 600-1200ms for healthy adults
;; - Maximum physiological change: ~3x during extreme exercise transitions
;; - Safety margin: 5 seconds allows for any conceivable physiological change
;;
;; This conservative threshold ensures we don't split continuous recordings while 
;; reliably detecting device interruptions, battery changes, or data transmission gaps.

(let [params {:jump-threshold 5000}
      device-data-with-jumps (-> data-with-timestamps
                                 (tc/select-rows #(= (:Device-UUID %)
                                                     #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f"))
                                 (ppi/recognize-jumps params)
                                 (tc/select-columns [:timestamp :delta-timestamp :jump :jump-count]))]
  (kind/hiccup
   [:div
    [:div {:style {:max-height "400px"
                   :overflow-y "auto"}}
     (-> device-data-with-jumps
         (print/print-range :all))]
    (-> device-data-with-jumps
        (plotly/layer-point {:=x :timestamp
                             :=y :jump-count}))]))

;; Now, let us partition the data into segments, where every jump in time
;; is a division between segments.

(let [params {:jump-threshold 5000}
      segments (-> data-with-timestamps
                   (tc/select-rows #(= (:Device-UUID %)
                                       #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f"))
                   (ppi/recognize-jumps params)
                   (tc/group-by [:jump-count] {:result-type :as-seq}))]
  (kind/hiccup
   (into [:div.limited-height]
         (comp
          (filter (fn [segment]
                    (-> segment
                        tc/row-count
                        (> 2))))
          (map (fn [segment]
                 (-> segment
                     (tc/order-by [:timestamp])
                     (plotly/layer-line {:=x :timestamp
                                         :=y :PpInMs})))))
         segments)))

;; ## Final data preparation

;; Here we prepare the main dataset to be used in this project.
;; 

(def continuous-pp-data
  (let [params {:jump-threshold 5000}]
    (-> data-with-timestamps
        (tc/select-columns [:Device-UUID :timestamp :PpErrorEstimate]))))

continuous-pp-data

(def continuous-csv-path "data/continuous-pp.csv.gz")

(when-not (fs/exists? continuous-csv-path)
  (tc/write! continuous-pp-data continuous-csv-path))


