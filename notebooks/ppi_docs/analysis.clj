(ns ppi-docs.analysis
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

;; ## Data preparation

;; Following the data preparation chapter, let us create the dataset
;; to be used in this analysis:

(def segmented-data
  (let [params {:jump-threshold 5000}]
    (-> "data/query_result_2025-05-30T07_52_48.720548159Z.standard.csv.gz"
        ppi/prepare-timestamped-ppi-data
        (ppi/recognize-jumps params))))

segmented-data

(tc/info timestamped-ppi)

;; Recall that `:jump-count` is used to recognize continuous segments.
;; A break of 5 seconds is considered a discontinuity --
;; a jump in time.

;; A segment is defined by specific values of
;; `:Device-UUID` and `:jump-count`.

;; Later in our analysis, we will pick a few relatively clean segments
;; and use them as ground truth to be distorted, to test our cleaning methods.


;; ## Finding clean segments


;; Let us explore our 'clean segment' criteria with the segments of one device:

(let [clean-params {:max-error-estimate 15
                    :max-heart-rate-cv 15
                    :max-successive-change 30
                    :min-clean-duration 30000
                    :min-clean-samples 25}
      segments (-> segmented-data
                   (tc/select-rows #(= (:Device-UUID %)
                                       #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f"))
                   (tc/group-by [:jump-count] {:result-type :as-seq}))]
  (kind/hiccup
   (into [:div.limited-height]
         (comp
          (filter (fn [segment]
                    (-> segment
                        tc/row-count
                        (> 2))))
          (map (fn [segment]
                 [:div
                  (kind/code (pr-str {:clean (ppi/clean-segment? segment clean-params)}))
                  (-> segment
                      (tc/order-by [:timestamp])
                      (plotly/layer-line {:=x :timestamp
                                          :=y :PpInMs}))])))
         segments)))
