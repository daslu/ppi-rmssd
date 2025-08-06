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

(def timestamped-ppi
  (ppi/prepare-timestamped-ppi-data
   "data/query_result_2025-05-30T07_52_48.720548159Z.standard.csv.gz"))

timestamped-ppi

(tc/info timestamped-ppi)


;; ## Segmenting the data

;; We will segment the data into relatively continuous segments.
;; A break of 5 seconds is considered a discontinuity --
;; a jump in time.

;; (Later in our analysis, we will pick a few relatively clean segments
;; and use them as ground truth to be distorted, to test our cleaning methods.)




