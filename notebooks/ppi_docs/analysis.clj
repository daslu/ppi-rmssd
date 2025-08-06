;; # Analysis walkthrough

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

(tc/info segmented-data)

;; Recall that `:jump-count` is used to recognize continuous segments.
;; A break of 5 seconds is considered a discontinuity --
;; a jump in time.

;; A segment is defined by specific values of
;; `:Device-UUID` and `:jump-count`.

;; Later in our analysis, we will pick a few relatively clean segments
;; and use them as ground truth to be distorted, to test our cleaning methods.

;; ## Finding clean segments

;; Let us explore our 'clean segment' criteria with the segments of one device.
;;
;; Clean segments are high-quality PPI data suitable for ground truth analysis.
;; The `ppi/clean-segment?` function identifies segments meeting five criteria:
;;
;; 1. **Sufficient samples** (≥25 points) and **duration** (≥30 seconds)
;; 2. **Low error** (≤15ms average measurement uncertainty)  
;; 3. **Stable heart rate** (≤15% coefficient of variation)
;; 4. **Smooth transitions** (≤30% maximum successive change)
;;
;; These segments represent normal sinus rhythm periods with minimal artifacts,
;; providing reliable reference data for algorithm validation and HRV analysis.

;; Note that many of the segments simply have too little data to be considered clean.

(def clean-params
  {:max-error-estimate 15
   :max-heart-rate-cv 15
   :max-successive-change 30
   :min-clean-duration 30000
   :min-clean-samples 25})

;; ### A few examples

;; Let us focus on one device, and check which of its segments are considered clean:

(let [segments (-> segmented-data
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
                 (let [clean (ppi/clean-segment? segment clean-params)]
                   [:div {:style {:background-color (if clean "#ddffdd" "#ffdddd")}}
                    [:p "clean? " clean]
                    (-> segment
                        (tc/order-by [:timestamp])
                        (plotly/base {:=height 200})
                        (plotly/layer-line {:=x :timestamp
                                            :=y :PpInMs}))]))))
         segments)))

;; ### Clean segment statistics

(-> segmented-data
    (tc/group-by [:Device-UUID :jump-count])
    (tc/aggregate {:clean #(ppi/clean-segment? % clean-params)})
    (tc/group-by [:Device-UUID])
    (tc/aggregate {:n-segments tc/row-count
                   :n-clean #(int (tcc/sum (:clean %)))})
    (tc/map-columns :clean-percentage
                    [:n-clean :n-segments]
                    #(/ (* 100.0 %1) %2)))

;; While our definition of "clean" might be a bit relaxed, the purpose is to serve
;; as ground truth for our experiments in distorting the date and the cleaning them back.

;; Since we need a decent amount of samples, this choice of parameters seems like
;; a reasonable compromise.

;; ### How to clean segments look?

;; Let us visualized a few more clean segments, so we can have a visual idea
;; of the kind of data we are handling.

(let [segments (tc/group-by segmented-data
                            [:Device-UUID :jump-count]
                            {:result-type :as-seq})]
  (kind/hiccup
   (into [:div.limited-height]
         (->> segments
              (filter #(ppi/clean-segment? % clean-params))
              (sort-by (fn [segment] ; shuffle the segments a bit:
                         (-> segment
                             tc/rows
                             first
                             hash)))
              (take 10)
              (map (fn [segment]
                     [:div {:style {:background-color "#ddffdd"}}
                      [:p "Device: " (-> segment :Device-UUID first)]
                      [:p "Jump #: " (-> segment :jump-count first)]
                      (-> segment
                          (tc/order-by [:timestamp])
                          (plotly/base {:=height 200})
                          (plotly/layer-line {:=x :timestamp
                                              :=y :PpInMs}))]))))))

;; ## Computing time-window RMSSD

;; Let us see a few examples of computing time-window RMSSD
;; over segments that we considered clean.

;; Our RMSSD function is `ppi/windowed-dataset->rmssd`,
;; which is built to work with the `WindowedDataset` efficient construct
;; that is explained int he API reference.

;; It is a bit delicate to use, as it is a mutable construct.

;; While it is built to work with streaming data, 
;; we have the `ppi/add-column-by-windowed-fn` function
;; that applies it sequentially
;; to an in-memory time-series and adds an appropriate column.



(let [segments (tc/group-by segmented-data
                            [:Device-UUID :jump-count]
                            {:result-type :as-seq})
      windowed-dataset-size 240
      time-window 60000]
  (kind/hiccup
   (into
    [:div.limited-height]
    (->> segments
         (filter #(ppi/clean-segment? % clean-params))
         (sort-by (fn [segment] ; shuffle the segments a bit:
                    (-> segment
                        tc/rows
                        first
                        hash)))
         (take 10)
         (map
          (fn [segment]
            [:div {:style {:background "#dddddd"}}
             [:p "Device: " (-> segment :Device-UUID first)]
             [:p "Jump #: " (-> segment :jump-count first)]
             (-> segment
                 (plotly/base {:=height 200})
                 (plotly/layer-line {:=x :timestamp
                                     :=y :PpInMs
                                     :=height 200}))
             (-> segment
                 (ppi/add-column-by-windowed-fn {:colname :RMSSD
                                                 :windowed-fn #(ppi/windowed-dataset->rmssd
                                                                % :timestamp time-window)
                                                 :windowed-dataset-size windowed-dataset-size})
                 (plotly/base {:=height 200})
                 (plotly/layer-line {:=x :timestamp
                                     :=y :RMSSD
                                     :=mark-color "brown"}))]))))))



;; ## Distorting clean segments

;; To evaluate our cleaning algorithms, we will take relatively
;; clean segments as ground truth, and distort them.

;; Let us take one relatively clean segment and plot it again.

(def clean-segment-example
  (-> segmented-data
      (tc/select-rows #(= (:Device-UUID %)
                          #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f"))
      (tc/group-by [:jump-count] {:result-type :as-seq})
      second))

(-> clean-segment-example
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "Clean"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; Now let's apply some distortion functions to see how artifacts affect the clean signal:

;; ### Adding Gaussian Noise

(-> clean-segment-example
    (ppi/add-gaussian-noise :PpInMs 25.0)
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "Noisy (25ms σ)"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; ### Adding Outliers

(-> clean-segment-example
    (ppi/add-outliers :PpInMs 0.15 4.0)
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "With Outliers"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; ### Comprehensive Distortion

(-> clean-segment-example
    (ppi/distort-segment {:noise-std 15.0
                          :outlier-prob 0.08
                          :outlier-magnitude 3.5
                          :missing-prob 0.02
                          :extra-prob 0.015})
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "Fully Distorted"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; ### Comparison: Clean vs Distorted

;; Let's compare the clean and distorted signals side by side:

(let [clean-plot (-> clean-segment-example
                     (tc/add-columns {:signal-type "Clean"})
                     (tc/order-by [:timestamp]))
      distorted-plot (-> clean-segment-example
                         (ppi/distort-segment {:noise-std 15.0
                                               :outlier-prob 0.08
                                               :outlier-magnitude 3.5
                                               :missing-prob 0.02
                                               :extra-prob 0.015})
                         (tc/add-columns {:signal-type "Distorted"})
                         (tc/order-by [:timestamp]))
      combined-data (tc/concat clean-plot distorted-plot)]

  (-> combined-data
      (plotly/base {:=height 300})
      (plotly/layer-line {:=x :timestamp
                          :=y :PpInMs
                          :=color :signal-type})))


;; ## Measuring distortion

(let [distorted-segment (ppi/distort-segment clean-segment-example
                                             {:noise-std 15.0
                                              :outlier-prob 0.08
                                              :outlier-magnitude 3.5
                                              :missing-prob 0.02
                                              :extra-prob 0.015})
      time-window 60000
      windowed-dataset-size 240
      add-rmssd (fn [time-series]
                  (ppi/add-column-by-windowed-fn
                   time-series
                   {:colname :RMSSD
                    :windowed-fn #(ppi/windowed-dataset->rmssd
                                   % :timestamp time-window)
                    :windowed-dataset-size windowed-dataset-size}))]
  (->> [clean-segment-example
        distorted-segment]
       (map add-rmssd)
       (apply #(tc/left-join %1 %2 [:timestamp]))
       ((fn [joined-ds]
          joined-ds
          (let [real (:RMSSD joined-ds)
                distorted (:right.RMSSD joined-ds)]
            (-> distorted
                (tcc/- real)
                (tcc// real)
                tcc/mean))))))
