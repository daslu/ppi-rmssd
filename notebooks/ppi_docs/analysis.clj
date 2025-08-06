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

;; ## Measuring distortion impact

;; Now we can use our new `measure-distortion-impact` function to quantify
;; how distortion affects RMSSD calculations in a clean, systematic way.

(let [;; Define distortion parameters
      distortion-params {:noise-std 15.0
                         :outlier-prob 0.08
                         :outlier-magnitude 3.5
                         :missing-prob 0.02
                         :extra-prob 0.015}

      ;; Configure RMSSD calculation
      rmssd-config {:colname :RMSSD
                    :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                    :windowed-dataset-size 240}

      ;; Measure the distortion impact
      impact-result (ppi/measure-distortion-impact clean-segment-example
                                                   distortion-params
                                                   rmssd-config)]

  ;; Display the results using Clay's hiccup rendering  
  (kind/hiccup
   [:div
    [:h3 "Distortion Impact Analysis"]
    [:table {:style {:border-collapse "collapse" :width "100%"}}
     [:tbody
      [:tr
       [:td {:style {:font-weight "bold" :padding "8px" :border "1px solid #ddd"}} "Mean relative error:"]
       [:td {:style {:padding "8px" :border "1px solid #ddd"}}
        (format "%.1f%%" (* 100 (:mean-relative-error impact-result)))]]
      [:tr
       [:td {:style {:font-weight "bold" :padding "8px" :border "1px solid #ddd"}} "Valid measurement pairs:"]
       [:td {:style {:padding "8px" :border "1px solid #ddd"}}
        (format "%d" (:n-valid-pairs impact-result))]]]]
    [:h4 "Interpretation:"]
    [:ul
     [:li (format "The distortion causes RMSSD to be %.1f%% different on average"
                  (* 100 (Math/abs (:mean-relative-error impact-result))))]
     [:li (format "Analysis based on %d valid time points" (:n-valid-pairs impact-result))]]]))

;; This approach is much cleaner and more systematic than the manual calculation.
;; The function handles edge cases, provides comprehensive results, and can be
;; easily used for comparing different distortion levels or cleaning algorithms.

;; ## Comparing distortion levels

;; Let's demonstrate how easy it is to compare different distortion scenarios:

(let [rmssd-config {:colname :RMSSD
                    :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                    :windowed-dataset-size 240}

      ;; Different distortion scenarios
      scenarios {"Light distortion" {:noise-std 5.0 :outlier-prob 0.02}
                 "Medium distortion" {:noise-std 10.0 :outlier-prob 0.05}
                 "Heavy distortion" {:noise-std 20.0 :outlier-prob 0.10 :outlier-magnitude 4.0}
                 "Comprehensive distortion" {:noise-std 15.0
                                             :outlier-prob 0.08
                                             :outlier-magnitude 3.5
                                             :missing-prob 0.02
                                             :extra-prob 0.015}}

      ;; Measure impact for each scenario
      results (into {}
                    (map (fn [[name params]]
                           [name (-> (ppi/measure-distortion-impact clean-segment-example
                                                                    params
                                                                    rmssd-config)
                                     (select-keys [:mean-relative-error
                                                   :n-valid-pairs]))])
                         scenarios))]

  (kind/hiccup
   [:div
    [:h3 "Distortion Impact Comparison"]
    [:table {:style {:border-collapse "collapse" :width "100%" :margin-top "10px"}}
     [:thead
      [:tr {:style {:background-color "#f5f5f5"}}
       [:th {:style {:padding "12px" :border "1px solid #ddd" :text-align "left"}} "Scenario"]
       [:th {:style {:padding "12px" :border "1px solid #ddd" :text-align "right"}} "Mean Error"]
       [:th {:style {:padding "12px" :border "1px solid #ddd" :text-align "right"}} "Valid Pairs"]]]
     [:tbody
      (for [[scenario result] (sort-by #(-> % second :mean-relative-error) results)]
        [:tr
         [:td {:style {:padding "10px" :border "1px solid #ddd"}} scenario]
         [:td {:style {:padding "10px" :border "1px solid #ddd" :text-align "right"}}
          (format "%.1f%%" (* 100 (:mean-relative-error result)))]
         [:td {:style {:padding "10px" :border "1px solid #ddd" :text-align "right"}}
          (format "%d" (:n-valid-pairs result))]])]]])

  ;; Return results for further analysis
  results)

;; ## Systematic Smoothing Algorithm Comparison

;; Let's compare different smoothing algorithms to see how well they can
;; restore clean RMSSD values after various types of distortion.

(let [;; Base RMSSD configuration
      base-rmssd-config {:colname :RMSSD
                         :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                         :windowed-dataset-size 240}

      ;; Smoothing algorithm configurations
      smoothing-algorithms {"No smoothing" base-rmssd-config

                            "Moving average (5pt)"
                            {:colname :RMSSD-MA5
                             :windowed-fn #(ppi/moving-average % 5)
                             :windowed-dataset-size 240}

                            "Moving average (10pt)"
                            {:colname :RMSSD-MA10
                             :windowed-fn #(ppi/moving-average % 10)
                             :windowed-dataset-size 240}

                            "Median filter (5pt)"
                            {:colname :RMSSD-Med5
                             :windowed-fn #(ppi/median-filter % 5)
                             :windowed-dataset-size 240}

                            "Median filter (7pt)"
                            {:colname :RMSSD-Med7
                             :windowed-fn #(ppi/median-filter % 7)
                             :windowed-dataset-size 240}

                            "Cascaded median"
                            {:colname :RMSSD-CascMed
                             :windowed-fn #(ppi/cascaded-median-filter %)
                             :windowed-dataset-size 240}

                            "Exponential MA (α=0.3)"
                            {:colname :RMSSD-EMA3
                             :windowed-fn #(ppi/exponential-moving-average % 0.3)
                             :windowed-dataset-size 240}

                            "Exponential MA (α=0.1)"
                            {:colname :RMSSD-EMA1
                             :windowed-fn #(ppi/exponential-moving-average % 0.1)
                             :windowed-dataset-size 240}}

      ;; Different distortion scenarios  
      distortion-scenarios {"Light noise" {:noise-std 8.0}

                            "Heavy noise" {:noise-std 20.0}

                            "Outliers" {:noise-std 5.0 :outlier-prob 0.08 :outlier-magnitude 3.0}

                            "Missing beats" {:noise-std 5.0 :missing-prob 0.03}

                            "Extra beats" {:noise-std 5.0 :extra-prob 0.02}

                            "Combined artifacts" {:noise-std 12.0
                                                  :outlier-prob 0.05
                                                  :outlier-magnitude 2.5
                                                  :missing-prob 0.015
                                                  :extra-prob 0.01}}

      ;; Test each smoothing algorithm against each distortion scenario
      results (for [[smoothing-name smoothing-config] smoothing-algorithms
                    [distortion-name distortion-params] distortion-scenarios]
                (try
                  (let [impact (ppi/measure-distortion-impact clean-segment-example
                                                              distortion-params
                                                              smoothing-config)]
                    {:smoothing smoothing-name
                     :distortion distortion-name
                     :error (* 100 (Math/abs (:mean-relative-error impact)))
                     :valid-pairs (:n-valid-pairs impact)})
                  (catch Exception e
                    {:smoothing smoothing-name
                     :distortion distortion-name
                     :error "Failed"
                     :valid-pairs 0})))]

  ;; Display results in a comprehensive table
  (kind/hiccup
   [:div
    [:h3 "Smoothing Algorithm Performance Comparison"]
    [:p "Mean absolute relative error (%) across different distortion scenarios:"]

    ;; Create comparison table
    [:table {:style {:border-collapse "collapse" :width "100%" :margin "20px 0"}}

     ;; Header row
     [:thead
      [:tr {:style {:background-color "#f8f9fa"}}
       [:th {:style {:padding "12px" :border "1px solid #dee2e6" :text-align "left"}} "Algorithm"]
       (for [distortion-name (map first distortion-scenarios)]
         [:th {:style {:padding "12px" :border "1px solid #dee2e6" :text-align "center"}} distortion-name])]]

     ;; Data rows
     [:tbody
      (for [smoothing-name (map first smoothing-algorithms)]
        [:tr
         [:td {:style {:padding "10px" :border "1px solid #dee2e6" :font-weight "bold"}} smoothing-name]
         (for [distortion-name (map first distortion-scenarios)]
           (let [result (first (filter #(and (= (:smoothing %) smoothing-name)
                                             (= (:distortion %) distortion-name))
                                       results))
                 error (:error result)
                 cell-style (cond
                              (= error "Failed") {:padding "10px" :border "1px solid #dee2e6"
                                                  :text-align "center" :background-color "#ffe6e6"}
                              (< error 50) {:padding "10px" :border "1px solid #dee2e6"
                                            :text-align "center" :background-color "#e8f5e8"}
                              (< error 100) {:padding "10px" :border "1px solid #dee2e6"
                                             :text-align "center" :background-color "#fff3cd"}
                              :else {:padding "10px" :border "1px solid #dee2e6"
                                     :text-align "center" :background-color "#f8d7da"})]
             [:td {:style cell-style}
              (if (number? error)
                (format "%.0f%%" error)
                error)]))])]]

    [:div {:style {:margin-top "20px"}}
     [:h4 "Color Legend:"]
     [:ul {:style {:list-style-type "none" :padding-left "0"}}
      [:li [:span {:style {:background-color "#e8f5e8" :padding "3px 8px" :margin-right "10px"}} "  "] "< 50% error (Good)"]
      [:li [:span {:style {:background-color "#fff3cd" :padding "3px 8px" :margin-right "10px"}} "  "] "50-100% error (Moderate)"]
      [:li [:span {:style {:background-color "#f8d7da" :padding "3px 8px" :margin-right "10px"}} "  "] "> 100% error (Poor)"]
      [:li [:span {:style {:background-color "#ffe6e6" :padding "3px 8px" :margin-right "10px"}} "  "] "Failed"]]]]))

;; ## Comprehensive comparison over many clean segments

(def a-few-clean-segments
  (->> (tc/group-by segmented-data [:Device-UUID :jump-count] {:result-type :as-seq})
       (filter #(ppi/clean-segment? % clean-params))
       (sort-by (fn [segment] ; shuffle the segments a bit:
                  (-> segment
                      tc/rows
                      first
                      hash)))
       (take 10))) ; Take first 10 clean segments for testing

(count a-few-clean-segments)

;; Now let's test our smoothing algorithms across multiple clean segments
;; to get a more robust assessment of their performance.

(let [;; Use a subset of algorithms for comprehensive testing
      key-algorithms {"No smoothing" {:colname :RMSSD
                                      :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                                      :windowed-dataset-size 240}

                      "Moving average (5pt)" {:colname :RMSSD-MA5
                                              :windowed-fn #(ppi/moving-average % 5)
                                              :windowed-dataset-size 240}

                      "Median filter (5pt)" {:colname :RMSSD-Med5
                                             :windowed-fn #(ppi/median-filter % 5)
                                             :windowed-dataset-size 240}

                      "Cascaded median" {:colname :RMSSD-CascMed
                                         :windowed-fn #(ppi/cascaded-median-filter %)
                                         :windowed-dataset-size 240}

                      "Exponential MA (α=0.2)" {:colname :RMSSD-EMA2
                                                :windowed-fn #(ppi/exponential-moving-average % 0.2)
                                                :windowed-dataset-size 240}}

      ;; Focus on key distortion scenarios
      key-scenarios {"Light noise" {:noise-std 8.0}
                     "Heavy noise" {:noise-std 20.0}
                     "Outliers" {:noise-std 5.0 :outlier-prob 0.08 :outlier-magnitude 3.0}
                     "Combined artifacts" {:noise-std 12.0
                                           :outlier-prob 0.05
                                           :outlier-magnitude 2.5
                                           :missing-prob 0.015
                                           :extra-prob 0.01}}

      ;; Test each algorithm-scenario combination across all clean segments
      all-results (for [[algorithm-name algorithm-config] key-algorithms
                        [scenario-name scenario-params] key-scenarios]
                    (let [segment-results (pmap (fn [segment]
                                                  (try
                                                    (let [impact (ppi/measure-distortion-impact segment
                                                                                                scenario-params
                                                                                                algorithm-config)]
                                                      {:error (* 100 (Math/abs (:mean-relative-error impact)))
                                                       :valid-pairs (:n-valid-pairs impact)
                                                       :success true})
                                                    (catch Exception e
                                                      {:error nil :valid-pairs 0 :success false})))
                                                a-few-clean-segments)

                          successful-results (filter :success segment-results)
                          errors (keep :error successful-results)]

                      {:algorithm algorithm-name
                       :scenario scenario-name
                       :mean-error (if (seq errors) (/ (reduce + errors) (count errors)) nil)
                       :std-error (when (> (count errors) 1)
                                    (let [mean-err (/ (reduce + errors) (count errors))
                                          variance (/ (reduce + (map #(* (- % mean-err) (- % mean-err)) errors))
                                                      (dec (count errors)))]
                                      (Math/sqrt variance)))
                       :n-segments (count successful-results)
                       :total-segments (count a-few-clean-segments)}))

      ;; Group results by algorithm for display
      results-by-algo (group-by :algorithm all-results)]

  ;; Display comprehensive results
  (kind/hiccup
   [:div
    [:h3 "Multi-Segment Smoothing Performance Analysis"]
    [:p (format "Performance averaged across %d clean segments:" (count a-few-clean-segments))]

    [:table {:style {:border-collapse "collapse" :width "100%" :margin "20px 0"}}
     [:thead
      [:tr {:style {:background-color "#f8f9fa"}}
       [:th {:style {:padding "12px" :border "1px solid #dee2e6" :text-align "left"}} "Algorithm"]
       (for [scenario-name (map first key-scenarios)]
         [:th {:style {:padding "12px" :border "1px solid #dee2e6" :text-align "center"}} scenario-name])]]

     [:tbody
      (for [[algorithm-name algo-results] results-by-algo]
        [:tr
         [:td {:style {:padding "10px" :border "1px solid #dee2e6" :font-weight "bold"}} algorithm-name]
         (for [scenario-name (map first key-scenarios)]
           (let [result (first (filter #(= (:scenario %) scenario-name) algo-results))
                 mean-error (:mean-error result)
                 std-error (:std-error result)
                 n-segments (:n-segments result)
                 cell-style (cond
                              (nil? mean-error) {:padding "10px" :border "1px solid #dee2e6"
                                                 :text-align "center" :background-color "#ffe6e6"}
                              (< mean-error 50) {:padding "10px" :border "1px solid #dee2e6"
                                                 :text-align "center" :background-color "#e8f5e8"}
                              (< mean-error 100) {:padding "10px" :border "1px solid #dee2e6"
                                                  :text-align "center" :background-color "#fff3cd"}
                              :else {:padding "10px" :border "1px solid #dee2e6"
                                     :text-align "center" :background-color "#f8d7da"})]
             [:td {:style cell-style}
              (if mean-error
                [:div
                 [:div {:style {:font-weight "bold"}} (format "%.1f%%" mean-error)]
                 [:div {:style {:font-size "0.8em" :color "#666"}}
                  (if std-error
                    (format "±%.1f (n=%d)" std-error n-segments)
                    (format "(n=%d)" n-segments))]]
                "Failed")]))])]]

    [:div {:style {:margin-top "20px"}}
     [:h4 "Interpretation:"]
     [:ul
      [:li "Values show mean ± standard deviation of relative error across segments"]
      [:li "n = number of segments successfully processed"]
      [:li [:strong "Lower values are better"] " - they indicate less distortion of RMSSD"]
      [:li "Standard deviation shows consistency across different data conditions"]]]

    [:div {:style {:margin-top "15px"}}
     [:h4 "Color Legend:"]
     [:ul {:style {:list-style-type "none" :padding-left "0"}}
      [:li [:span {:style {:background-color "#e8f5e8" :padding "3px 8px" :margin-right "10px"}} "  "] "< 50% error (Excellent)"]
      [:li [:span {:style {:background-color "#fff3cd" :padding "3px 8px" :margin-right "10px"}} "  "] "50-100% error (Good)"]
      [:li [:span {:style {:background-color "#f8d7da" :padding "3px 8px" :margin-right "10px"}} "  "] "> 100% error (Poor)"]
      [:li [:span {:style {:background-color "#ffe6e6" :padding "3px 8px" :margin-right "10px"}} "  "] "Failed"]]]]))


