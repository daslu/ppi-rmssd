;; # Real-Time RMSSD Smoothing for Relaxation Monitoring
;;
;; A data science analysis to find stable methods for heart rate variability monitoring.

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

;; ## The Problem We're Solving
;;
;; Heart rate variability (HRV) monitoring using RMSSD is valuable for relaxation training,
;; but real-time measurements are often noisy and unstable. Users need smooth, interpretable
;; feedback rather than jumpy numbers that change dramatically from moment to moment.
;;
;; This analysis tests different smoothing algorithms to find the best approach for
;; stable real-time RMSSD computation that still responds meaningfully to actual changes
;; in heart rate patterns.
;;
;; ## Analysis Plan
;;
;; Our approach:

;; 1. **Find clean reference data** - Identify high-quality PPI segments to use as ground truth
;; 2. **Add realistic artifacts** - Simulate measurement noise, outliers, and missing beats  
;; 3. **Test smoothing algorithms** - Compare moving averages, median filters, and cascaded approaches
;; 4. **Measure performance** - Quantify how well each algorithm restores clean RMSSD values
;; 5. **Validate across segments** - Test on multiple data segments for robust results
;; 6. **Make recommendations** - Choose the best algorithm for real-world implementation

;; ## Our Data: Pulse-to-Pulse Intervals from Polar Devices
;;
;; We're working with real PPI data collected during relaxation sessions.
;; Let's prepare the dataset and see what we have:

(def segmented-data
  (let [params {:jump-threshold 5000}]
    (-> "data/query_result_2025-05-30T07_52_48.720548159Z.standard.csv.gz"
        ppi/prepare-timestamped-ppi-data
        (ppi/recognize-jumps params))))

(tc/info segmented-data)

;; The data contains pulse-to-pulse intervals measured in milliseconds, with timestamps
;; and device identifiers. We've detected measurement discontinuities (gaps longer than 5 seconds)
;; and split the data into continuous segments for analysis.

;; Each segment is identified by `:Device-UUID` and `:jump-count` values.

;; ## Finding Clean Reference Data
;;
;; To test our smoothing algorithms, we need some "ground truth" - clean segments
;; that represent good quality HRV data. We'll use these to simulate realistic
;; noise and then see how well different algorithms can restore the original signal.

;; Our criteria for clean segments:

;; - At least 30 seconds of data with 25+ measurements  
;; - Low measurement uncertainty (≤15ms average error)
;; - Stable heart rate (≤15% variation)
;; - Smooth beat-to-beat transitions (≤30% successive changes)

(def clean-params
  {:max-error-estimate 15
   :max-heart-rate-cv 15
   :max-successive-change 30
   :min-clean-duration 30000
   :min-clean-samples 25})

;; Let's see how much clean data we have:

(-> segmented-data
    (tc/group-by [:Device-UUID :jump-count])
    (tc/aggregate {:clean #(ppi/clean-segment? % clean-params)})
    (tc/group-by [:Device-UUID])
    (tc/aggregate {:n-segments tc/row-count
                   :n-clean #(int (tcc/sum (:clean %)))})
    (tc/map-columns :clean-percentage
                    [:n-clean :n-segments]
                    #(/ (* 100.0 %1) %2)))

;; About 10-30% of our segments meet the "clean" criteria, which gives us
;; a good foundation for testing. Here's what clean PPI data looks like:

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
              (take 6)
              (map (fn [segment]
                     [:div {:style {:background-color "#f8f9fa" :margin "10px 0" :padding "10px"}}
                      [:p {:style {:margin "5px 0" :font-size "0.9em"}}
                       "Device: " (-> segment :Device-UUID first str (subs 0 8)) "..."]
                      (-> segment
                          (tc/order-by [:timestamp])
                          (plotly/base {:=height 150})
                          (plotly/layer-line {:=x :timestamp
                                              :=y :PpInMs}))]))))))

;; Clean segments show the natural rhythm of a healthy heart - mostly steady intervals
;; with gentle variations that reflect normal physiological processes.

;; ## The Challenge: How Noise Affects RMSSD
;;
;; Real-world HRV data is never perfectly clean. Let's see how different types
;; of measurement artifacts affect RMSSD calculations.

;; First, let's pick one clean segment to work with:

(def clean-segment-example
  (-> segmented-data
      (tc/select-rows #(= (:Device-UUID %)
                          #uuid "8d453046-24f2-921e-34be-7ed0d7a37d6f"))
      (tc/group-by [:jump-count] {:result-type :as-seq})
      second))

;; ### Original Clean Signal

(-> clean-segment-example
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "Original Clean PPI Signal"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; ### Effect of Different Distortions

;; **Adding Gaussian Noise** (simulates measurement uncertainty):

(-> clean-segment-example
    (ppi/add-gaussian-noise :PpInMs 25.0)
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "With Measurement Noise (25ms σ)"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; **Adding Outliers** (simulates detection errors):

(-> clean-segment-example
    (ppi/add-outliers :PpInMs 0.15 4.0)
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "With Outlier Artifacts"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; **Combined Realistic Distortion**:

(-> clean-segment-example
    (ppi/distort-segment {:noise-std 15.0
                          :outlier-prob 0.08
                          :outlier-magnitude 3.5
                          :missing-prob 0.02
                          :extra-prob 0.015})
    (tc/order-by [:timestamp])
    (plotly/base {:=height 200
                  :=title "Realistic Mixed Artifacts"})
    (plotly/layer-line {:=x :timestamp
                        :=y :PpInMs}))

;; ### The RMSSD Problem
;;
;; Now let's see how these distortions affect RMSSD calculations.
;; RMSSD measures the variation between consecutive heartbeats - it's sensitive
;; to noise and artifacts, which is exactly our problem.

(let [rmssd-config {:colname :RMSSD
                    :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                    :windowed-dataset-size 240}

      ;; Calculate RMSSD for clean data
      clean-with-rmssd (-> clean-segment-example
                           (ppi/add-column-by-windowed-fn rmssd-config))

      ;; Calculate RMSSD for distorted data  
      distorted-with-rmssd (-> clean-segment-example
                               (ppi/distort-segment {:noise-std 15.0
                                                     :outlier-prob 0.08
                                                     :outlier-magnitude 3.5
                                                     :missing-prob 0.02
                                                     :extra-prob 0.015})
                               (ppi/add-column-by-windowed-fn rmssd-config))

      ;; Combine for comparison
      combined-rmssd (-> (tc/concat (tc/add-columns clean-with-rmssd {:signal-type "Clean"})
                                    (tc/add-columns distorted-with-rmssd {:signal-type "Distorted"}))
                         (tc/select-rows #(not (nil? (:RMSSD %)))))] ; Remove nil RMSSD values

  (-> combined-rmssd
      (plotly/base {:=height 300
                    :=title "RMSSD Comparison: Clean vs Distorted Data"})
      (plotly/layer-line {:=x :timestamp
                          :=y :RMSSD
                          :=color :signal-type})))

;; The distorted RMSSD signal is much more volatile and has higher overall values.
;; This is the core problem: users would see confusing, jumpy feedback instead
;; of the smoother trends they need for relaxation training.

;; Let's quantify the impact:

(let [distortion-params {:noise-std 15.0
                         :outlier-prob 0.08
                         :outlier-magnitude 3.5
                         :missing-prob 0.02
                         :extra-prob 0.015}

      rmssd-config {:colname :RMSSD
                    :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                    :windowed-dataset-size 240}

      impact-result (ppi/measure-distortion-impact clean-segment-example
                                                   distortion-params
                                                   rmssd-config)]

  (kind/hiccup
   [:div {:style {:background-color "#f8f9fa" :padding "15px" :margin "10px 0"}}
    [:h4 {:style {:margin-top "0"}} "Distortion Impact on RMSSD"]
    [:p {:style {:font-size "1.1em"}}
     "Realistic artifacts cause RMSSD values to be "
     [:strong {:style {:color "#d63384"}}
      (format "%.01f%% different" (* 100 (Math/abs (:mean-relative-error impact-result))))]
     " on average."]
    [:p {:style {:color "#6c757d"}}
     (format "Analysis based on %d time windows" (:n-valid-pairs impact-result))]]))

;; ## Our Approach: Testing Smoothing Algorithms
;;
;; We'll test several smoothing algorithms to see which ones can reduce this volatility
;; while preserving meaningful RMSSD trends. Our goal is to find algorithms that:
;;
;; 1. **Reduce noise** without over-smoothing
;; 2. **Preserve real trends** in heart rate variability  
;; 3. **Work in real-time** with acceptable computational cost
;; 4. **Handle various artifact types** robustly

;; ### Smoothing Algorithms We're Testing
;;
;; - **Moving Average**: Simple averaging over recent values
;; - **Median Filter**: Robust to outliers, preserves edges
;; - **Exponential Moving Average**: Responsive with memory
;; - **Cascaded Median**: Combined median filtering steps for outlier removal
;; - **Cascaded Smoothing**: Combines median filtering with moving average smoothing

;; Let's compare these across multiple distortion scenarios:

(let [;; Core algorithms to test
      algorithms {"No smoothing" {:colname :RMSSD
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

                  "Cascaded smoothing" {:colname :RMSSD-CascSmooth
                                        :windowed-fn #(ppi/cascaded-smoothing-filter % 5 3)
                                        :windowed-dataset-size 240}

                  "Exponential MA (α=0.2)" {:colname :RMSSD-EMA2
                                            :windowed-fn #(ppi/exponential-moving-average % 0.2)
                                            :windowed-dataset-size 240}}

      ;; Different types of artifacts
      scenarios {"Light noise" {:noise-std 8.0}
                 "Heavy noise" {:noise-std 20.0}
                 "Outliers" {:noise-std 5.0 :outlier-prob 0.08 :outlier-magnitude 3.0}
                 "Combined artifacts" {:noise-std 12.0
                                       :outlier-prob 0.05
                                       :outlier-magnitude 2.5
                                       :missing-prob 0.015
                                       :extra-prob 0.01}}

      ;; Test each algorithm against each scenario
      results (for [[algorithm-name algorithm-config] algorithms
                    [scenario-name scenario-params] scenarios]
                (try
                  (let [impact (ppi/measure-distortion-impact clean-segment-example
                                                              scenario-params
                                                              algorithm-config)]
                    {:algorithm algorithm-name
                     :scenario scenario-name
                     :error (* 100 (Math/abs (:mean-relative-error impact)))
                     :valid-pairs (:n-valid-pairs impact)})
                  (catch Exception e
                    {:algorithm algorithm-name
                     :scenario scenario-name
                     :error "Failed"
                     :valid-pairs 0})))]

  ;; Display results
  (kind/hiccup
   [:div
    [:h3 "Single-Segment Algorithm Performance"]
    [:p "Mean absolute error (%) for each algorithm-scenario combination:"]

    [:table {:style {:border-collapse "collapse" :width "100%" :margin "20px 0"}}
     [:thead
      [:tr {:style {:background-color "#f8f9fa"}}
       [:th {:style {:padding "12px" :border "1px solid #dee2e6" :text-align "left"}} "Algorithm"]
       (for [scenario-name (map first scenarios)]
         [:th {:style {:padding "12px" :border "1px solid #dee2e6" :text-align "center"}} scenario-name])]]

     [:tbody
      (for [algorithm-name (map first algorithms)]
        [:tr
         [:td {:style {:padding "10px" :border "1px solid #dee2e6" :font-weight "bold"}} algorithm-name]
         (for [scenario-name (map first scenarios)]
           (let [result (first (filter #(and (= (:algorithm %) algorithm-name)
                                             (= (:scenario %) scenario-name))
                                       results))
                 error (:error result)
                 cell-style (cond
                              (= error "Failed") {:padding "10px" :border "1px solid #dee2e6"
                                                  :text-align "center" :background-color "#ffe6e6"}
                              (< error 30) {:padding "10px" :border "1px solid #dee2e6"
                                            :text-align "center" :background-color "#d1edff"}
                              (< error 60) {:padding "10px" :border "1px solid #dee2e6"
                                            :text-align "center" :background-color "#b3d9ff"}
                              (< error 100) {:padding "10px" :border "1px solid #dee2e6"
                                             :text-align "center" :background-color "#ffecb3"}
                              :else {:padding "10px" :border "1px solid #dee2e6"
                                     :text-align "center" :background-color "#ffcdd2"})]
             [:td {:style cell-style}
              (if (number? error)
                (format "%.01f%%" error)
                error)]))])]]

    [:div {:style {:margin-top "15px" :font-size "0.9em" :color "#6c757d"}}
     [:p "Lower percentages are better. Colors: "
      [:span {:style {:background-color "#d1edff" :padding "2px 6px"}} "< 30%"] " excellent, "
      [:span {:style {:background-color "#b3d9ff" :padding "2px 6px"}} "30-60%"] " good, "
      [:span {:style {:background-color "#ffecb3" :padding "2px 6px"}} "60-100%"] " moderate, "
      [:span {:style {:background-color "#ffcdd2" :padding "2px 6px"}} "> 100%"] " poor."]]]))

;; Early results look promising! Most smoothing algorithms significantly reduce
;; error compared to no smoothing, with median filters and moving averages
;; performing particularly well.

;; ## Testing on Multiple Segments
;;
;; One segment might not tell the whole story. Let's test our algorithms
;; across multiple clean segments to get more robust results:

(def test-segments
  (->> (tc/group-by segmented-data [:Device-UUID :jump-count] {:result-type :as-seq})
       (filter #(ppi/clean-segment? % clean-params))
       (sort-by (fn [segment] (-> segment tc/rows first hash)))
       (take 8))) ; Use 8 segments for testing

(let [;; Focus on the most promising algorithms
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

                      "Cascaded smoothing" {:colname :RMSSD-CascSmooth
                                            :windowed-fn #(ppi/cascaded-smoothing-filter % 5 3)
                                            :windowed-dataset-size 240}

                      "Exponential MA (α=0.2)" {:colname :RMSSD-EMA2
                                                :windowed-fn #(ppi/exponential-moving-average % 0.2)
                                                :windowed-dataset-size 240}}

      ;; Representative distortion scenarios
      key-scenarios {"Light noise" {:noise-std 8.0}
                     "Heavy noise" {:noise-std 20.0}
                     "Outliers" {:noise-std 5.0 :outlier-prob 0.08 :outlier-magnitude 3.0}
                     "Combined artifacts" {:noise-std 12.0
                                           :outlier-prob 0.05
                                           :outlier-magnitude 2.5
                                           :missing-prob 0.015
                                           :extra-prob 0.01}}

      ;; Test each algorithm-scenario combination across all segments
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
                                                test-segments)

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
                       :total-segments (count test-segments)}))

      ;; Group results by algorithm
      results-by-algo (group-by :algorithm all-results)]

  ;; Display comprehensive results
  (kind/hiccup
   [:div
    [:h3 "Multi-Segment Performance Analysis"]
    [:p (format "Results averaged across %d clean segments:" (count test-segments))]

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
                              (< mean-error 40) {:padding "10px" :border "1px solid #dee2e6"
                                                 :text-align "center" :background-color "#d1edff"}
                              (< mean-error 70) {:padding "10px" :border "1px solid #dee2e6"
                                                 :text-align "center" :background-color "#b3d9ff"}
                              (< mean-error 120) {:padding "10px" :border "1px solid #dee2e6"
                                                  :text-align "center" :background-color "#ffecb3"}
                              :else {:padding "10px" :border "1px solid #dee2e6"
                                     :text-align "center" :background-color "#ffcdd2"})]
             [:td {:style cell-style}
              (if mean-error
                [:div
                 [:div {:style {:font-weight "bold"}} (format "%.01f%%" mean-error)]
                 [:div {:style {:font-size "0.8em" :color "#666"}}
                  (if std-error
                    (format "±%.01f" std-error)
                    (format "n=%d" n-segments))]]
                "Failed")]))])]]

    [:div {:style {:margin-top "15px" :font-size "0.9em" :color "#6c757d"}}
     [:p "Values show mean ± standard deviation across segments. Lower is better."]]]))

;; ## Visual Example: Before and After Smoothing
;;
;; Let's see what the actual RMSSD signals look like when computed from raw vs smoothed PPI data:

(let [;; Create distorted PPI data
      distorted-segment (-> clean-segment-example
                            (ppi/distort-segment {:noise-std 12.0
                                                  :outlier-prob 0.05
                                                  :outlier-magnitude 2.5
                                                  :missing-prob 0.015
                                                  :extra-prob 0.01}))

      ;; Calculate RMSSD from raw distorted PPI data
      raw-rmssd-data (-> distorted-segment
                         (ppi/add-column-by-windowed-fn {:colname :RMSSD
                                                         :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                                                         :windowed-dataset-size 240})
                         (tc/add-columns {:signal-type "Raw PPI → RMSSD"}))

      ;; Calculate RMSSD from smoothed PPI data (two-step process)
      smoothed-rmssd-data (-> distorted-segment
                              ;; Step 1: Smooth the PPI data
                              (ppi/add-column-by-windowed-fn {:colname :PpInMs
                                                              :windowed-fn #(ppi/cascaded-smoothing-filter % 5 3)
                                                              :windowed-dataset-size 240})
                              ;; Step 2: Compute RMSSD from smoothed PPI  
                              (ppi/add-column-by-windowed-fn {:colname :RMSSD
                                                              :windowed-fn #(ppi/windowed-dataset->rmssd % :timestamp 60000)
                                                              :windowed-dataset-size 240})
                              (tc/add-columns {:signal-type "Smoothed PPI → RMSSD"}))

      ;; Combine for comparison
      combined-data (-> (tc/concat raw-rmssd-data smoothed-rmssd-data)
                        (tc/select-rows #(not (nil? (:RMSSD %)))))]

  (-> combined-data
      (plotly/base {:=height 350
                    :=title "RMSSD Comparison: Raw vs Smoothed PPI Data"})
      (plotly/layer-line {:=x :timestamp
                          :=y :RMSSD
                          :=color :signal-type})))

;; The smoothed signal clearly shows the underlying trends while reducing
;; the moment-to-moment volatility that would confuse users.

;; ## Key Findings
;;
;; Based on our analysis across multiple segments and distortion scenarios:
;;
;; ### 1. Smoothing Significantly Helps
;; All smoothing algorithms reduce RMSSD volatility by 30-70% compared to raw calculations.
;;
;; ### 2. Median Filters Work Well with Outliers  
;; Median filters (5-point) consistently perform well across all artifact types, especially outliers.
;;
;; ### 3. Moving Averages Handle Noise Well
;; Simple 5-point moving averages work effectively for Gaussian noise with minimal computational cost.
;;
;; ### 4. Cascaded Approaches Are Robust
;; Cascaded filters (both median-only and median+smoothing) handle complex, mixed artifacts well.
;;
;; ### 5. Algorithm Choice Depends on Context
;; No single algorithm dominates - the best choice depends on expected artifact types and computational constraints.

;; ## Recommendations
;;
;; For real-time RMSSD smoothing:
;;
;; ### Use Cascaded Smoothing Filter
;; - Combines median filtering with moving average smoothing
;; - Handles all types of artifacts well
;; - Should give the best balance of noise reduction and trend preservation
;; - If performance becomes an issue, fall back to 5-point median filter
;;
;; ### Implementation Approach
;; - Start with cascaded smoothing and test performance
;; - Switch to simpler filters only if you hit performance limits
;; - Always validate with actual users

;; ## Next Steps
;;
;; This analysis provides a solid foundation for implementing RMSSD smoothing.
;; Recommended next steps include:
;;
;; 1. **More careful visual analysis** - Detailed examination of how different filters affect RMSSD time series patterns
;; 2. **Real-time testing** - Validate algorithms in live data streaming context
;; 3. **Advanced filtering methods** - Test Savitzky-Golay filters, Kalman filters, and other sophisticated approaches
;; 4. **Database integration** - Implement storage and retrieval of processed RMSSD data
;; 5. **More careful performance tests** - Systematic benchmarking of computational costs and memory usage
;; 6. **Adaptive algorithms** - Develop methods that adjust smoothing based on detected artifact levels
;;
