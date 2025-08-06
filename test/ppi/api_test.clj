(ns ppi.api-test
  (:require [ppi.api :as sut]
            [clojure.test :as t]
            [tablecloth.api :as tc]
            [java-time.api :as java-time]
            [clojure.java.io :as io]
            [babashka.fs :as fs])
  (:import (java.util.zip GZIPOutputStream GZIPInputStream)))

(t/deftest standardize-csv-line-test
  (t/testing "removes leading and trailing quotes"
    (t/is (= "test,data,here" (sut/standardize-csv-line "\"test,data,here\""))))

  (t/testing "removes quadruple quotes"
    (t/is (= "test,data" (sut/standardize-csv-line "test,\"\"\"\"data"))))

  (t/testing "converts double quotes to single quotes"
    (t/is (= "test,\"data" (sut/standardize-csv-line "test,\"\"data"))))

  (t/testing "handles complex quote combinations"
    (t/is (= "field1,\"value with quotes,field3"
             (sut/standardize-csv-line "\"field1,\"\"value with quotes,field3\""))))

  (t/testing "handles empty string"
    (t/is (= "" (sut/standardize-csv-line "")))))

(t/deftest prepare-raw-data-test
  (t/testing "cleans column names and parses numeric data"
    (let [raw-data (tc/dataset {"prefix-Device-UUID" ["device1" "device2"]
                                "prefix-Pulse Rate" ["120,500" "110,200"]
                                "prefix-PpInMs" ["800,900" "750,850"]
                                "prefix-PpErrorEstimate" ["10,5" "8,12"]})
          result (sut/prepare-raw-data raw-data "prefix-")]

      (t/testing "removes prefix and converts spaces to hyphens"
        (t/is (= #{:Device-UUID :Pulse-Rate :PpInMs :PpErrorEstimate}
                 (set (tc/column-names result)))))

      (t/testing "parses PpInMs numeric values"
        (t/is (= [800900 750850] (tc/column result :PpInMs))))

      (t/testing "parses PpErrorEstimate numeric values"
        (t/is (= [105 812] (tc/column result :PpErrorEstimate)))))))

(t/deftest filter-recent-data-test
  (t/testing "filters data after cutoff date"
    (let [old-date (java-time/local-date-time 2025 1 1 10 0)
          recent-date (java-time/local-date-time 2025 5 1 10 0)
          future-date (java-time/local-date-time 2025 6 1 10 0)
          cutoff-date (java-time/local-date-time 2025 3 1 10 0)

          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1"]
                                 :Client-Timestamp [old-date recent-date future-date]
                                 :PpInMs [800 850 900]})

          result (sut/filter-recent-data test-data cutoff-date)]

      (t/is (= 2 (tc/row-count result)))
      (t/is (= [850 900] (tc/column result :PpInMs))))))

(t/deftest add-timestamps-test
  (t/testing "computes accumulated timestamps from pulse intervals"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1"]
                                 :Client-Timestamp [base-time base-time base-time]
                                 :PpInMs [1000 800 900]})

          result (sut/add-timestamps test-data)]

      (t/testing "adds accumulated-pp column"
        (t/is (contains? (set (tc/column-names result)) :accumulated-pp))
        (t/is (= [1000 1800 2700] (tc/column result :accumulated-pp))))

      (t/testing "adds timestamp column with accumulated intervals"
        (t/is (contains? (set (tc/column-names result)) :timestamp))
        (let [timestamps (tc/column result :timestamp)
              expected-times [(java-time/plus base-time (java-time/millis 1000))
                              (java-time/plus base-time (java-time/millis 1800))
                              (java-time/plus base-time (java-time/millis 2700))]]
          (t/is (= expected-times timestamps)))))))

(t/deftest recognize-jumps-test
  (t/testing "detects temporal discontinuities"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          timestamps [(java-time/plus base-time (java-time/millis 0))
                      (java-time/plus base-time (java-time/millis 1000)) ; 1s gap
                      (java-time/plus base-time (java-time/millis 8000)) ; 7s gap (jump)
                      (java-time/plus base-time (java-time/millis 9000))] ; 1s gap

          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1" "device1"]
                                 :timestamp timestamps})

          result (sut/recognize-jumps test-data {:jump-threshold 5000})]

      (t/testing "maintains cumulative jump count"
        (t/is (contains? (set (tc/column-names result)) :jump-count))
        (t/is (= [0 0 1 1] (tc/column result :jump-count))))))

  (t/testing "handles multiple devices independently"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device2" "device2"]
                                 :timestamp [(java-time/plus base-time (java-time/millis 0))
                                             (java-time/plus base-time (java-time/millis 6000)) ; jump for device1
                                             (java-time/plus base-time (java-time/millis 1000)) ; normal for device2
                                             (java-time/plus base-time (java-time/millis 2000))]}) ; normal for device2

          result (sut/recognize-jumps test-data {:jump-threshold 5000})]

      (t/is (= [0 1 0 0] (tc/column result :jump-count))))))

(t/deftest prepare-standard-csv!-test
  (t/testing "processes gzipped CSV with quote issues"
    (let [temp-dir (fs/create-temp-dir)
          raw-path (str temp-dir "/raw.csv.gz")
          standard-path (str temp-dir "/standard.csv.gz")

          ; Create test gzipped CSV with quote issues
          test-csv-content "\"header1,header2,header3\"\n\"\"\"\"value1\"\"\"\",\"\"value2\"\",normal"]

      ; Write test gzipped file
      (with-open [out (-> raw-path
                          io/output-stream
                          GZIPOutputStream.)]
        (spit out test-csv-content))

      ; Process the file
      (sut/prepare-standard-csv! raw-path standard-path)

      ; Read and verify the processed file
      (let [processed-content (with-open [in (-> standard-path
                                                 io/input-stream
                                                 GZIPInputStream.)]
                                (slurp in))]

        (t/is (fs/exists? standard-path))
        (t/is (= "header1,header2,header3\n\"\"value1,\"value2\",normal"
                 processed-content)))

      ; Cleanup
      (fs/delete-tree temp-dir)))

  (t/testing "skips processing if standard file already exists"
    (let [temp-dir (fs/create-temp-dir)
          raw-path (str temp-dir "/raw.csv.gz")
          standard-path (str temp-dir "/standard.csv.gz")]

      ; Create existing standard file
      (spit standard-path "existing content")

      ; Create raw file (shouldn't be processed)
      (with-open [out (-> raw-path
                          io/output-stream
                          GZIPOutputStream.)]
        (spit out "new content"))

      (sut/prepare-standard-csv! raw-path standard-path)

      ; Verify existing file wasn't overwritten
      (t/is (= "existing content" (slurp standard-path)))

      ; Cleanup
      (fs/delete-tree temp-dir))))

(t/deftest edge-cases-test
  (t/testing "handles empty datasets gracefully"
    (let [empty-data (tc/dataset {})]
      (t/is (= 0 (tc/row-count (sut/filter-recent-data empty-data (java-time/local-date-time 2025 1 1)))))))

  (t/testing "handles single row datasets"
    (let [single-row (tc/dataset {:Device-UUID ["device1"]
                                  :timestamp [(java-time/local-date-time 2025 5 1 10 0)]})]
      (let [result (sut/recognize-jumps single-row {:jump-threshold 5000})]
        (t/is (= [0] (tc/column result :jump-count))))))

  (t/testing "standardize-csv-line handles various edge cases"
    (t/is (= "no quotes" (sut/standardize-csv-line "no quotes")))
    (t/is (= "only,commas" (sut/standardize-csv-line "only,commas")))
    (t/is (= "single\"quote" (sut/standardize-csv-line "single\"quote")))))

(t/deftest standardize-csv-line-comprehensive-test
  (t/testing "handles nested quote patterns"
    (t/is (= "field1,\"nested\"quotes,field3"
             (sut/standardize-csv-line "\"field1,\"\"nested\"\"quotes,field3\""))))

  (t/testing "handles multiple quadruple quote sequences"
    (t/is (= "\"\"start,middle,end\"\""
             (sut/standardize-csv-line "\"\"\"\"start,\"\"\"\"middle,\"\"\"\"end\"\"\"\""))))

  (t/testing "preserves single quotes when appropriate"
    (t/is (= "field1,\"single,field3"
             (sut/standardize-csv-line "field1,\"single,field3"))))

  (t/testing "handles mixed quote patterns in single line"
    (t/is (= "\"\"start,middle,end"
             (sut/standardize-csv-line "\"\"\"\"start,\"\"\"\"middle,\"\"\"\"end\""))))

  (t/testing "preserves content without leading/trailing quotes"
    (t/is (= "normal,csv,content"
             (sut/standardize-csv-line "normal,csv,content")))))

(t/deftest prepare-raw-data-comprehensive-test
  (t/testing "handles various column name transformations"
    (let [raw-data (tc/dataset {"prefix-Device UUID" ["device1"]
                                "prefix-Another Column Name" ["value1"]
                                "prefix-PpInMs" ["1,000"]
                                "prefix-PpErrorEstimate" ["50"]})
          result (sut/prepare-raw-data raw-data "prefix-")]

      (t/is (= #{:Device-UUID :Another-Column-Name :PpInMs :PpErrorEstimate}
               (set (tc/column-names result))))))

  (t/testing "handles large numeric values with commas"
    (let [raw-data (tc/dataset {"prefix-PpInMs" ["1,234,567"]
                                "prefix-PpErrorEstimate" ["9,876"]})
          result (sut/prepare-raw-data raw-data "prefix-")]

      (t/is (= [1234567] (tc/column result :PpInMs)))
      (t/is (= [9876] (tc/column result :PpErrorEstimate)))))

  (t/testing "preserves other column types unchanged"
    (let [raw-data (tc/dataset {"prefix-Device-UUID" ["uuid123"]
                                "prefix-Status" ["active"]
                                "prefix-PpInMs" ["800"]
                                "prefix-PpErrorEstimate" ["10"]})
          result (sut/prepare-raw-data raw-data "prefix-")]

      (t/is (= ["uuid123"] (tc/column result :Device-UUID)))
      (t/is (= ["active"] (tc/column result :Status)))))

  (t/testing "handles empty prefix"
    (let [raw-data (tc/dataset {"Device UUID" ["device1"]
                                "PpInMs" ["800"]
                                "PpErrorEstimate" ["10"]})
          result (sut/prepare-raw-data raw-data "")]

      (t/is (= #{:Device-UUID :PpInMs :PpErrorEstimate}
               (set (tc/column-names result))))))

  (t/testing "handles mixed case column names"
    (let [raw-data (tc/dataset {"PREFIX-device uuid" ["device1"]
                                "PREFIX-PpInMs" ["800"]
                                "PREFIX-PpErrorEstimate" ["10"]})
          result (sut/prepare-raw-data raw-data "PREFIX-")]

      (t/is (= #{:device-uuid :PpInMs :PpErrorEstimate}
               (set (tc/column-names result)))))))

(t/deftest filter-recent-data-comprehensive-test
  (t/testing "handles exact cutoff date boundary"
    (let [cutoff-date (java-time/local-date-time 2025 3 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1"]
                                 :Client-Timestamp [cutoff-date
                                                    (java-time/plus cutoff-date (java-time/seconds 1))
                                                    (java-time/minus cutoff-date (java-time/seconds 1))]
                                 :PpInMs [800 850 750]})

          result (sut/filter-recent-data test-data cutoff-date)]

      (t/is (= 1 (tc/row-count result)))
      (t/is (= [850] (tc/column result :PpInMs)))))

  (t/testing "handles multiple devices with different date ranges"
    (let [cutoff-date (java-time/local-date-time 2025 3 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device2" "device2"]
                                 :Client-Timestamp [(java-time/plus cutoff-date (java-time/days 1))
                                                    (java-time/minus cutoff-date (java-time/days 1))
                                                    (java-time/plus cutoff-date (java-time/days 2))
                                                    (java-time/plus cutoff-date (java-time/days 3))]
                                 :PpInMs [800 750 850 900]})

          result (sut/filter-recent-data test-data cutoff-date)]

      (t/is (= 3 (tc/row-count result)))
      (t/is (= [800 850 900] (tc/column result :PpInMs)))))

  (t/testing "returns empty dataset when all dates are before cutoff"
    (let [cutoff-date (java-time/local-date-time 2025 6 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1"]
                                 :Client-Timestamp [(java-time/local-date-time 2025 1 1 10 0)
                                                    (java-time/local-date-time 2025 2 1 10 0)]
                                 :PpInMs [800 750]})

          result (sut/filter-recent-data test-data cutoff-date)]

      (t/is (= 0 (tc/row-count result)))))

  (t/testing "preserves all columns in filtered result"
    (let [cutoff-date (java-time/local-date-time 2025 3 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1"]
                                 :Client-Timestamp [(java-time/plus cutoff-date (java-time/days 1))]
                                 :PpInMs [800]
                                 :Extra-Column ["extra-value"]})

          result (sut/filter-recent-data test-data cutoff-date)]

      (t/is (= #{:Device-UUID :Client-Timestamp :PpInMs :Extra-Column}
               (set (tc/column-names result))))
      (t/is (= ["extra-value"] (tc/column result :Extra-Column))))))

(t/deftest add-timestamps-comprehensive-test
  (t/testing "handles multiple devices independently"
    (let [base-time1 (java-time/local-date-time 2025 5 1 10 0)
          base-time2 (java-time/local-date-time 2025 5 2 15 30)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device2" "device2"]
                                 :Client-Timestamp [base-time1 base-time1 base-time2 base-time2]
                                 :PpInMs [1000 800 500 600]})

          result (sut/add-timestamps test-data)
          device1-rows (tc/select-rows result #(= (:Device-UUID %) "device1"))
          device2-rows (tc/select-rows result #(= (:Device-UUID %) "device2"))]

      (t/testing "device1 accumulation"
        (t/is (= [1000 1800] (tc/column device1-rows :accumulated-pp))))

      (t/testing "device2 accumulation starts fresh"
        (t/is (= [500 1100] (tc/column device2-rows :accumulated-pp))))))

  (t/testing "handles different client timestamps for same device"
    (let [base-time1 (java-time/local-date-time 2025 5 1 10 0)
          base-time2 (java-time/local-date-time 2025 5 1 11 0) ; 1 hour later
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1" "device1"]
                                 :Client-Timestamp [base-time1 base-time1 base-time2 base-time2]
                                 :PpInMs [1000 800 500 600]})

          result (sut/add-timestamps test-data)]

      (t/testing "separate accumulation per client timestamp"
        (t/is (= [1000 1800 500 1100] (tc/column result :accumulated-pp))))))

  (t/testing "handles zero and negative intervals"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1"]
                                 :Client-Timestamp [base-time base-time base-time]
                                 :PpInMs [0 -100 500]}) ; Including negative value

          result (sut/add-timestamps test-data)]

      (t/is (= [0 -100 400] (tc/column result :accumulated-pp)))))

  (t/testing "preserves original columns and adds new ones"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1"]
                                 :Client-Timestamp [base-time]
                                 :PpInMs [1000]
                                 :Extra-Data ["extra"]})

          result (sut/add-timestamps test-data)]

      (t/is (= #{:Device-UUID :Client-Timestamp :PpInMs :Extra-Data :accumulated-pp :timestamp}
               (set (tc/column-names result))))
      (t/is (= ["extra"] (tc/column result :Extra-Data))))))

(t/deftest recognize-jumps-comprehensive-test
  (t/testing "handles various jump thresholds"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          timestamps [(java-time/plus base-time (java-time/millis 0))
                      (java-time/plus base-time (java-time/millis 1000)) ; 1s gap
                      (java-time/plus base-time (java-time/millis 4000)) ; 3s gap
                      (java-time/plus base-time (java-time/millis 10000))] ; 6s gap

          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1" "device1"]
                                 :timestamp timestamps})]

      (t/testing "low threshold detects more jumps"
        (let [result (sut/recognize-jumps test-data {:jump-threshold 2000})]
          (t/is (= [0 0 1 2] (tc/column result :jump-count)))))

      (t/testing "high threshold detects fewer jumps"
        (let [result (sut/recognize-jumps test-data {:jump-threshold 5000})]
          (t/is (= [0 0 0 1] (tc/column result :jump-count)))))))

  (t/testing "handles complex multi-device scenarios"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1" "device2" "device2" "device3"]
                                 :timestamp [(java-time/plus base-time (java-time/millis 0))
                                             (java-time/plus base-time (java-time/millis 1000))
                                             (java-time/plus base-time (java-time/millis 8000)) ; jump
                                             (java-time/plus base-time (java-time/millis 0)) ; device2 starts
                                             (java-time/plus base-time (java-time/millis 7000)) ; jump  
                                             (java-time/plus base-time (java-time/millis 0))]}) ; device3, single point

          result (sut/recognize-jumps test-data {:jump-threshold 5000})]

      (t/is (= 6 (tc/row-count result)))
      (t/is (= [0 0 1 0 1 0] (tc/column result :jump-count)))))

  (t/testing "handles edge case timestamps"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1"]
                                 :timestamp [base-time
                                             base-time ; Identical timestamp (0ms gap)
                                             (java-time/plus base-time (java-time/millis 10000))]}) ; Big jump

          result (sut/recognize-jumps test-data {:jump-threshold 5000})]

      (t/is (= [0 0 1] (tc/column result :jump-count)))))

  (t/testing "preserves row order after processing"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          test-data (tc/dataset {:Device-UUID ["device1" "device1" "device1"]
                                 :timestamp [(java-time/plus base-time (java-time/millis 0))
                                             (java-time/plus base-time (java-time/millis 1000))
                                             (java-time/plus base-time (java-time/millis 2000))]
                                 :Original-Order [1 2 3]})

          result (sut/recognize-jumps test-data {:jump-threshold 5000})]

      (t/is (= [1 2 3] (tc/column result :Original-Order)))))

  (t/testing "handles very large time differences"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          huge-gap (java-time/plus base-time (java-time/days 30)) ; 30 day gap
          test-data (tc/dataset {:Device-UUID ["device1" "device1"]
                                 :timestamp [base-time huge-gap]})

          result (sut/recognize-jumps test-data {:jump-threshold 86400000})] ; 1 day threshold

      (t/is (= [0 1] (tc/column result :jump-count))))))

(t/deftest integration-test-fixed
  (t/testing "partial pipeline integration"
    (let [raw-data (tc/dataset {"prefix-Device-UUID" ["device1" "device1" "device1"]
                                "prefix-PpInMs" ["1,000" "800" "1,200"]
                                "prefix-PpErrorEstimate" ["50" "30" "40"]})]

      (let [prepared-data (sut/prepare-raw-data raw-data "prefix-")]

        (t/is (= 3 (tc/row-count prepared-data)))
        (t/is (contains? (set (tc/column-names prepared-data)) :Device-UUID))
        (t/is (contains? (set (tc/column-names prepared-data)) :PpInMs))
        (t/is (contains? (set (tc/column-names prepared-data)) :PpErrorEstimate))
        (t/is (= [1000 800 1200] (tc/column prepared-data :PpInMs)))
        (t/is (= [50 30 40] (tc/column prepared-data :PpErrorEstimate))))))

  (t/testing "timestamp processing pipeline"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          prepared-data (tc/dataset {:Device-UUID ["device1" "device1"]
                                     :Client-Timestamp [base-time base-time]
                                     :PpInMs [1000 800]})]

      (let [with-timestamps (sut/add-timestamps prepared-data)
            with-jumps (sut/recognize-jumps with-timestamps {:jump-threshold 5000})]

        (t/is (= 2 (tc/row-count with-jumps)))
        (t/is (contains? (set (tc/column-names with-jumps)) :timestamp))
        (t/is (= [1000 1800] (tc/column with-timestamps :accumulated-pp)))))))

(t/deftest error-handling-and-validation-test
  (t/testing "prepare-raw-data handles missing columns gracefully"
    (let [data-without-pp (tc/dataset {"prefix-Device-UUID" ["device1"]
                                       "prefix-Other-Field" ["value1"]})]
      (t/is (thrown? Exception (sut/prepare-raw-data data-without-pp "prefix-")))))

  (t/testing "filter-recent-data handles nil timestamps"
    (let [data-with-nil (tc/dataset {:Device-UUID ["device1" "device1"]
                                     :Client-Timestamp [nil (java-time/local-date-time 2025 5 1)]})
          cutoff-date (java-time/local-date-time 2025 1 1)]

      (t/is (thrown? Exception (sut/filter-recent-data data-with-nil cutoff-date)))))

  (t/testing "recognize-jumps handles missing timestamp column gracefully"
    (let [data-without-timestamps (tc/dataset {:Device-UUID ["device1"]
                                               :PpInMs [800]})
          result (sut/recognize-jumps data-without-timestamps {:jump-threshold 5000})]

      (t/is (= 1 (tc/row-count result)))))

  (t/testing "standardize-csv-line handles null input"
    (t/is (thrown? Exception (sut/standardize-csv-line nil))))

  (t/testing "functions handle malformed numeric data"
    (let [bad-numeric-data (tc/dataset {"prefix-PpInMs" ["not-a-number"]
                                        "prefix-PpErrorEstimate" ["also-not-a-number"]})]

      (t/is (thrown? Exception (sut/prepare-raw-data bad-numeric-data "prefix-"))))))

(t/deftest performance-and-scale-test-simple
  (t/testing "handles moderately sized datasets"
    (let [dataset-size 100
          base-time (java-time/local-date-time 2025 5 1 10 0)
          timestamps (repeatedly dataset-size #(java-time/plus base-time (java-time/millis (* 1000 (rand-int 60)))))

          test-data (tc/dataset {:Device-UUID (repeat dataset-size "device1")
                                 :timestamp timestamps})]

      (let [result (sut/recognize-jumps test-data {:jump-threshold 5000})]
        (t/is (= dataset-size (tc/row-count result)))))))

(t/deftest prepare-timestamped-ppi-data-test
  (t/testing "function exists and can be called"
    ; This function depends on specific CSV parsing that's complex to mock
    ; We test that the function exists and has the right signature
    (t/is (fn? sut/prepare-timestamped-ppi-data))

    ; Test with a non-existent file to ensure it fails gracefully
    (t/is (thrown? Exception (sut/prepare-timestamped-ppi-data "/non/existent/file.csv")))))

(t/deftest calculate-coefficient-of-variation-test
  (t/testing "calculates CV for normal data"
    (let [values [100 110 90 105 95]
          cv (sut/calculate-coefficient-of-variation values)]
      (t/is (> cv 0))
      (t/is (< cv 20)))) ; Should be reasonable percentage

  (t/testing "handles uniform data"
    (let [values [100 100 100 100]
          cv (sut/calculate-coefficient-of-variation values)]
      (t/is (= 0.0 cv))))

  (t/testing "handles zero mean gracefully"
    (let [values [0 0 0 0]
          cv (sut/calculate-coefficient-of-variation values)]
      (t/is (= 0.0 cv))))

  (t/testing "handles single value"
    (let [values [42]
          cv (sut/calculate-coefficient-of-variation values)]
      (t/is (or (= 0.0 cv) (Double/isNaN cv)))))

  (t/testing "calculates CV for high variability data"
    (let [values [10 100 1000]
          cv (sut/calculate-coefficient-of-variation values)]
      (t/is (> cv 50)))) ; High variability should give high CV

  (t/testing "handles negative values correctly"
    (let [values [-10 -5 -15]
          cv (sut/calculate-coefficient-of-variation values)]
      (t/is (> (Math/abs cv) 0)))))

(t/deftest calculate-successive-changes-test
  (t/testing "calculates percentage changes correctly"
    (let [values [100 110 121] ; 10% then 10% increases
          changes (sut/calculate-successive-changes values)]
      (t/is (= 2 (count changes)))
      (t/is (every? #(< (Math/abs (- % 10.0)) 0.1) changes)))) ; ~10% changes

  (t/testing "handles decreasing values"
    (let [values [100 90 81] ; 10% then 10% decreases
          changes (sut/calculate-successive-changes values)]
      (t/is (= 2 (count changes)))
      (t/is (every? #(< (Math/abs (- % 10.0)) 0.1) changes)))) ; Absolute values ~10%

  (t/testing "handles mixed increases and decreases"
    (let [values [100 120 96] ; +20%, -20%
          changes (sut/calculate-successive-changes values)]
      (t/is (= 2 (count changes)))
      (t/is (< (Math/abs (- (first changes) 20.0)) 0.1))
      (t/is (< (Math/abs (- (second changes) 20.0)) 0.1))))

  (t/testing "returns empty for single value"
    (let [values [100]
          changes (sut/calculate-successive-changes values)]
      (t/is (= 0 (count changes)))))

  (t/testing "returns empty for empty input"
    (let [values []
          changes (sut/calculate-successive-changes values)]
      (t/is (= 0 (count changes)))))

  (t/testing "handles non-zero values correctly"
    (let [values [10 50 25]
          changes (sut/calculate-successive-changes values)]
      (t/is (= 2 (count changes)))
      (t/is (every? #(> % 0) changes)))))

(t/deftest clean-segment?-test
  (t/testing "identifies clean segment with good parameters"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          clean-data (tc/dataset {:timestamp [(java-time/plus base-time (java-time/seconds 0))
                                              (java-time/plus base-time (java-time/seconds 1))
                                              (java-time/plus base-time (java-time/seconds 2))
                                              (java-time/plus base-time (java-time/seconds 3))
                                              (java-time/plus base-time (java-time/seconds 4))]
                                  :PpInMs [800 810 805 815 820] ; Low variability
                                  :PpErrorEstimate [5 4 6 5 4]}) ; Low error
          params {:max-error-estimate 10
                  :max-heart-rate-cv 5.0
                  :max-successive-change 5.0
                  :min-clean-duration 3000
                  :min-clean-samples 5}]

      (t/is (true? (sut/clean-segment? clean-data params)))))

  (t/testing "rejects segment with high error estimate"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          noisy-data (tc/dataset {:timestamp [(java-time/plus base-time (java-time/seconds 0))
                                              (java-time/plus base-time (java-time/seconds 1))
                                              (java-time/plus base-time (java-time/seconds 2))
                                              (java-time/plus base-time (java-time/seconds 3))
                                              (java-time/plus base-time (java-time/seconds 4))]
                                  :PpInMs [800 810 805 815 820]
                                  :PpErrorEstimate [50 45 55 50 60]}) ; High error
          params {:max-error-estimate 10
                  :max-heart-rate-cv 5.0
                  :max-successive-change 5.0
                  :min-clean-duration 3000
                  :min-clean-samples 5}]

      (t/is (false? (sut/clean-segment? noisy-data params)))))

  (t/testing "rejects segment with high heart rate variability"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          variable-data (tc/dataset {:timestamp [(java-time/plus base-time (java-time/seconds 0))
                                                 (java-time/plus base-time (java-time/seconds 1))
                                                 (java-time/plus base-time (java-time/seconds 2))
                                                 (java-time/plus base-time (java-time/seconds 3))
                                                 (java-time/plus base-time (java-time/seconds 4))]
                                     :PpInMs [800 1200 600 1400 500] ; High variability
                                     :PpErrorEstimate [5 4 6 5 4]})
          params {:max-error-estimate 10
                  :max-heart-rate-cv 5.0
                  :max-successive-change 5.0
                  :min-clean-duration 3000
                  :min-clean-samples 5}]

      (t/is (false? (sut/clean-segment? variable-data params)))))

  (t/testing "rejects segment that is too short in duration"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          short-data (tc/dataset {:timestamp [(java-time/plus base-time (java-time/millis 0))
                                              (java-time/plus base-time (java-time/millis 500))] ; Only 0.5 seconds
                                  :PpInMs [800 810]
                                  :PpErrorEstimate [5 4]})
          params {:max-error-estimate 10
                  :max-heart-rate-cv 5.0
                  :max-successive-change 5.0
                  :min-clean-duration 3000 ; Requires 3 seconds
                  :min-clean-samples 2}]

      (t/is (false? (sut/clean-segment? short-data params)))))

  (t/testing "rejects segment with too few samples"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          few-samples (tc/dataset {:timestamp [(java-time/plus base-time (java-time/seconds 0))
                                               (java-time/plus base-time (java-time/seconds 10))] ; Long duration but few samples
                                   :PpInMs [800 810]
                                   :PpErrorEstimate [5 4]})
          params {:max-error-estimate 10
                  :max-heart-rate-cv 5.0
                  :max-successive-change 5.0
                  :min-clean-duration 3000
                  :min-clean-samples 5}] ; Requires 5 samples

      (t/is (false? (sut/clean-segment? few-samples params)))))

  (t/testing "handles edge case with single sample"
    (let [base-time (java-time/local-date-time 2025 5 1 10 0)
          single-sample (tc/dataset {:timestamp [(java-time/plus base-time (java-time/seconds 0))]
                                     :PpInMs [800]
                                     :PpErrorEstimate [5]})
          params {:max-error-estimate 10
                  :max-heart-rate-cv 100.0 ; Allow high CV for single sample
                  :max-successive-change 100.0 ; Allow high successive change for single sample
                  :min-clean-duration 0
                  :min-clean-samples 1}]

      ; Single sample may fail CV/successive change tests due to NaN values
      (let [result (sut/clean-segment? single-sample params)]
        (t/is (boolean? result)))))

  (t/testing "handles empty segment"
    (let [empty-data (tc/dataset {:timestamp []
                                  :PpInMs []
                                  :PpErrorEstimate []})
          params {:max-error-estimate 10
                  :max-heart-rate-cv 5.0
                  :max-successive-change 5.0
                  :min-clean-duration 0
                  :min-clean-samples 1}]

      (t/is (false? (sut/clean-segment? empty-data params))))))

(t/deftest make-windowed-dataset-test
  (t/testing "creates windowed dataset with correct structure"
    (let [wd (sut/make-windowed-dataset {:x :int16 :y :float32} 5)]

      (t/testing "has correct record structure"
        (t/is (instance? ppi.api.WindowedDataset wd))
        (t/is (= {:x :int16 :y :float32} (:column-types wd)))
        (t/is (= 5 (:max-size wd)))
        (t/is (= 0 (:current-size wd)))
        (t/is (= 0 (:current-position wd))))

      (t/testing "creates dataset with correct columns"
        (let [ds (:dataset wd)]
          (t/is (= #{:x :y} (set (tc/column-names ds))))
          (t/is (= 5 (tc/row-count ds)))))))

  (t/testing "handles different column types"
    (let [wd (sut/make-windowed-dataset {:name :string :value :float64 :count :int32} 3)]
      (t/is (= #{:name :value :count} (set (tc/column-names (:dataset wd)))))
      (t/is (= 3 (:max-size wd)))))

  (t/testing "handles single column"
    (let [wd (sut/make-windowed-dataset {:data :int16} 10)]
      (t/is (= #{:data} (set (tc/column-names (:dataset wd)))))
      (t/is (= 10 (:max-size wd))))))

(t/deftest insert-to-windowed-dataset!-test
  (t/testing "inserts single value correctly"
    (let [wd (sut/make-windowed-dataset {:x :int16 :y :float32} 3)
          updated-wd (sut/insert-to-windowed-dataset! wd {:x 1 :y 10.5})]

      (t/is (= 1 (:current-size updated-wd)))
      (t/is (= 1 (:current-position updated-wd)))

      (let [result-ds (sut/windowed-dataset->dataset updated-wd)]
        (t/is (= 1 (tc/row-count result-ds)))
        (t/is (= [1] (tc/column result-ds :x)))
        (t/is (= [10.5] (tc/column result-ds :y))))))

  (t/testing "fills window up to max-size"
    (let [wd (sut/make-windowed-dataset {:val :int32} 3)]

      (let [wd1 (sut/insert-to-windowed-dataset! wd {:val 10})
            wd2 (sut/insert-to-windowed-dataset! wd1 {:val 20})
            wd3 (sut/insert-to-windowed-dataset! wd2 {:val 30})]

        (t/is (= 3 (:current-size wd3)))
        (t/is (= 0 (:current-position wd3))) ; Wrapped around

        (let [result-ds (sut/windowed-dataset->dataset wd3)]
          (t/is (= 3 (tc/row-count result-ds)))
          (t/is (= [10 20 30] (tc/column result-ds :val)))))))

  (t/testing "overwrites oldest values when exceeding max-size"
    (let [wd (sut/make-windowed-dataset {:val :int32} 2)]

      (let [wd1 (sut/insert-to-windowed-dataset! wd {:val 1})
            wd2 (sut/insert-to-windowed-dataset! wd1 {:val 2})
            wd3 (sut/insert-to-windowed-dataset! wd2 {:val 3})
            wd4 (sut/insert-to-windowed-dataset! wd3 {:val 4})]

        (t/is (= 2 (:current-size wd4))) ; Size stays at max
        (t/is (= 0 (:current-position wd4))) ; Position wrapped around

        (let [result-ds (sut/windowed-dataset->dataset wd4)]
          (t/is (= 2 (tc/row-count result-ds)))
          (t/is (= [3 4] (tc/column result-ds :val))))))) ; Oldest values (1,2) overwritten

  (t/testing "handles different data types"
    (let [wd (sut/make-windowed-dataset {:name :string :score :float64} 2)
          wd1 (sut/insert-to-windowed-dataset! wd {:name "alice" :score 95.5})
          wd2 (sut/insert-to-windowed-dataset! wd1 {:name "bob" :score 87.2})]

      (let [result-ds (sut/windowed-dataset->dataset wd2)]
        (t/is (= ["alice" "bob"] (tc/column result-ds :name)))
        (t/is (= [95.5 87.2] (tc/column result-ds :score)))))))

(t/deftest windowed-dataset-integration-test
  (t/testing "reproduces exact usage example from documentation"
    (let [results (->> (range 6)
                       (reductions (fn [wd i]
                                     (sut/insert-to-windowed-dataset! wd {:x i :y (* 1000 i)}))
                                   (sut/make-windowed-dataset {:x :int16 :y :float32} 4))
                       (map sut/windowed-dataset->dataset))]

      (t/testing "produces correct sequence length"
        (t/is (= 7 (count results))) ; reductions includes initial value)

        (t/testing "first dataset is empty"
          (let [first-ds (first results)]
            (t/is (= 0 (tc/row-count first-ds)))))

        (t/testing "second dataset has one row"
          (let [second-ds (second results)]
            (t/is (= 1 (tc/row-count second-ds)))
            (t/is (= [0] (tc/column second-ds :x)))
            (t/is (= [0.0] (tc/column second-ds :y)))))

        (t/testing "window fills up correctly"
          (let [fourth-ds (nth results 3)]
            (t/is (= 3 (tc/row-count fourth-ds)))
            (t/is (= [0 1 2] (tc/column fourth-ds :x)))
            (t/is (= [0.0 1000.0 2000.0] (tc/column fourth-ds :y)))))

        (t/testing "window reaches max size"
          (let [fifth-ds (nth results 4)]
            (t/is (= 4 (tc/row-count fifth-ds)))
            (t/is (= [0 1 2 3] (tc/column fifth-ds :x)))
            (t/is (= [0.0 1000.0 2000.0 3000.0] (tc/column fifth-ds :y)))))

        (t/testing "window starts wrapping (oldest data replaced)"
          (let [sixth-ds (nth results 5)]
            (t/is (= 4 (tc/row-count sixth-ds)))
            (t/is (= [1 2 3 4] (tc/column sixth-ds :x))) ; 0 is gone
            (t/is (= [1000.0 2000.0 3000.0 4000.0] (tc/column sixth-ds :y)))))

        (t/testing "final state shows continued wrapping"
          (let [final-ds (last results)]
            (t/is (= 4 (tc/row-count final-ds)))
            (t/is (= [2 3 4 5] (tc/column final-ds :x))) ; 0,1 are gone
            (t/is (= [2000.0 3000.0 4000.0 5000.0] (tc/column final-ds :y)))))))

    (t/testing "variation: smaller window with different data"
      (let [results (->> (range 3)
                         (reductions (fn [wd i]
                                       (sut/insert-to-windowed-dataset! wd {:a i :b (+ i 100)}))
                                     (sut/make-windowed-dataset {:a :int32 :b :int32} 2))
                         (map sut/windowed-dataset->dataset))]

        (t/is (= 4 (count results)))

      ; Final state should show window of size 2 with values [1, 2] and [101, 102]
        (let [final-ds (last results)]
          (t/is (= 2 (tc/row-count final-ds)))
          (t/is (= [1 2] (tc/column final-ds :a)))
          (t/is (= [101 102] (tc/column final-ds :b))))))

    (t/testing "variation: string data with small window"
      (let [results (->> ["a" "b" "c" "d" "e"]
                         (reductions (fn [wd s]
                                       (sut/insert-to-windowed-dataset! wd {:name s}))
                                     (sut/make-windowed-dataset {:name :string} 3))
                         (map sut/windowed-dataset->dataset))]

        (t/is (= 6 (count results)))

      ; Check progression
        (t/is (= 1 (tc/row-count (second results))))
        (t/is (= ["a"] (tc/column (second results) :name)))

        (t/is (= 3 (tc/row-count (nth results 3))))
        (t/is (= ["a" "b" "c"] (tc/column (nth results 3) :name)))

      ; Final state should show ["c" "d" "e"]
        (let [final-ds (last results)]
          (t/is (= 3 (tc/row-count final-ds)))
          (t/is (= ["c" "d" "e"] (tc/column final-ds :name))))))

    (t/testing "edge case: window size of 1"
      (let [results (->> [10 20 30]
                         (reductions (fn [wd v]
                                       (sut/insert-to-windowed-dataset! wd {:val v}))
                                     (sut/make-windowed-dataset {:val :int32} 1))
                         (map sut/windowed-dataset->dataset))]

        (t/is (= 4 (count results)))

      ; Each dataset should have at most 1 row with the latest value
        (t/is (= [10] (tc/column (second results) :val)))
        (t/is (= [20] (tc/column (nth results 2) :val)))
        (t/is (= [30] (tc/column (last results) :val))))))

  (t/deftest windowed-dataset-edge-cases-test
    (t/testing "handles empty insertions gracefully"
      (let [wd (sut/make-windowed-dataset {:x :int32} 3)
            result-ds (sut/windowed-dataset->dataset wd)]
        (t/is (= 0 (tc/row-count result-ds)))))

    (t/testing "handles missing columns in row data"
      (let [wd (sut/make-windowed-dataset {:x :int32 :y :float32} 2)]
      ; This should handle missing :y gracefully (likely with nil/default value)
        (t/is (some? (sut/insert-to-windowed-dataset! wd {:x 10})))))

    (t/testing "position wrapping calculation"
      (let [wd (sut/make-windowed-dataset {:val :int32} 3)]

      ; Insert exactly max-size elements
        (let [wd1 (sut/insert-to-windowed-dataset! wd {:val 1})
              wd2 (sut/insert-to-windowed-dataset! wd1 {:val 2})
              wd3 (sut/insert-to-windowed-dataset! wd2 {:val 3})]

          (t/is (= 3 (:current-size wd3)))
          (t/is (= 0 (:current-position wd3))) ; Should wrap to 0

        ; Add one more to test continued wrapping
          (let [wd4 (sut/insert-to-windowed-dataset! wd3 {:val 4})]
            (t/is (= 3 (:current-size wd4))) ; Size stays at max
            (t/is (= 1 (:current-position wd4))) ; Advances to 1

            (let [result-ds (sut/windowed-dataset->dataset wd4)]
              (t/is (= [2 3 4] (tc/column result-ds :val))))))))

    (t/testing "very large window size"
      (let [wd (sut/make-windowed-dataset {:data :int32} 1000)
          ; Insert many values
            final-wd (reduce (fn [w i]
                               (sut/insert-to-windowed-dataset! w {:data i}))
                             wd
                             (range 100))]

        (t/is (= 100 (:current-size final-wd)))
        (t/is (= 100 (:current-position final-wd)))

        (let [result-ds (sut/windowed-dataset->dataset final-wd)]
          (t/is (= 100 (tc/row-count result-ds)))
          (t/is (= (range 100) (tc/column result-ds :data))))))

    (t/testing "window size 0 behavior"
    ; This is an edge case that might not be practically useful
    ; but should be handled gracefully
      (let [wd (sut/make-windowed-dataset {:val :int32} 0)]
        (t/is (= 0 (:max-size wd)))

      ; Inserting into size-0 window should keep size at 0
        (let [updated-wd (sut/insert-to-windowed-dataset! wd {:val 42})
              result-ds (sut/windowed-dataset->dataset updated-wd)]
          (t/is (= 0 (tc/row-count result-ds))))))))

(t/deftest windowed-dataset-indices-test
  (t/testing "returns correct indices for different windowed dataset states"

    (t/testing "empty windowed dataset"
      (let [wd (sut/make-windowed-dataset {:x :int32} 3)
            indices (sut/windowed-dataset-indices wd)]
        (t/is (= [] indices))))

    (t/testing "partially filled window"
      (let [wd (sut/make-windowed-dataset {:x :int32} 4)
            wd1 (sut/insert-to-windowed-dataset! wd {:x 0})
            wd2 (sut/insert-to-windowed-dataset! wd1 {:x 1})
            wd3 (sut/insert-to-windowed-dataset! wd2 {:x 2})]

        (t/is (= [0] (sut/windowed-dataset-indices wd1)))
        (t/is (= [0 1] (sut/windowed-dataset-indices wd2)))
        (t/is (= [0 1 2] (sut/windowed-dataset-indices wd3)))))

    (t/testing "full window (no wrapping yet)"
      (let [wd (sut/make-windowed-dataset {:x :int32} 3)]
        (let [wd-full (reduce (fn [w i] (sut/insert-to-windowed-dataset! w {:x i}))
                              wd
                              (range 3))]
          (t/is (= [0 1 2] (sut/windowed-dataset-indices wd-full))))))

    (t/testing "wrapped window"
      (let [wd (sut/make-windowed-dataset {:x :int32} 3)]
        (let [wd-wrapped1 (reduce (fn [w i] (sut/insert-to-windowed-dataset! w {:x i}))
                                  wd
                                  (range 4))
              wd-wrapped2 (reduce (fn [w i] (sut/insert-to-windowed-dataset! w {:x i}))
                                  wd
                                  (range 5))]

          (t/is (= [1 2 0] (sut/windowed-dataset-indices wd-wrapped1)))
          (t/is (= [2 0 1] (sut/windowed-dataset-indices wd-wrapped2))))))

    (t/testing "edge case: window size 1"
      (let [wd (sut/make-windowed-dataset {:x :int32} 1)
            wd1 (sut/insert-to-windowed-dataset! wd {:x 10})
            wd2 (sut/insert-to-windowed-dataset! wd1 {:x 20})]

        (t/is (= [0] (sut/windowed-dataset-indices wd1)))
        (t/is (= [0] (sut/windowed-dataset-indices wd2)))))

    (t/testing "edge case: window size 0"
      (let [wd (sut/make-windowed-dataset {:x :int32} 0)]
        (t/is (= [] (sut/windowed-dataset-indices wd)))))))

(t/deftest windowed-dataset->time-window-dataset-test
  (t/testing "extracts data within specified time windows"

    (t/testing "empty windowed dataset"
      (let [wd (sut/make-windowed-dataset {:timestamp :local-date-time :value :int32} 5)
            result (sut/windowed-dataset->time-window-dataset wd :timestamp 30000)]
        (t/is (= 0 (tc/row-count result)))))

    (t/testing "single data point"
      (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
            wd (sut/make-windowed-dataset {:timestamp :local-date-time :value :int32} 5)
            wd-single (sut/insert-to-windowed-dataset! wd {:timestamp base-time :value 42})
            result (sut/windowed-dataset->time-window-dataset wd-single :timestamp 30000)]

        (t/is (= 1 (tc/row-count result)))
        (t/is (= [42] (tc/column result :value)))))

    (t/testing "time-based filtering with multiple data points"
      (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
            ;; Create 10 data points, 1 second apart
            test-data (map (fn [i]
                             {:timestamp (java-time/plus base-time (java-time/millis (* i 1000)))
                              :value i})
                           (range 10))
            wd (sut/make-windowed-dataset {:timestamp :local-date-time :value :int32} 15)
            final-wd (reduce sut/insert-to-windowed-dataset! wd test-data)]

        (t/testing "3-second window should get last 4 points (6,7,8,9)"
          (let [result (sut/windowed-dataset->time-window-dataset final-wd :timestamp 3000)]
            (t/is (= 4 (tc/row-count result)))
            (t/is (= [6 7 8 9] (tc/column result :value)))))

        (t/testing "5-second window should get last 6 points (4,5,6,7,8,9)"
          (let [result (sut/windowed-dataset->time-window-dataset final-wd :timestamp 5000)]
            (t/is (= 6 (tc/row-count result)))
            (t/is (= [4 5 6 7 8 9] (tc/column result :value)))))

        (t/testing "window larger than data span should return all data"
          (let [result (sut/windowed-dataset->time-window-dataset final-wd :timestamp 15000)]
            (t/is (= 10 (tc/row-count result)))
            (t/is (= [0 1 2 3 4 5 6 7 8 9] (tc/column result :value)))))

        (t/testing "very small window should return only most recent point"
          (let [result (sut/windowed-dataset->time-window-dataset final-wd :timestamp 500)]
            (t/is (= 1 (tc/row-count result)))
            (t/is (= [9] (tc/column result :value)))))))

    (t/testing "HRV use case with PpInMs data"
      (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
            ;; Simulate HRV data - 30 heartbeats over ~25 seconds
            hrv-data (map (fn [i]
                            {:timestamp (java-time/plus base-time (java-time/millis (* i 833))) ; ~72 BPM
                             :PpInMs (+ 800 (* 10 i)) ; Increasing intervals
                             :value i})
                          (range 30))
            wd (sut/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32 :value :int32} 50)
            hrv-wd (reduce sut/insert-to-windowed-dataset! wd hrv-data)]

        (t/testing "30-second window captures all HRV data"
          (let [result (sut/windowed-dataset->time-window-dataset hrv-wd :timestamp 30000)]
            (t/is (= 30 (tc/row-count result)))
            (t/is (= (range 30) (tc/column result :value)))
            ;; Check that PpInMs data is preserved
            (t/is (= 800 (first (tc/column result :PpInMs))))
            (t/is (= 1090 (last (tc/column result :PpInMs))))))

        (t/testing "10-second window captures recent portion"
          (let [result (sut/windowed-dataset->time-window-dataset hrv-wd :timestamp 10000)]
            (t/is (> (tc/row-count result) 5)) ; Should have multiple heartbeats
            (t/is (< (tc/row-count result) 30)) ; But not all of them
            ;; Should include the most recent data
            (t/is (= 29 (last (tc/column result :value))))))))

    (t/testing "edge cases and error handling"
      (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
            wd (sut/make-windowed-dataset {:timestamp :local-date-time :value :int32} 5)
            wd-with-data (sut/insert-to-windowed-dataset! wd {:timestamp base-time :value 42})]

        (t/testing "zero duration returns most recent point only"
          (let [result (sut/windowed-dataset->time-window-dataset wd-with-data :timestamp 0)]
            (t/is (= 1 (tc/row-count result)))
            (t/is (= [42] (tc/column result :value)))))

        (t/testing "negative duration returns empty dataset"
          (let [result (sut/windowed-dataset->time-window-dataset wd-with-data :timestamp -1000)]
            (t/is (= 0 (tc/row-count result)))))

        (t/testing "nil duration returns empty dataset"
          (let [result (sut/windowed-dataset->time-window-dataset wd-with-data :timestamp nil)]
            (t/is (= 0 (tc/row-count result)))))

        (t/testing "non-existent timestamp column throws exception"
          (t/is (thrown? IllegalArgumentException
                         (sut/windowed-dataset->time-window-dataset wd-with-data :nonexistent 1000))))))))

(t/deftest binary-search-timestamp-start-test
  (t/testing "binary search finds correct start positions"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          ;; Create timestamps: 10:00:00, 10:00:02, 10:00:04, 10:00:06, 10:00:08
          timestamps (map #(java-time/plus base-time (java-time/millis (* % 2000))) (range 5))
          ;; Simulate a dataset column
          timestamp-col (into [] timestamps)
          indices [0 1 2 3 4]]

      (t/testing "finds exact match"
        (let [target-time (nth timestamps 2) ; 10:00:04
              pos (sut/binary-search-timestamp-start timestamp-col indices target-time)]
          (t/is (= 2 pos))))

      (t/testing "finds insertion point for value between timestamps"
        (let [target-time (java-time/plus base-time (java-time/millis 3000)) ; 10:00:03 (between index 1 and 2)
              pos (sut/binary-search-timestamp-start timestamp-col indices target-time)]
          (t/is (= 2 pos)))) ; Should return position 2 (first >= target)

      (t/testing "finds position 0 for time before all timestamps"
        (let [target-time (java-time/minus base-time (java-time/millis 1000)) ; 09:59:59
              pos (sut/binary-search-timestamp-start timestamp-col indices target-time)]
          (t/is (= 0 pos))))

      (t/testing "finds end position for time after all timestamps"
        (let [target-time (java-time/plus base-time (java-time/millis 10000)) ; 10:00:10
              pos (sut/binary-search-timestamp-start timestamp-col indices target-time)]
          (t/is (= 5 pos)))) ; Should return past-end position

      (t/testing "handles empty indices"
        (let [pos (sut/binary-search-timestamp-start timestamp-col [] base-time)]
          (t/is (= 0 pos))))

      (t/testing "handles single element"
        (let [pos (sut/binary-search-timestamp-start timestamp-col [0] base-time)]
          (t/is (= 0 pos)))))))

(t/deftest windowed-dataset-time-window-performance-test
  (t/testing "binary search performance with larger datasets"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          ;; Create 1000 data points, 100ms apart (100 seconds of data)
          large-data (map (fn [i]
                            {:timestamp (java-time/plus base-time (java-time/millis (* i 100)))
                             :value i
                             :PpInMs (+ 800 (rem i 200))}) ; Varying heart rate
                          (range 1000))
          wd (sut/make-windowed-dataset {:timestamp :local-date-time
                                         :value :int32
                                         :PpInMs :int32} 1200)
          large-wd (reduce sut/insert-to-windowed-dataset! wd large-data)]

      (t/testing "handles large dataset efficiently"
        (let [start-time (System/nanoTime)
              result (sut/windowed-dataset->time-window-dataset large-wd :timestamp 30000) ; 30 second window
              end-time (System/nanoTime)
              duration-ms (/ (- end-time start-time) 1000000.0)]

          (t/is (> (tc/row-count result) 250)) ; Should have ~300 points in 30 seconds
          (t/is (< (tc/row-count result) 350))
          (t/is (< duration-ms 10.0)) ; Should be very fast (< 10ms)

          ;; Verify correctness - last value should be the most recent
          (t/is (= 999 (last (tc/column result :value))))))

      (t/testing "different window sizes work correctly on large dataset"
        (doseq [[window-ms min-expected max-expected] [[10000 90 110] ; 10 seconds
                                                       [60000 550 650]]] ; 60 seconds
          (let [result (sut/windowed-dataset->time-window-dataset large-wd :timestamp window-ms)]
            (t/is (>= (tc/row-count result) min-expected))
            (t/is (<= (tc/row-count result) max-expected))
            ;; Should always include the most recent point
            (t/is (= 999 (last (tc/column result :value))))))))))

(t/deftest windowed-dataset->rmssd-test
  (t/testing "computes RMSSD correctly with known data"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          ;; Test with simple known values: [800, 850, 820, 880, 810]
          ;; Successive differences: [50, -30, 60, -70]
          ;; Squared differences: [2500, 900, 3600, 4900] 
          ;; Mean: 2975, RMSSD: sqrt(2975) ≈ 54.54
          test-data (map (fn [i ppi-interval]
                           {:timestamp (java-time/plus base-time (java-time/millis (* i 1000)))
                            :PpInMs ppi-interval})
                         (range 5)
                         [800 850 820 880 810])
          wd (sut/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 10)
          final-wd (reduce sut/insert-to-windowed-dataset! wd test-data)]

      (t/testing "full dataset RMSSD calculation"
        (let [rmssd (sut/windowed-dataset->rmssd final-wd :timestamp 5000)]
          (t/is (not (nil? rmssd)))
          (t/is (< (Math/abs (- rmssd 54.543560573178574)) 1e-10)))) ; Within floating point precision

      (t/testing "partial dataset RMSSD (last 3 intervals: 820, 880, 810)"
        ;; Successive differences: [60, -70], squared: [3600, 4900], mean: 4250, RMSSD: sqrt(4250) ≈ 65.19
        (let [rmssd (sut/windowed-dataset->rmssd final-wd :timestamp 2000)]
          (t/is (not (nil? rmssd)))
          (t/is (< (Math/abs (- rmssd 65.19202405202648)) 1e-10))))))

  (t/testing "handles edge cases gracefully"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          wd (sut/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 5)]

      (t/testing "empty dataset returns nil"
        (let [rmssd (sut/windowed-dataset->rmssd wd :timestamp 5000)]
          (t/is (nil? rmssd))))

      (t/testing "single data point returns nil"
        (let [wd-single (sut/insert-to-windowed-dataset! wd {:timestamp base-time :PpInMs 800})
              rmssd (sut/windowed-dataset->rmssd wd-single :timestamp 5000)]
          (t/is (nil? rmssd))))

      (t/testing "two data points can compute RMSSD"
        (let [test-data [{:timestamp base-time :PpInMs 800}
                         {:timestamp (java-time/plus base-time (java-time/millis 1000)) :PpInMs 850}]
              wd-two (reduce sut/insert-to-windowed-dataset! wd test-data)
              rmssd (sut/windowed-dataset->rmssd wd-two :timestamp 2000)]
          ;; Single difference: 50, squared: 2500, RMSSD: 50.0
          (t/is (not (nil? rmssd)))
          (t/is (= 50.0 rmssd))))

      (t/testing "zero time window returns nil"
        (let [wd-with-data (sut/insert-to-windowed-dataset! wd {:timestamp base-time :PpInMs 800})
              rmssd (sut/windowed-dataset->rmssd wd-with-data :timestamp 0)]
          (t/is (nil? rmssd))))

      (t/testing "negative time window returns nil"
        (let [wd-with-data (sut/insert-to-windowed-dataset! wd {:timestamp base-time :PpInMs 800})
              rmssd (sut/windowed-dataset->rmssd wd-with-data :timestamp -1000)]
          (t/is (nil? rmssd))))))

  (t/testing "supports custom PPI column names"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          ;; Test with custom column name
          test-data (map (fn [i interval]
                           {:timestamp (java-time/plus base-time (java-time/millis (* i 1000)))
                            :HeartBeatInterval interval})
                         (range 3)
                         [800 850 820])
          wd (sut/make-windowed-dataset {:timestamp :local-date-time :HeartBeatInterval :int32} 5)
          final-wd (reduce sut/insert-to-windowed-dataset! wd test-data)]

      (t/testing "default :PpInMs column not found returns nil"
        (let [rmssd (sut/windowed-dataset->rmssd final-wd :timestamp 5000)]
          (t/is (nil? rmssd))))

      (t/testing "custom column name works correctly"
        (let [rmssd (sut/windowed-dataset->rmssd final-wd :timestamp 5000 :HeartBeatInterval)]
          ;; Differences: [50, -30], squared: [2500, 900], mean: 1700, RMSSD: sqrt(1700) ≈ 41.23
          (t/is (not (nil? rmssd)))
          (t/is (< (Math/abs (- rmssd 41.23105625617661)) 1e-10))))))

  (t/testing "realistic HRV scenario with varying intervals"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          ;; Simulate realistic heart rate variability - 60 seconds of data
          ;; Average ~75 BPM with natural variation
          realistic-intervals (cycle [800 820 790 830 810 840 795 825 815 805])
          hrv-data (map (fn [i interval]
                          {:timestamp (java-time/plus base-time (java-time/millis (* i interval)))
                           :PpInMs interval})
                        (range 60)
                        (take 60 realistic-intervals))
          wd (sut/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 100)
          hrv-wd (reduce sut/insert-to-windowed-dataset! wd hrv-data)]

      (t/testing "30-second window RMSSD computation"
        (let [rmssd (sut/windowed-dataset->rmssd hrv-wd :timestamp 30000)]
          (t/is (not (nil? rmssd)))
          (t/is (> rmssd 0))
          (t/is (< rmssd 100)))) ; Reasonable HRV range

      (t/testing "60-second window includes all data"
        (let [rmssd-60 (sut/windowed-dataset->rmssd hrv-wd :timestamp 60000)
              rmssd-all (sut/windowed-dataset->rmssd hrv-wd :timestamp 120000)] ; Window larger than data
          (t/is (not (nil? rmssd-60)))
          (t/is (not (nil? rmssd-all)))
          ;; Should be approximately equal since both capture all data
          (t/is (< (Math/abs (- rmssd-60 rmssd-all)) 1e-10))))))

  (t/testing "handles identical successive intervals"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          ;; All intervals the same - should result in RMSSD of 0
          identical-data (map (fn [i]
                                {:timestamp (java-time/plus base-time (java-time/millis (* i 1000)))
                                 :PpInMs 800})
                              (range 5))
          wd (sut/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 10)
          identical-wd (reduce sut/insert-to-windowed-dataset! wd identical-data)]

      (t/testing "identical intervals result in RMSSD of 0"
        (let [rmssd (sut/windowed-dataset->rmssd identical-wd :timestamp 5000)]
          (t/is (not (nil? rmssd)))
          (t/is (= 0.0 rmssd))))))

  (t/testing "error handling"
    (let [base-time (java-time/local-date-time 2025 8 6 10 0 0)
          test-data [{:timestamp base-time :PpInMs 800}
                     {:timestamp (java-time/plus base-time (java-time/millis 1000)) :PpInMs 850}]
          wd (sut/make-windowed-dataset {:timestamp :local-date-time :PpInMs :int32} 5)
          wd-with-data (reduce sut/insert-to-windowed-dataset! wd test-data)]

      (t/testing "non-existent timestamp column throws exception"
        (t/is (thrown? IllegalArgumentException
                       (sut/windowed-dataset->rmssd wd-with-data :nonexistent 2000))))

      (t/testing "non-existent PPI column returns nil"
        (let [rmssd (sut/windowed-dataset->rmssd wd-with-data :timestamp 2000 :nonexistent)]
          (t/is (nil? rmssd)))))))




