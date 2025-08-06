(ns docs
  (:require [scicloj.clay.v2.api :as clay]))

(clay/make! {:format [:quarto :html]
             :base-source-path "notebooks"
             :source-path ["index.clj"
                           "ppi_docs/api_reference.clj"
                           "ppi_docs/preparations.clj"
                           "ppi_docs/analysis.clj"]
             :base-target-path "docs"
             :book {:title "PPI RMSSD Data Analysis Library"}
             :clean-up-target-dir true})
