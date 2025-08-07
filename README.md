# PPI-RMSSD Analysis

A Clojure library for analyzing heart rate variability from Polar device data.

## What this does

This project processes Pulse-to-Pulse Interval (PPI) data from Polar heart rate monitors to calculate RMSSD (Root Mean Square of Successive Differences) - a key measure of heart rate variability used in cardiovascular health and relaxation monitoring.

The main challenge we're solving: real-time HRV measurements are often noisy and jumpy, making them hard to interpret. This library provides smoothing algorithms and data quality checks to produce stable, meaningful HRV feedback.

## Table of contents

* [Main page](https://daslu.github.io/ppi-rmssd/)
* [API Reference](https://daslu.github.io/ppi-rmssd/ppi_docs.api_reference.html)
* [Getting the Data Ready](https://daslu.github.io/ppi-rmssd/ppi_docs.preparations.html)
* [Analysis: Data Smoothing/Cleaning and RMSSD Estimation](https://daslu.github.io/ppi-rmssd/ppi_docs.analysis.html)

## Key features

- **Data cleaning**: Handles malformed CSV files from Polar devices
- **Signal processing**: Implements various smoothing filters (moving averages, median filters, cascaded approaches)
- **Window-based processing:** Offers a round-robin structure over tech.ml.dataset datasets for efficient window-based processing (both batch and streaming)
- **Quality assessment**: Detects measurement discontinuities and data quality issues
- **RMSSD calculation**: Windowed computation with configurable parameters
- **Analysis notebooks**: Literate programming approach for research and visualization
- **Systematic comparison of methods**: Evaluated methods against synthetic distrortion of ground-truth series

## Structure

- `src/ppi/api.clj` - Core data processing functions
- `notebooks/` - Research notebooks with analysis and visualizations
- `test/` - Unit tests for the API functions

## Dependencies

Built on the Clojure data science ecosystem:

- [**Noj**](https://scicloj.github.io/noj/) - data science toolkit:
  - [**dtype-next**](https://cnuernber.github.io/dtype-next/) - array programming
  - [**tech.ml.dataset**](https://techascent.github.io/tech.ml.dataset/) - efficient datasets
  - [**Tablecloth**](https://scicloj.github.io/tablecloth/) - dataset ergonomics
  - [**Fastmath**](https://generateme.github.io/fastmath/clay/) - math
  - [**Tableplot**](https://scicloj.github.io/tableplot/) - plotting
  - [**Clay**](https://scicloj.github.io/clay/) - Literate programming and visualization

## Recommended workflow

* Add the raw data file under the `data` directory.
* Just explore most of the code in the REPL.
* For tests: `clj -M:test-main`.
* For visualizations, use Clay's [setup](https://scicloj.github.io/clay/#setup) in your favourite editor.
* To render [the documentation](https://daslu.github.io/ppi-rmssd/), run the `dev/docs.clj` script. You will need [Quarto](https://quarto.org/) installed in your system.




