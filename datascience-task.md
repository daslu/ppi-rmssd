# Data Science Assignment: Real-Time RMSSD Smoothing for Relaxation Monitoring
## Overview
We are facing instability and noise in real-time RMSSD values in our data, and we need your expertise to help us identify suitable methods to clean pulse-to-pulse interval data and produce smooth, interpretable RMSSD estimates!
Your Python application must analyze a dataset containing pulse-to-pulse interval (PPI) data collected during relaxation sessions and identify a method for stable real-time RMSSD computation using rolling windows and smoothing techniques. 
This assignment will test your ability to work with real-world data, implement efficient algorithms, and provide actionable insights.

## Requirements
- Create a Python project that connects to a PostgreSQL database
- Import the provided CSV dataset into the database
- Implement data processing and analysis algorithms to compute RMSSD over rolling windows and apply noise reduction techniques for stable real-time feedback
- Provide clear documentation of your approach and findings
- Demonstrate your understanding of:
  - Data preprocessing and cleaning
  - Signal processing and smoothing of physiological time-series data
  - Efficient database operations
  - Python best practices (PEP 8, type hints, docstrings)
  - Proper error handling and logging
- Include comprehensive unit tests
- Use atomic commit history to show your progression
- Create a GitHub repository with your solution and share the link with us

## Dataset
We will provide you with a CSV file containing pulse-to-pulse interval (PPI) data collected during elaxation sessions exported from our PostgreSQL database.

## Task
Develop a Python solution that:

- Imports the PPI data into PostgreSQL
- Preprocesses the signal (e.g., detects and removes artifacts or implausible values)
- Segments the data using a rolling time window (e.g., 30--60 seconds) and computes RMSSD for each window
- Applies smoothing techniques such as moving average, exponential smoothing, or adaptive filters to reduce RMSSD volatility
- Stores the smoothed RMSSD signal back into the database
- Provides visual comparisons of raw vs. cleaned/smoothed RMSSD time series and explains the rationale behind chosen filtering techniques

## Expected Deliverables
### GitHub repository containing

- Python code for data import, processing, and analysis
- SQL scripts for database setup and queries
- Unit tests
- Documentation

### Comprehensive README.md that includes
- Clear explanation of your approach
- Reasoning behind your methodology
- Instructions for setting up and running your code
- Summary of findings and insights
- Any assumptions or limitations of your solution

## Tips
- Focus on creating efficient algorithms that can scale with larger datasets
- Consider both artifact removal and signal smoothing approaches
- Pay attention to edge cases in the data
- Document your thought process and decision-making
- Use Python's scientific stack (NumPy, pandas, scipy) effectively
- Follow best practices for database operations

## Evaluation Criteria
We will evaluate your submission based on:

- Code quality and organization
- Algorithmic efficiency
- Accuracy of real-time RMSSD smoothing and noise handling
- Documentation quality
- Test coverage
- Adherence to Python best practices
- Database design and query optimization
- Overall approach and reasoning

Good luck! We're excited to see your solution to this challenging problem.
