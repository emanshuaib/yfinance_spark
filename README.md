## Global Market Real-Time Monitoring and Visualization

## Overview
Develops a Spark Streaming application to monitor the real-time status of major global stock market indices and generate periodic visual summaries to track market trends.


## Steps
1. Data Acquisition: Real-time stock data is fetched every minute and saved as a CSV file.
2. Spark Streaming: Reads the data as a stream and computes average price, volatility, and price changes.
3. Visualization:  
   - Time-series plots  
   - Volatility bar charts  
   - Price change heatmaps  
   - Interactive dashboards using Plotly and Streamlit


## Folder Structure

- spark_steaming/data_streaming.py: Streams data from Yahoo Finance and stores it in a CSV file.  
- spark_steaming/structured_streaming.py: Reads the CSV file as a stream and computes metrics using a 5-minute sliding window.  
- spark_steaming/shared_spark.py: Initializes and returns a Spark session.  
- spark_steaming/streamlit_app.py: Streamlit dashboard to visualize real-time data.  
- spark_steaming/yfinance_visualization.ipynb: Listens to a socket port for incoming structured data. Extracts subsets every 10 minutes and saves them. Generates time-series, heatmaps, and bar charts.  
- spark_steaming/global_stock_data/: all_stocks_data.csv: Main CSV storing real-time stock data.  
- spark_steaming/market_dashboard.html: A basic HTML dashboard summarizing trends.  
- spark_steaming/output/: CSV storing structured real-time stock data for last 10 min.


## Requirements

- Python 3.10+
- Libraries: pyspark, pandas, matplotlib, seaborn, plotly, yfinance
- Jupyter Lab or Notebook


## Install Requirements

conda create -n spark python=3.10
conda activate spark
conda install pyspark pandas matplotlib seaborn plotly
pip install yfinance
conda install -c conda-forge jupyterlab


## How to Run (Using Anaconda): 

Open your terminal, navigate to the project directory, and run the following commands:

### Step 1: Launch Jupyter Notebook
conda activate spark
jupyter lab

### Step 2: Start Data Acquisition
conda activate spark
cd spark_steaming
python data_streaming.py

### Step 3: Start Spark Streaming Pipeline
conda activate spark
cd spark_steaming
spark-submit structured_streaming.py

### Step 4: Launch Streamlit Dashboard
conda activate spark
cd spark_steaming
streamlit run streamlit_app.py


## 👩‍💻 Author
Eman Shuaib
University of Bahrain  
ITML606 - Big Data Analytics (Semester 2, 2024/2025)