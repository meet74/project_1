# Project 1: Health Data Analysis

## Project Title
Key Indicators of Health Data Analysis

## Project Description
This project focuses on analyzing key indicators of heart disease using a dataset derived from the CDC's Behavioral Risk Factor Surveillance System (BRFSS). The dataset includes various health-related variables that can help predict heart disease. The project involves data ingestion, cleaning, transformation, and analysis to derive insights and support healthcare decision-making.

## Steps Followed and Observations
## Data Ingestion:
   - Ingested data from the CDC's BRFSS dataset using Azure Data Factory.
   - Stored raw data in Azure Data Lake Storage (ADLS) Gen2.

## Data Cleaning and Transformation:
   - Used PySpark in Azure Databricks to clean and transform the data.
   - Handled missing values and corrected inconsistencies.
   - Performed feature engineering to create new variables for analysis.

## Data Warehousing:
   - Loaded cleaned data into Azure Synapse Analytics.
   - Created external tables to reference data stored in ADLS for efficient querying.

## Data Analysis:
   - Executed SQL queries in Synapse Analytics to analyze the data.
   - Visualized insights using Power BI.

## Data Pipeline Process
- **Data Ingestion:** Ingested data from CDC BRFSS using ADF.
- **Data Cleaning and Transformation:** Cleaned and transformed data using PySpark in Databricks.
- **Data Warehousing:** Loaded data into Synapse Analytics and created external tables.
- **Data Analysis:** Performed SQL queries and visualized insights in Power BI.

## Transformation and Analytics Results
- **Total Sales by Country:** Bar charts showing total sales for each country.
- **High Quantity Transactions:** Tables listing high quantity transactions.
- **Average Unit Price by Stock Code:** Line charts showing average unit prices.
- **Monthly Sales Analysis:** Area charts showing monthly sales trends.
- **Top Selling Products:** Horizontal bar charts showing top selling products.
- **Customer Purchase Patterns:** Pie charts showing customer spending distribution.

## Conclusion
The project successfully analyzed key indicators of heart disease, providing valuable insights for healthcare decision-making. The use of Azure services ensured scalability and efficiency, enabling thorough data cleaning, transformation, and analysis.

