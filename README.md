# Multi-Source Data Warehouse on Snowflake

## 📌 Project Overview
This project demonstrates the creation of an end-to-end Data Warehouse on **Snowflake**, integrating multiple data sources (CSV, JSON, and API) and orchestrating the ETL pipeline using **Apache Airflow**. The architecture follows a **Raw → Staging → Silver → Gold** pipeline, transforming raw data into clean, structured, and analytics-ready datasets for business insights.

---

## 🔗 Data Sources
- **CSV Files**: Orders, Customers, Products
- **JSON Files**: Clickstream logs
- **API**: Real-time weather data from OpenWeather

---

## 🏗️ Architecture
The data pipeline is structured into the following stages:

1. **Raw (Stage)**: Landing zone for raw CSV, JSON, and API data.
2. **Staging**: Temporary structured tables (e.g., `stg_orders`, `stg_weather`).
3. **Silver**: Cleaned and transformed data with proper data type conversions and JSON parsing.
4. **Gold**: Analytics-ready star schema with a fact table (`fact_sales`) and dimension tables (`dim_customer`, `dim_product`, `dim_date`, `dim_location`).
5. **Automation**: Apache Airflow schedules and orchestrates the ETL workflow.

### 📊 Star Schema (Gold Layer)
- **Fact Table**: `fact_sales`
- **Dimension Tables**:
  - `dim_customer`
  - `dim_product`
  - `dim_date`
  - `dim_location`

---

## ⚙️ Tech Stack
- **Snowflake**: Cloud-based Data Warehouse
- **Python**: Data ingestion and API handling
- **Apache Airflow**: ETL pipeline orchestration
- **SQL**: Data transformations and schema design

---

## 🚀 Key Features
- ✅ Multi-source data ingestion (CSV, JSON, API)
- ✅ Fully automated ETL pipeline with Apache Airflow
- ✅ Cleaned and transformed data in the Silver Layer
- ✅ Business-friendly star schema in the Gold Layer
- ✅ Scalable, production-grade Data Warehouse architecture



---

## 🎯 Outcomes
- Built a production-ready Data Warehouse on Snowflake
- Automated ETL processes using Apache Airflow
- Designed a scalable, analytics-ready star schema
- Created a portfolio-worthy Data Engineering project

---

## ⚡ Next Steps
- Integrate BI tools (e.g., Power BI, Tableau) for visualization and dashboards
- Enhance Airflow pipelines to support real-time workflows
- Expand data sources and optimize for larger datasets

---

## 📝 Getting Started
1. Clone the repository.
2. Set up Snowflake and Apache Airflow environments.
3. Configure data sources (CSV, JSON, OpenWeather API).
4. Run the Airflow DAGs to execute the ETL pipeline.
5. Query the Gold Layer for analytics and insights.

---

## 📂 Repository Structure
