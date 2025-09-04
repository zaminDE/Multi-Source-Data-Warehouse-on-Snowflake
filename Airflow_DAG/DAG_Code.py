from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json, os, subprocess
import snowflake.connector

# 🔑 OpenWeather API Key
API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = "Karachi"

# ❄️ Snowflake Connection
SNOWFLAKE_CONN = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

# 📂 Local folder for JSON files
LOCAL_DIR = "F:/CDE/Projects/Open_Weather_Data_Warehouse"
os.makedirs(LOCAL_DIR, exist_ok=True)

def fetch_and_put():
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    print("🌍 API Response:", data)  # Debugging

    # ✅ Save JSON to local file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(LOCAL_DIR, f"weather_{CITY.lower()}_{timestamp}.json")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"✅ File saved: {file_path}")

    # ✅ Upload file to Snowflake stage
    stage_name = "WEATHER_DB.RAW.WEATHER_STAGE_API"
    put_command = f'snowsql -q "PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE"'
    subprocess.run(put_command, shell=True, check=True)

    print("✅ File uploaded to Snowflake stage")

def load_into_table():
    # ❄️ Connect to Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cs = conn.cursor()

    # Ensure staging table exists
    cs.execute("""
        CREATE TABLE IF NOT EXISTS STG_WEATHER_API (
            CITY STRING,
            TEMPERATURE_C FLOAT,
            HUMIDITY FLOAT,
            WEATHER_CONDITION STRING,
            WIND_SPEED_KPH FLOAT,
            FETCH_DATE TIMESTAMP
        )
    """)

    # ✅ Copy from staged JSON into table (with Snowflake JSON parsing)
    cs.execute("""
        COPY INTO STG_WEATHER_API
        FROM (
            SELECT 
                $1:name::STRING AS CITY,
                $1:main.temp::FLOAT AS TEMPERATURE_C,
                $1:main.humidity::FLOAT AS HUMIDITY,
                $1:weather[0].description::STRING AS WEATHER_CONDITION,
                $1:wind.speed::FLOAT AS WIND_SPEED_KPH,
                CURRENT_TIMESTAMP AS FETCH_DATE
            FROM @WEATHER_DB.RAW.WEATHER_STAGE_API
        )
        FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
    """)

    print("✅ Data loaded into STG_WEATHER_API")

    conn.commit()
    cs.close()
    conn.close()

# DAG defaults
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "openweather_api_put_to_snowflake",
    default_args=default_args,
    start_date=datetime(2025, 9, 3),
    schedule_interval="*/2 22 * * *",  # 10–11 pm, every 2 min
    catchup=False,
) as dag:

    fetch_and_put_task = PythonOperator(
        task_id="fetch_and_put_openweather",
        python_callable=fetch_and_put
    )

    load_into_table_task = PythonOperator(
        task_id="load_into_snowflake_table",
        python_callable=load_into_table
    )

    fetch_and_put_task >> load_into_table_task