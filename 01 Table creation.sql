-- 1. Database & Schema Setup
CREATE OR REPLACE DATABASE weather_db;
CREATE OR REPLACE SCHEMA raw;

USE DATABASE weather_db;
USE SCHEMA raw;

-- 2. Stages 
CREATE OR REPLACE STAGE weather_stage;
CREATE OR REPLACE STAGE weather_stage_json;
CREATE OR REPLACE STAGE weather_stage_api;

-- 3. Staging Tables
CREATE OR REPLACE TABLE stg_weather_daily (
    date DATE,
    city STRING,
    temperature_c FLOAT,
    humidity INT,
    weather_condition STRING
);

CREATE OR REPLACE TABLE stg_weather_hourly (
    raw VARIANT
);

CREATE OR REPLACE TABLE stg_weather_api (
    city STRING,
    temperature_c FLOAT,
    humidity INT,
    weather_condition STRING,
    wind_speed_kph FLOAT,
    fetch_date TIMESTAMP
);