import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# Generate weather_hourly.json with exactly 100 records
np.random.seed(42)  # For reproducibility
start_date = datetime(2025, 8, 1)
hours = [(start_date + timedelta(hours=x)).strftime('%Y-%m-%d %H:%M') for x in range(100)]  # 100 hours
weather_hourly_data = [
    {
        'timestamp': h,
        'temperature_c': round(np.random.uniform(12, 22), 1),  # London-like temps
        'wind_speed_kph': np.random.randint(5, 25),
        'precipitation_mm': round(np.random.uniform(0, 2), 1)
    } for h in hours
]
with open('weather_hourly.json', 'w') as f:
    json.dump(weather_hourly_data, f, indent=2)
print("Generated weather_hourly.json with 100 records")