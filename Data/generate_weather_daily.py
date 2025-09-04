import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate weather_daily.csv with exactly 100 records
np.random.seed(42)  # For reproducibility
dates = [datetime(2025, 1, 1) + timedelta(days=x) for x in range(34)] + \
        [datetime(2025, 1, 1) + timedelta(days=x) for x in range(33)] + \
        [datetime(2025, 1, 1) + timedelta(days=x) for x in range(33)]  # 34 + 33 + 33 = 100 records
cities = ['New York'] * 34 + ['Chicago'] * 33 + ['Los Angeles'] * 33
weather_conditions = ['Sunny', 'Cloudy', 'Rainy', 'Partly Cloudy', 'Snow']
weather_daily_data = {
    'date': [d.strftime('%Y-%m-%d') for d in dates],
    'city': cities,
    'temperature_c': np.random.uniform(10, 35, 100).round(1),  # Random temps between 10 and 35°C
    'humidity': np.random.randint(40, 90, 100),  # Random humidity between 40-90%
    'weather_condition': np.random.choice(weather_conditions, 100)
}
pd.DataFrame(weather_daily_data).to_csv('weather_daily.csv', index=False)
print("Generated weather_daily.csv with 100 records")