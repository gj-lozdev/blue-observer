import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
import json

cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

month_ranges = {
    "january":   ("2024-01-01", "2024-01-31"),
    "february":  ("2024-02-01", "2024-02-29"),  
    "march":     ("2024-03-01", "2024-03-31"),
    "april":     ("2024-04-01", "2024-04-30"),
    "may":       ("2024-05-01", "2024-05-31"),
    "june":      ("2024-06-01", "2024-06-30"),
    "july":      ("2024-07-01", "2024-07-31"),
    "august":    ("2024-08-01", "2024-08-31"),
    "september": ("2024-09-01", "2024-09-30"),
    "october":   ("2024-10-01", "2024-10-31"),
    "november":  ("2024-11-01", "2024-11-30"),
    "december":  ("2024-12-01", "2024-12-31")
}

def get_weather(lat, lon, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,rain_sum,windspeed_10m_max",
        "timezone": "Europe/London"
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    daily = response.Daily()

    dates = pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )

    df = pd.DataFrame({
        "date": dates,
        "temp_max": daily.Variables(0).ValuesAsNumpy(),
        "temp_min": daily.Variables(1).ValuesAsNumpy(),
        "rain_sum": daily.Variables(2).ValuesAsNumpy(),
        "wind_max": daily.Variables(3).ValuesAsNumpy()
    })

    avg_temp = round(((df["temp_max"] + df["temp_min"]) / 2).mean(), 2)
    avg_rain = round(df["rain_sum"].mean(), 2)
    avg_wind = round(df["wind_max"].mean(), 2)

    return {
        "avg_temp": f"{avg_temp:.2f}",
        "avg_rain": f"{avg_rain:.2f}",
        "avg_wind": f"{avg_wind:.2f}"
    }


# Example usage
with open('./_site/data/points.json', 'r', encoding='utf-8') as f:
    all_points = json.load(f)

updated_points = []

for point in all_points:
    month = point.get('month', '').lower()
    start_date, end_date = month_ranges.get(month, ("2024-01-01", "2024-12-31"))

    weather = get_weather(
        point['lat'],
        point['lng'],
        start_date,
        end_date
    )

    point["avg_temp"] = weather["avg_temp"]
    point["avg_rain"] = weather["avg_rain"]
    point["avg_wind"] = weather["avg_wind"]

    updated_points.append(point)

output_path = './_site/data/points.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(updated_points, f, indent=4, ensure_ascii=False)