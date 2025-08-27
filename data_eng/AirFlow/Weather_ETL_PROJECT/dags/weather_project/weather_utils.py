import os
import requests
from dotenv import load_dotenv
from datetime import datetime, date

load_dotenv()

API_KEY = os.getenv('WEATHER_API_KEY')

# BASE_URL_CURRENT = "https://api.openweathermap.org/data/2.5/weather"
# BASE_URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def fetch_current_weather(lat,lon):
    """_summary_:
       Fetch the live weather using Open Weather API 

    Args:
        lat (_type_): latitude of a particular location
        lon (_type_): longitude of a particular location
    """

    params = {
        "lat":lat,
        "lon":lon,
        "appid":API_KEY,
        "units":"metric"
    }

    r = requests.get(BASE_URL,params=params)
    r.raise_for_status()
    return r.json()