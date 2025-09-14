# ===== producer/data_models.py =====
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Optional

@dataclass
class WeatherData:
    city: str
    country: str
    lat: float
    lon: float
    temp: float
    humidity: int
    weather_desc: str
    utc_timestamp: str
    local_timestamp: str
    dt: int
    timezone_offset: int
    
    def to_json(self) -> str:
        return json.dumps(self.__dict__)
    
    @classmethod
    def from_api_response(cls, api_data: dict, city: str) -> 'WeatherData':
        from datetime import datetime, timezone, timedelta
        
        # Extract data from API response
        temp = api_data['main']['temp'] - 273.15  # Convert Kelvin to Celsius
        humidity = api_data['main']['humidity']
        weather_desc = api_data['weather'][0]['description']
        country = api_data['sys']['country']
        dt = api_data['dt']
        timezone_offset = api_data['timezone']
        lat = api_data['coord']['lat']
        lon = api_data['coord']['lon']
        
        # Convert timestamps
        utc_datetime = datetime.fromtimestamp(dt, tz=timezone.utc)
        utc_timestamp = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        offset_delta = timedelta(seconds=timezone_offset)
        local_datetime = utc_datetime + offset_delta
        local_timestamp = local_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        return cls(
            city=city,
            country=country,
            lat=lat,
            lon=lon,
            temp=temp,
            humidity=humidity,
            weather_desc=weather_desc,
            utc_timestamp=utc_timestamp,
            local_timestamp=local_timestamp,
            dt=dt,
            timezone_offset=timezone_offset
        )