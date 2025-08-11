# This file is responsible for creating the indian_cities.csv data to make api calls more efficient


import json
with open('city.list.json','r') as f:
    data = json.load(f)
    
wf =  open('india_cities.csv','w')
wf.write("city,id,lon,lat,country\n")
for v in data:
    if v['country']=="IN":
        wf.write(f"{v['name']},{v['id']},{v['coord']['lon']},{v['coord']['lat']},{v['country']}\n")


# city,code
# New Delhi,1261481
    

'''
Sample API Call

https://api.openweathermap.org/data/2.5/weather?lat=44.34&lon=10.99&appid={API key}

Sample OUTPUT

{
  "coord": {
    "lon": 10.99,
    "lat": 44.34
  },
  "weather": [
    {
      "id": 501,
      "main": "Rain",
      "description": "moderate rain",
      "icon": "10d"
    }
  ],
  "base": "stations",
  "main": {
    "temp": 298.48, // kelvin
    "feels_like": 298.74, //kelvin
    "temp_min": 297.56,
    "temp_max": 300.05,
    "pressure": 1015,
    "humidity": 64,
    "sea_level": 1015,
    "grnd_level": 933
  },
  "visibility": 10000, // in meters
  "wind": {
    "speed": 0.62,
    "deg": 349,
    "gust": 1.18
  },
  "rain": {
    "1h": 3.16
  },
  "clouds": {
    "all": 100
  },
  "dt": 1661870592,
  "sys": {
    "type": 2,
    "id": 2075663,
    "country": "IT",
    "sunrise": 1661834187,
    "sunset": 1661882248
  },
  "timezone": 7200,
  "id": 3163858,
  "name": "Zocca",
  "cod": 200
}


'''