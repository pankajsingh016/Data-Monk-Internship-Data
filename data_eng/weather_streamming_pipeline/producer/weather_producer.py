#  ===== producer/weather_producer.py =====
import json
import time
import csv
import requests
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import os
from dotenv import load_dotenv
from data_models import WeatherData

# Load environment variables
load_dotenv('config/.env')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherProducer:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_WEATHER_TOPIC', 'weather-data')
        
        if not self.api_key:
            raise ValueError("OPENWEATHER_API_KEY not found in environment variables")
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        
    def fetch_weather_data(self, lat: float, lon: float) -> dict:
        """Fetch weather data from OpenWeatherMap API"""
        try:
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def send_weather_data(self, weather_data: WeatherData):
        """Send weather data to Kafka topic"""
        try:
            # Use city as key for partitioning
            future = self.producer.send(
                self.topic,
                key=weather_data.city,
                value=weather_data.__dict__
            )
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            logger.info(f"Sent data for {weather_data.city} to partition {record_metadata.partition}")
            
        except KafkaError as e:
            logger.error(f"Failed to send data to Kafka: {e}")
            raise
    
    def load_cities_from_csv(self, csv_file: str):
        """Load cities from CSV file"""
        cities = []
        try:
            # Fix path to look in data directory
            if not os.path.exists(csv_file):
                csv_file = os.path.join('data', csv_file)
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cities.append({
                        'city': row['city'],
                        'lat': float(row['lat']),
                        'lon': float(row['lon'])
                    })
            return cities
        except Exception as e:
            logger.error(f"Failed to load cities from CSV: {e}")
            raise
    
    def run_producer(self, csv_file: str = 'indian_cities.csv', interval: int = 300):
        """Run the weather data producer"""
        cities = self.load_cities_from_csv(csv_file)
        logger.info(f"Loaded {len(cities)} cities from {csv_file}")
        
        while True:
            try:
                for city_info in cities:
                    try:
                        # Fetch weather data
                        api_data = self.fetch_weather_data(
                            city_info['lat'], 
                            city_info['lon']
                        )
                        
                        # Create WeatherData object
                        weather_data = WeatherData.from_api_response(
                            api_data, 
                            city_info['city']
                        )
                        
                        # Send to Kafka
                        self.send_weather_data(weather_data)
                        logger.info(f"Processed {weather_data.city}: {weather_data.temp:.1f}°C, {weather_data.weather_desc}")
                        
                        # Small delay between API calls to avoid rate limiting
                        time.sleep(1)  # Increased to 1 second for safety
                        
                    except Exception as e:
                        logger.error(f"Error processing {city_info['city']}: {e}")
                        continue
                
                logger.info(f"Completed batch processing. Sleeping for {interval} seconds...")
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("Producer stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in producer loop: {e}")
                time.sleep(30)  # Wait before retrying
        
        self.producer.close()

if __name__ == "__main__":
    producer = WeatherProducer()
    producer.run_producer()