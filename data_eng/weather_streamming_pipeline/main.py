# ===== main.py =====
import os
import sys
import time
import threading
import logging
import signal
from dotenv import load_dotenv

# Add project paths
sys.path.append('producer')
sys.path.append('consumer')

from setup_environment import KafkaSetup
from producer.weather_producer import WeatherProducer
from consumer.spark_consumer import WeatherStreamProcessor

# Load environment variables
load_dotenv('config/.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherPipeline:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.producer_thread = None
        self.consumer_thread = None
        self.running = False
    
    def setup_environment(self):
        """Setup Kafka topics and environment"""
        logger.info("Setting up environment...")
        setup = KafkaSetup()
        
        if not setup.setup_kafka_topics():
            logger.error("Failed to setup Kafka topics")
            return False
        
        if not setup.create_cities_csv():
            logger.error("Failed to create cities CSV")
            return False
        
        return True
    
    def start_producer(self):
        """Start the weather data producer"""
        try:
            logger.info("Starting weather producer...")
            self.producer = WeatherProducer()
            self.producer.run_producer(interval=300)  # 5 minutes
        except Exception as e:
            logger.error(f"Error in producer: {e}")
    
    def start_consumer(self):
        """Start the Spark stream processor"""
        try:
            logger.info("Starting Spark stream processor...")
            # Wait a bit for producer to start sending data
            time.sleep(10)
            self.consumer = WeatherStreamProcessor()
            self.consumer.process_stream()
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal, stopping pipeline...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the complete pipeline"""
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Setup environment
        if not self.setup_environment():
            logger.error("Environment setup failed")
            return
        
        self.running = True
        
        # Start producer in a separate thread
        self.producer_thread = threading.Thread(target=self.start_producer, daemon=True)
        self.producer_thread.start()
        
        # Start consumer in a separate thread
        self.consumer_thread = threading.Thread(target=self.start_consumer, daemon=True)
        self.consumer_thread.start()
        
        logger.info("Weather streaming pipeline started successfully!")
        logger.info("Press Ctrl+C to stop the pipeline")
        
        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop the pipeline"""
        logger.info("Stopping weather streaming pipeline...")
        self.running = False
        
        # Stop threads gracefully
        if self.producer:
            logger.info("Stopping producer...")
        
        if self.consumer:
            logger.info("Stopping consumer...")
        
        logger.info("Pipeline stopped successfully")

if __name__ == "__main__":
    pipeline = WeatherPipeline()
    pipeline.start()