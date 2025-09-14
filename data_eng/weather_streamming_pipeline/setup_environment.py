# ===== setup_environment.py =====
import subprocess
import logging
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/.env')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaSetup:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.weather_topic = os.getenv('KAFKA_WEATHER_TOPIC', 'weather-data')
        self.analytics_topic = os.getenv('KAFKA_ANALYTICS_TOPIC', 'weather-analytics')
        # Fix the path to kafka scripts
        self.kafka_bin = os.path.join(os.getcwd(), 'kafka', 'bin')
    
    def create_topic(self, topic_name: str, partitions: int = 3, replication: int = 1):
        """Create Kafka topic if it doesn't exist"""
        try:
            # Use correct path to kafka-topics.sh
            topics_script = os.path.join(self.kafka_bin, 'kafka-topics.sh')
            
            cmd = [
                topics_script,
                '--create',
                '--topic', topic_name,
                '--bootstrap-server', self.kafka_servers,
                '--partitions', str(partitions),
                '--replication-factor', str(replication),
                '--if-not-exists'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                logger.info(f"Topic '{topic_name}' created successfully")
            else:
                logger.warning(f"Topic creation result: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout creating topic {topic_name}")
        except FileNotFoundError:
            logger.error(f"kafka-topics.sh not found. Make sure Kafka is installed at {self.kafka_bin}")
        except Exception as e:
            logger.error(f"Error creating topic {topic_name}: {e}")
    
    def list_topics(self):
        """List all Kafka topics"""
        try:
            topics_script = os.path.join(self.kafka_bin, 'kafka-topics.sh')
            cmd = [topics_script, '--list', '--bootstrap-server', self.kafka_servers]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                topics = result.stdout.strip().split('\n') if result.stdout.strip() else []
                logger.info(f"Available topics: {topics}")
                return topics
            else:
                logger.error(f"Error listing topics: {result.stderr}")
                
        except FileNotFoundError:
            logger.error(f"kafka-topics.sh not found. Make sure Kafka is installed at {self.kafka_bin}")
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
        
        return []
    
    def check_kafka_server(self):
        """Check if Kafka server is running"""
        try:
            topics = self.list_topics()
            return topics is not None
        except Exception:
            return False
    
    def setup_kafka_topics(self):
        """Setup required Kafka topics"""
        logger.info("Setting up Kafka topics...")
        
        # Check if Kafka server is running
        if not self.check_kafka_server():
            logger.error("Kafka server is not running or not accessible")
            logger.info("Please start Kafka server first:")
            logger.info(f"cd {os.path.join(os.getcwd(), 'kafka')}")
            logger.info("bin/zookeeper-server-start.sh config/zookeeper.properties &")
            logger.info("bin/kafka-server-start.sh config/server.properties &")
            return False
        
        # Create topics
        self.create_topic(self.weather_topic, partitions=3)
        self.create_topic(self.analytics_topic, partitions=1)
        
        return True
    
    def create_cities_csv(self):
        """Create Indian cities CSV - with fallback cities"""
        csv_path = os.path.join('data', 'indian_cities.csv')
        
        # Check if CSV already exists
        if os.path.exists(csv_path):
            logger.info(f"Cities CSV already exists at {csv_path}")
            return True
        
        # Try to create from city.list.json first
        if os.path.exists('city.list.json'):
            try:
                import json
                with open('city.list.json', 'r') as f:
                    data = json.load(f)
                
                # Ensure data directory exists
                os.makedirs('data', exist_ok=True)
                
                with open(csv_path, 'w') as wf:
                    wf.write("city,id,lon,lat,country\n")
                    count = 0
                    for v in data:
                        if v['country'] == "IN":
                            wf.write(f"{v['name']},{v['id']},{v['coord']['lon']},{v['coord']['lat']},{v['country']}\n")
                            count += 1
                
                logger.info(f"Created indian_cities.csv with {count} cities from city.list.json")
                return True
                
            except Exception as e:
                logger.error(f"Error creating cities CSV from JSON: {e}")
        
        # Fallback: create CSV with hardcoded major Indian cities
        logger.info("city.list.json not found, creating CSV with major Indian cities...")
        
        try:
            # Ensure data directory exists
            os.makedirs('data', exist_ok=True)
            
            # Major Indian cities with their coordinates
            cities_data = [
                "city,id,lon,lat,country",
                "New Delhi,1261481,77.23114,28.61282,IN",
                "Mumbai,1275339,72.847939,19.01441,IN", 
                "Bengaluru,1277333,77.60335,12.97194,IN",
                "Kolkata,1275004,88.36972,22.56972,IN",
                "Chennai,1264527,80.27847,13.08784,IN",
                "Hyderabad,1269843,78.47444,17.38405,IN",
                "Pune,1259229,73.85535,18.51957,IN",
                "Ahmedabad,1279233,72.58602,23.02579,IN",
                "Jaipur,1269515,75.79216,26.92207,IN",
                "Surat,1255364,72.83198,21.19594,IN",
                "Lucknow,1264733,80.94616,26.83928,IN",
                "Kanpur,1267995,80.35033,26.47194,IN",
                "Nagpur,1262180,79.08886,21.14631,IN",
                "Indore,1269743,75.8472,22.71792,IN",
                "Bhopal,1275841,77.40289,23.25469,IN",
                "Visakhapatnam,1253405,83.21815,17.68009,IN",
                "Patna,1260086,85.13563,25.59408,IN",
                "Vadodara,1253573,73.20812,22.29941,IN",
                "Agra,1279259,78.00122,27.18671,IN",
                "Nashik,1262321,73.78121,19.99727,IN"
            ]
            
            with open(csv_path, 'w') as wf:
                for line in cities_data:
                    wf.write(line + '\n')
            
            logger.info(f"Created indian_cities.csv with {len(cities_data)-1} major Indian cities")
            return True
            
        except Exception as e:
            logger.error(f"Error creating fallback cities CSV: {e}")
            return False

def main():
    setup = KafkaSetup()
    
    # Create cities CSV first (doesn't require Kafka)
    if setup.create_cities_csv():
        logger.info("Cities CSV created successfully")
    else:
        logger.error("Cities CSV creation failed")
        return
    
    # Setup Kafka topics
    if setup.setup_kafka_topics():
        logger.info("Kafka setup completed successfully")
    else:
        logger.error("Kafka setup failed - make sure Kafka server is running")
        return

if __name__ == "__main__":
    main()