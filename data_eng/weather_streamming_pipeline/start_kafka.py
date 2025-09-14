#!/usr/bin/env python3
# ===== start_kafka.py =====
import os
import sys
import time
import subprocess
import logging
import signal
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaServerManager:
    def __init__(self):
        self.kafka_dir = Path('kafka')
        self.zookeeper_proc = None
        self.kafka_proc = None
        self.running = False
        
        # Check if Kafka directory exists
        if not self.kafka_dir.exists():
            logger.error(f"Kafka directory not found at {self.kafka_dir}")
            sys.exit(1)
    
    def start_zookeeper(self):
        """Start Zookeeper server"""
        logger.info("Starting Zookeeper...")
        try:
            zk_script = self.kafka_dir / 'bin' / 'zookeeper-server-start.sh'
            zk_config = self.kafka_dir / 'config' / 'zookeeper.properties'
            
            self.zookeeper_proc = subprocess.Popen(
                [str(zk_script), str(zk_config)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for Zookeeper to start
            time.sleep(5)
            
            if self.zookeeper_proc.poll() is None:
                logger.info("Zookeeper started successfully")
                return True
            else:
                logger.error("Failed to start Zookeeper")
                return False
                
        except Exception as e:
            logger.error(f"Error starting Zookeeper: {e}")
            return False
    
    def start_kafka(self):
        """Start Kafka server"""
        logger.info("Starting Kafka server...")
        try:
            kafka_script = self.kafka_dir / 'bin' / 'kafka-server-start.sh'
            kafka_config = self.kafka_dir / 'config' / 'server.properties'
            
            self.kafka_proc = subprocess.Popen(
                [str(kafka_script), str(kafka_config)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for Kafka to start
            time.sleep(10)
            
            if self.kafka_proc.poll() is None:
                logger.info("Kafka server started successfully")
                return True
            else:
                logger.error("Failed to start Kafka server")
                return False
                
        except Exception as e:
            logger.error(f"Error starting Kafka server: {e}")
            return False
    
    def check_kafka_health(self):
        """Check if Kafka is responding"""
        try:
            topics_script = self.kafka_dir / 'bin' / 'kafka-topics.sh'
            result = subprocess.run(
                [str(topics_script), '--list', '--bootstrap-server', 'localhost:9092'],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def stop_servers(self):
        """Stop Kafka and Zookeeper servers"""
        logger.info("Stopping servers...")
        
        if self.kafka_proc:
            logger.info("Stopping Kafka server...")
            self.kafka_proc.terminate()
            try:
                self.kafka_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.kafka_proc.kill()
        
        if self.zookeeper_proc:
            logger.info("Stopping Zookeeper...")
            self.zookeeper_proc.terminate()
            try:
                self.zookeeper_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.zookeeper_proc.kill()
        
        logger.info("Servers stopped")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal")
        self.running = False
        self.stop_servers()
        sys.exit(0)
    
    def start_all(self):
        """Start both Zookeeper and Kafka"""
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        try:
            # Start Zookeeper
            if not self.start_zookeeper():
                return False
            
            # Start Kafka
            if not self.start_kafka():
                self.stop_servers()
                return False
            
            # Health check
            logger.info("Performing health check...")
            if self.check_kafka_health():
                logger.info("✅ Kafka cluster is healthy and ready!")
                logger.info("✅ You can now run: python main.py")
                self.running = True
            else:
                logger.error("❌ Kafka health check failed")
                self.stop_servers()
                return False
            
            # Keep running
            logger.info("Press Ctrl+C to stop the servers")
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.stop_servers()
        
        return True

def main():
    manager = KafkaServerManager()
    
    logger.info("=" * 50)
    logger.info("🚀 Starting Kafka Cluster for Weather Pipeline")
    logger.info("=" * 50)
    
    if manager.start_all():
        logger.info("Kafka cluster started successfully!")
    else:
        logger.error("Failed to start Kafka cluster")
        sys.exit(1)

if __name__ == "__main__":
    main()