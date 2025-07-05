from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.sasl.oauth import AbstractTokenProvider
import socket
import signal
import sys
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
from dotenv import load_dotenv

load_dotenv()

class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self, region='us-east-1'):
        self.region = region
    
    def token(self):
        try:
            token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
            return token
        except Exception as e:
            raise RuntimeError(f"Failed to generate MSK auth token: {e}")

def create_producer():
    """Create and validate MSK producer"""
    # Validate required environment variables
    bootstrap_servers = os.getenv("MSK_BS_IAM")
    if not bootstrap_servers:
        raise ValueError("MSK_BS_IAM environment variable is required")
    
    region = os.getenv("AWS_REGION", "us-east-1")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider(region),
            client_id=socket.gethostname(),
            request_timeout_ms=30000,
            retries=3
        )
        return producer
    except Exception as e:
        raise RuntimeError(f"Failed to create Kafka producer: {e}")

def signal_handler(sig, frame):
    """Handle graceful shutdown"""
    print("\nShutting down gracefully...")
    sys.exit(0)

def main():
    """Main producer loop"""
    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        producer = create_producer()
        topic = os.getenv("MSK_TOPIC")
        if not topic:
            raise ValueError("MSK_TOPIC environment variable is required")
        
        print(f"Connected to MSK. Sending messages to topic: {topic}")
        print("Type messages (Ctrl+C to exit):")
        
        while True:
            try:
                message = input("> ")
                if message.strip():  # Only send non-empty messages
                    producer.send(topic, message.encode('utf-8'))
                    producer.flush()
                    print("Message sent!")
            except KafkaError as e:
                print(f"Kafka error: {e}")
            except Exception as e:
                print(f"Failed to send message: {e}")
                
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        try:
            producer.close()
            print("Producer closed.")
        except:
            pass

if __name__ == "__main__":
    main()