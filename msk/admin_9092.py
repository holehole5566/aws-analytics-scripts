from kafka import KafkaProducer,KafkaAdminClient
from kafka.errors import KafkaError
from kafka.sasl.oauth import AbstractTokenProvider
import socket
import signal
import sys
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
from dotenv import load_dotenv

load_dotenv()

def create_admin():
    """Create and validate MSK producer"""
    # Validate required environment variables
    bootstrap_servers = os.getenv("MSK_BS")
    if not bootstrap_servers:
        raise ValueError("MSK_BS environment variable is required")
    
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers
        )
        print(f"Connected to MSK.")
        return admin
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
        admin = create_admin()
        topics = admin.list_topics()
        for topic in topics:
            print(f"{topic}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        try:
            admin.close()
            print("Admin client closed successfully.")
        except:
            pass

if __name__ == "__main__":
    main()