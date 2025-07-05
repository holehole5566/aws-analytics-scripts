from opensearchpy import OpenSearch, helpers
import random
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

TICKERS = ["EKS", "S3", "EC2"]
DOC_COUNT = 256
BATCH_SIZE = 50

def create_opensearch_client():
    """Create and validate OpenSearch client"""
    # Validate required environment variables
    required_vars = ["OPENSEARCH_ENDPOINT", "OPENSEARCH_USER", "OPENSEARCH_PWD", "OPENSEARCH_INDEX"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    host = os.environ.get("OPENSEARCH_ENDPOINT")
    auth = (os.environ.get("OPENSEARCH_USER"), os.environ.get("OPENSEARCH_PWD"))
    
    try:
        client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=30
        )
        # Test connection
        client.info()
        return client
    except Exception as e:
        raise RuntimeError(f"Failed to connect to OpenSearch: {e}")

def generate_document():
    """Generate a single document with realistic timestamp"""
    return {
        "metadata": {
            "product": {
                "name": random.choice(TICKERS)
            }
        },
        "message": "ResponseComplete",
        "time": int(datetime.now(timezone.utc).timestamp() * 1000),
        "severity": "Informational",
        "activity_name": "Update",
        "@timestamp": datetime.now(timezone.utc).isoformat()
    }

def bulk_index_documents(client, index_name, doc_count=DOC_COUNT):
    """Bulk index documents for better performance"""
    def doc_generator():
        for i in range(doc_count):
            yield {
                "_index": index_name,
                "_source": generate_document()
            }
    
    try:
        # Use bulk helper for efficient indexing
        success_count, failed_docs = helpers.bulk(
            client,
            doc_generator(),
            chunk_size=BATCH_SIZE,
            request_timeout=60
        )
        
        print(f"Successfully indexed {success_count} documents")
        if failed_docs:
            print(f"Failed to index {len(failed_docs)} documents")
            
        return success_count
        
    except Exception as e:
        print(f"Bulk indexing failed: {e}")
        sys.exit(1)

def main():
    """Main execution function"""
    try:
        client = create_opensearch_client()
        index_name = os.environ.get("OPENSEARCH_INDEX")
        
        print(f"Starting bulk indexing to {index_name}...")
        success_count = bulk_index_documents(client, index_name)
        
        # Refresh index to make documents searchable
        client.indices.refresh(index=index_name)
        
        # Verify document count
        count = client.count(index=index_name)
        print(f"Total documents in index: {count['count']}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()