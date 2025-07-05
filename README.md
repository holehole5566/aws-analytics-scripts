## Configuration

This project uses environment variables stored in a .env file in the project root directory, and loads them using the python-dotenv package.

### Example `.env`
```
OPENSEARCH_ENDPOINT = "your-opensearch-endpoint"
OPENSEARCH_USER = "your-username"
OPENSEARCH_PWD = "your-password"
OPENSEARCH_INDEX = "your-index-name"


MSK_BS_IAM = "your-msk-bootstrap-server:9098"
MSK_TOPIC = "your-msk-topic"


HTTP_URL = "https://your-opensearch-domain-endpoint/_search"
```

