from opensearchpy import OpenSearch
import random
import os
from dotenv import load_dotenv

load_dotenv()


# 連接設置
host = os.environ.get("OPENSEARCH_ENDPOINT")  # OpenSearch主機
port = 443  # OpenSearch端口
auth = (os.environ.get("OPENSEARCH_USER"), os.environ.get("OPENSEARCH_PWD"))  # username/password

# 創建OpenSearch客戶端
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=auth,
    use_ssl=True,  # 如果使用https
    verify_certs=False,  # 在生產環境中建議設為True
    ssl_show_warn=False
)

# 索引名稱
index_name =  os.environ.get("OPENSEARCH_INDEX")

# 生成256筆隨機向量數據
for i in range(256):
    # 生成一個4維的隨機向量
    tickers = ["EKS", "S3", "EC2"]
    # 產生隨機數據
    data = {
        "metadata": {
            "product": {
                "name": random.choice(tickers)
            }
        },
        "message": "ResponseComplete",
        "time": 1750662612577,
        "severity": "Informational",
        "activity_name": "Update",
        "@timestamp": "2025-06-23T07:10:12.577Z"
    }

    # 插入文檔
    response = client.index(
        index=index_name,
        body=data
    )

    # 打印進度
    if (i + 1) % 50 == 0:
        print(f"已插入 {i + 1} 筆數據")

print("完成所有數據插入")

# 刷新索引以確保數據可見
client.indices.refresh(index=index_name)

# 驗證文檔數量
count = client.count(index=index_name)
print(f"索引中的文檔總數: {count['count']}")
