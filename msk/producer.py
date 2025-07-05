from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.sasl.oauth import AbstractTokenProvider
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
from dotenv import load_dotenv # type: ignore
load_dotenv()


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("MSK_BS_IAM"),
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    client_id=socket.gethostname(),
)

topic = os.getenv("MSK_TOPIC")
while True:
    try:
        inp=input(">")
        producer.send(topic, inp.encode())
        producer.flush()
        print("Produced!")
    except Exception:
        print("Failed to send message")

producer.close()