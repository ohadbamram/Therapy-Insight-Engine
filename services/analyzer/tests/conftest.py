import os
import sys

os.environ["RABBITMQ_USER"] = "guest"
os.environ["RABBITMQ_PASSWORD"] = "guest"
os.environ["RABBITMQ_HOST"] = "mock-broker"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_VHOST"] = "/"
os.environ["GEMINI_API_KEY"] = "fake-key"
os.environ["POSTGRES_USER"] = "user"
os.environ["POSTGRES_PASSWORD"] = "pass"
os.environ["POSTGRES_DB"] = "db"
os.environ["POSTGRES_HOST"] = "mock-postgres"
os.environ["POSTGRES_PORT"] = "5432"
os.environ["REDIS_HOST"] = "mock-redis"
os.environ["REDIS_PORT"] = "6379"
os.environ["REDIS_DB"] = "0"
os.environ["GEMINI_API_KEY"] = "fake-key"
os.environ["LLM"] = "gemini-3-pro"


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))