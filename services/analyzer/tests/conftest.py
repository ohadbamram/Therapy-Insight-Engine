import os
import sys
import pytest

os.environ["RABBITMQ_URL"] = "amqp://guest:guest@mock-broker:5672/"
os.environ["GEMINI_API_KEY"] = "fake-key"
os.environ["POSTGRES_USER"] = "user"
os.environ["POSTGRES_PASSWORD"] = "pass"
os.environ["POSTGRES_DB"] = "db"
os.environ["REDIS_URL"] = "redis://mock-redis:6379"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))