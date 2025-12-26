import os
import sys
import pytest

os.environ["RABBITMQ_URL"] = "amqp://guest:guest@mock-broker:5672/"
os.environ["RABBITMQ_USER"] = "guest"
os.environ["RABBITMQ_PASSWORD"] = "guest"
os.environ["RABBITMQ_HOST"] = "mock-broker"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_VHOST"] = "/"
os.environ["MINIO_HOST"] = "mock-minio"
os.environ["MINIO_PORT"] = "9000"
os.environ["MINIO_ROOT_USER"] = "admin"
os.environ["MINIO_ROOT_PASSWORD"] = "password123"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


@pytest.fixture(autouse=True)
def mock_env_setup():
    yield
