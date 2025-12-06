import os
import sys
import pytest

# Force the Test Environment Variables
# This runs BEFORE 'main.py' is imported, so it prevents the crash.
os.environ["RABBITMQ_USER"] = "guest"
os.environ["RABBITMQ_PASSWORD"] = "guest"
os.environ["RABBITMQ_HOST"] = "mock-broker"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_VHOST"] = "/"
os.environ["RABBITMQ_URL"] = "amqp://guest:guest@mock-broker:5672/"
os.environ["MINIO_HOST"] = "mock-minio"
os.environ["MINIO_PORT"] = "9000"
os.environ["MINIO_ENDPOINT"] = "mock-minio:9000"
os.environ["MINIO_ROOT_USER"] = "admin"
os.environ["MINIO_ROOT_PASSWORD"] = "password123"
os.environ["POSTGRES_USER"] = "user"
os.environ["POSTGRES_PASSWORD"] = "pass"
os.environ["POSTGRES_DB"] = "db"
os.environ["POSTGRES_HOST"] = "mock-postgres"

# Add the project root to sys.path
# This ensures we can import 'services' and 'common' without path errors
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

@pytest.fixture(autouse=True)
def mock_env_setup():
    """
    This fixture runs automatically for every test.
    It can be used to reset variables if needed.
    """
    yield