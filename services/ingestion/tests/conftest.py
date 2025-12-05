import os
import sys
import pytest

# Force the Test Environment Variables
# This runs BEFORE 'main.py' is imported, so it prevents the crash.
os.environ["RABBITMQ_URL"] = "amqp://guest:guest@mock-broker:5672/"
os.environ["MINIO_ENDPOINT"] = "mock-minio:9000"
os.environ["MINIO_ROOT_USER"] = "admin"
os.environ["MINIO_ROOT_PASSWORD"] = "password123"

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