import os
import sys
import pytest

# Mock Environment
os.environ["POSTGRES_USER"] = "user"
os.environ["POSTGRES_PASSWORD"] = "pass"
os.environ["POSTGRES_DB"] = "db"
os.environ["POSTGRES_HOST"] = "mock-postgres"

# Fix Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


@pytest.fixture(autouse=True)
def mock_env_setup():
    yield
