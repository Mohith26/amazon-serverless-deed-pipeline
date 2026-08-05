import pytest


@pytest.fixture(autouse=True)
def _aws_env(request, monkeypatch):
    """Provide dummy AWS creds. Unit tests (moto) must NOT see AWS_ENDPOINT_URL;
    integration tests manage it themselves."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    if request.node.get_closest_marker("integration") is None:
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
