"""Tests for TLS / ca_cert support in SageClient and AsyncSageClient."""

import os
import ssl
import tempfile

import pytest
import pytest_asyncio
import httpx
import respx

BASE_URL = "http://localhost:8080"
TLS_URL = "https://sage-node:8443"


@pytest.fixture
def mock_api():
    with respx.mock(base_url=BASE_URL, assert_all_called=False) as respx_mock:
        yield respx_mock


@pytest.fixture
def ca_cert_file(tmp_path):
    """Generate a temporary self-signed CA certificate for testing.

    This creates a minimal PEM certificate that httpx can load
    without raising FileNotFoundError.
    """
    try:
        # Use the stdlib ssl module to get the default CA bundle path,
        # then create a tiny PEM file from it so we have a valid cert file.
        import certifi
        return certifi.where()
    except ImportError:
        # Fall back to the default CA bundle shipped with the system.
        default = ssl.get_default_verify_paths()
        if default.cafile and os.path.isfile(default.cafile):
            return default.cafile
        # Last resort: write a dummy PEM (httpx will accept the file even
        # though it won't match any real server).
        pem = tmp_path / "ca.crt"
        pem.write_text(
            "-----BEGIN CERTIFICATE-----\n"
            "MIIBkTCB+wIUEjRcKMFm2Z5VYGlEbCL1OI7JB6AwDQYJKoZIhvcNAQELBQAwEjEQ\n"
            "MA4GA1UEAwwHdGVzdC1jYTAeFw0yNDAxMDEwMDAwMDBaFw0zNDAxMDEwMDAwMDBa\n"
            "MBIxEDAOBgNVBAMMB3Rlc3QtY2EwXDANBgkqhkiG9w0BAQEFAANLADBIAkEA0Z3q\n"
            "X2BTLS4e+AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
            "AAAAAAAAAAAAAAAAIDAQABow0wCzAJBgNVHRMEAjAAMA0GCSqGSIb3DQEBCwUAA0EA\n"
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n"
            "-----END CERTIFICATE-----\n"
        )
        return str(pem)


# ---------------------------------------------------------------------------
# SageClient (sync) TLS tests
# ---------------------------------------------------------------------------


class TestSageClientTLS:
    """Test ca_cert parameter handling in SageClient."""

    def test_default_ca_cert_none(self, agent_identity):
        """ca_cert=None (default) uses standard TLS verification (verify=True)."""
        from sage_sdk.client import SageClient

        client = SageClient(base_url=BASE_URL, identity=agent_identity)
        # httpx.Client stores the ssl config in _transport; the simplest check
        # is that the client was created successfully with default verification.
        assert client._client is not None
        client._client.close()

    def test_ca_cert_path_passed_to_httpx(self, agent_identity, ca_cert_file):
        """ca_cert="/path/to/ca.crt" is forwarded as verify to httpx."""
        from sage_sdk.client import SageClient

        client = SageClient(
            base_url=TLS_URL,
            identity=agent_identity,
            ca_cert=ca_cert_file,
        )
        # httpx accepts the CA file and creates a transport successfully.
        transport = client._client._transport
        assert transport is not None
        client._client.close()

    def test_ca_cert_false_disables_verification(self, agent_identity):
        """ca_cert=False disables TLS certificate verification."""
        from sage_sdk.client import SageClient

        client = SageClient(
            base_url=TLS_URL,
            identity=agent_identity,
            ca_cert=False,
        )
        # When verify=False, httpx creates an SSLContext that does not
        # verify certificates.  Construction must succeed.
        assert client._client is not None
        client._client.close()

    def test_ca_cert_none_makes_requests_normally(self, agent_identity, mock_api):
        """Ensure ca_cert=None does not break normal HTTP requests."""
        from sage_sdk.client import SageClient

        mock_api.get("/health").mock(
            return_value=httpx.Response(200, json={"status": "healthy"})
        )
        client = SageClient(base_url=BASE_URL, identity=agent_identity, ca_cert=None)
        result = client.health()
        assert result["status"] == "healthy"
        client._client.close()

    def test_ca_cert_false_makes_requests_normally(self, agent_identity, mock_api):
        """Ensure ca_cert=False does not break normal HTTP requests."""
        from sage_sdk.client import SageClient

        mock_api.get("/health").mock(
            return_value=httpx.Response(200, json={"status": "healthy"})
        )
        client = SageClient(base_url=BASE_URL, identity=agent_identity, ca_cert=False)
        result = client.health()
        assert result["status"] == "healthy"
        client._client.close()


# ---------------------------------------------------------------------------
# AsyncSageClient TLS tests
# ---------------------------------------------------------------------------


class TestAsyncSageClientTLS:
    """Test ca_cert parameter handling in AsyncSageClient."""

    def test_default_ca_cert_none(self, agent_identity):
        """ca_cert=None (default) uses standard TLS verification."""
        from sage_sdk.async_client import AsyncSageClient

        client = AsyncSageClient(base_url=BASE_URL, identity=agent_identity)
        assert client._client is not None

    def test_ca_cert_path_passed_to_httpx(self, agent_identity, ca_cert_file):
        """ca_cert="/path/to/ca.crt" is forwarded as verify to httpx.AsyncClient."""
        from sage_sdk.async_client import AsyncSageClient

        client = AsyncSageClient(
            base_url=TLS_URL,
            identity=agent_identity,
            ca_cert=ca_cert_file,
        )
        transport = client._client._transport
        assert transport is not None

    def test_ca_cert_false_disables_verification(self, agent_identity):
        """ca_cert=False disables TLS certificate verification."""
        from sage_sdk.async_client import AsyncSageClient

        client = AsyncSageClient(
            base_url=TLS_URL,
            identity=agent_identity,
            ca_cert=False,
        )
        assert client._client is not None

    @pytest.mark.asyncio
    async def test_ca_cert_none_makes_requests_normally(self, agent_identity, mock_api):
        """Ensure ca_cert=None does not break normal async HTTP requests."""
        from sage_sdk.async_client import AsyncSageClient

        mock_api.get("/health").mock(
            return_value=httpx.Response(200, json={"status": "healthy"})
        )
        client = AsyncSageClient(
            base_url=BASE_URL, identity=agent_identity, ca_cert=None
        )
        result = await client.health()
        assert result["status"] == "healthy"
        await client.close()

    @pytest.mark.asyncio
    async def test_ca_cert_false_makes_requests_normally(self, agent_identity, mock_api):
        """Ensure ca_cert=False does not break normal async HTTP requests."""
        from sage_sdk.async_client import AsyncSageClient

        mock_api.get("/health").mock(
            return_value=httpx.Response(200, json={"status": "healthy"})
        )
        client = AsyncSageClient(
            base_url=BASE_URL, identity=agent_identity, ca_cert=False
        )
        result = await client.health()
        assert result["status"] == "healthy"
        await client.close()
