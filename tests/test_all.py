from unittest.mock import patch

import pytest

import slurm_requests as slurm
from slurm_requests import RequestMethod


def test_init_defaults():

    slurm.init_defaults(
        url="http://example.com",
        api_version="v1.2.3",
        user_name="test_user",
        user_token="test_token",
        partition="test_partition",
        constraints="test_constraints",
        environment=["VAR1=value1", "VAR2=value2"],
        headers={"X-Custom-Header": "value"},
        proxy_url="socks5://localhost:8080",
        dry_run=True,
    )

    assert slurm.default.url == "http://example.com"
    assert slurm.default.api_version == "v1.2.3"
    assert slurm.default.user_name == "test_user"
    assert slurm.default.user_token == "test_token"
    assert slurm.default.partition == "test_partition"
    assert slurm.default.constraints == "test_constraints"
    assert slurm.default.environment == ["VAR1=value1", "VAR2=value2"]
    assert slurm.default.headers == {"X-Custom-Header": "value"}
    assert slurm.default.proxy_url == "socks5://localhost:8080"
    assert slurm.default.dry_run is True


@pytest.mark.asyncio
async def test_request():

    # all default

    slurm.default.url = "http://example.com"
    slurm.default.api_version = "v1.2.3"
    slurm.default.user_name = "test_user"
    slurm.default.user_token = "test_token"
    slurm.default.partition = "test_partition"
    slurm.default.constraints = "test_constraints"
    slurm.default.environment = ["VAR1=value1", "VAR2=value2"]
    slurm.default.headers = {"X-Custom-Header": "value"}
    slurm.default.proxy_url = "socks5://localhost:8080"
    slurm.default.dry_run = True

    with patch("slurm_requests._rest_request") as mock_rest_request:

        await slurm.request(
            RequestMethod.GET,
            midpoint="slurm",
            endpoint="ping",
        )

        mock_rest_request.assert_called_once_with(
            RequestMethod.GET,
            url="http://example.com/slurm/v1.2.3/ping",
            headers={
                "Content-Type": "application/json",
                "X-SLURM-USER-NAME": "test_user",
                "X-SLURM-USER-TOKEN": "test_token",
                "X-Custom-Header": "value",
            },
            body={},
            proxy_url="socks5://localhost:8080",
            dry_run=True,
        )

    #  overwrite

    slurm.default.url = "http://example.com"
    slurm.default.api_version = "v1.2.3"
    slurm.default.user_name = "test_user"
    slurm.default.user_token = "test_token"
    slurm.default.partition = "test_partition"
    slurm.default.constraints = "test_constraints"
    slurm.default.environment = ["VAR1=value1", "VAR2=value2"]
    slurm.default.headers = {"X-Custom-Header": "value"}
    slurm.default.proxy_url = None
    slurm.default.dry_run = False

    with patch("slurm_requests._rest_request") as mock_rest_request:

        await slurm.request(
            RequestMethod.GET,
            midpoint="slurm",
            endpoint="ping",
            url="http://example.com/overridden",
            api_version="v9.9.9",
            user_name="overridden_user",
            user_token="overridden_token",
            headers={"X-Custom-Header": "overridden_value"},
            proxy_url="socks5://localhost:9090",
            dry_run=True,
        )

        mock_rest_request.assert_called_once_with(
            RequestMethod.GET,
            url="http://example.com/overridden/slurm/v9.9.9/ping",
            headers={
                "Content-Type": "application/json",
                "X-SLURM-USER-NAME": "overridden_user",
                "X-SLURM-USER-TOKEN": "overridden_token",
                "X-Custom-Header": "overridden_value",
            },
            body={},
            proxy_url="socks5://localhost:9090",
            dry_run=True,
        )


@pytest.mark.asyncio
async def test_relay_to_request():

    # all default

    slurm.default.url = "http://example.com"
    slurm.default.api_version = "v1.2.3"
    slurm.default.user_name = "test_user"
    slurm.default.user_token = "test_token"
    slurm.default.partition = "test_partition"
    slurm.default.constraints = "test_constraints"
    slurm.default.environment = ["VAR1=value1", "VAR2=value2"]
    slurm.default.headers = {"X-Custom-Header": "value"}
    slurm.default.proxy_url = None
    slurm.default.dry_run = True

    with patch("slurm_requests._rest_request") as mock_rest_request:

        await slurm.ping()

        mock_rest_request.assert_called_once_with(
            RequestMethod.GET,
            url="http://example.com/slurm/v1.2.3/ping",
            headers={
                "Content-Type": "application/json",
                "X-SLURM-USER-NAME": "test_user",
                "X-SLURM-USER-TOKEN": "test_token",
                "X-Custom-Header": "value",
            },
            body={},
            proxy_url=None,
            dry_run=True,
        )

    # overwrite

    with patch("slurm_requests._rest_request") as mock_rest_request:

        await slurm.ping(
            url="http://example.com/overridden",
            api_version="v9.9.9",
            user_name="overridden_user",
            user_token="overridden_token",
            proxy_url=None,
            dry_run=True,
        )

        mock_rest_request.assert_called_once_with(
            RequestMethod.GET,
            url="http://example.com/overridden/slurm/v9.9.9/ping",
            headers={
                "Content-Type": "application/json",
                "X-SLURM-USER-NAME": "overridden_user",
                "X-SLURM-USER-TOKEN": "overridden_token",
                "X-Custom-Header": "value",
            },
            body={},
            proxy_url=None,
            dry_run=True,
        )
