"""
Basic SLURM REST API requests for job submission and management.


Defaults:
    It is recommended to run `init_defaults` first to set the default parameters for SLURM requests.
    Defaults may be overridden in each function call or changed via the `default` configuration object.

"""

import logging
import re
from collections.abc import Callable
from enum import Enum
from typing import Any

from aiohttp import ClientResponseError
from rest_requests import JSON, RequestMethod
from rest_requests import request as _rest_request

_logger = logging.getLogger(__name__)


class _Defaults:
    """Configuration for SLURM requests."""

    # REST
    url: str | None = None
    """URL to SLURM server (e.g. `https://example.com/sapi`)."""

    api_version: str | None = None
    """API version to use for SLURM requests (e.g. `v0.0.40`)."""

    user_name: str | None = None
    """User name for SLURM authentication."""

    user_token: str | None = None
    """User token for SLURM authentication."""

    headers: dict[str, str] = {}
    """Headers to include in SLURM requests."""

    timeout: int | None = 600
    """Default request timeout in seconds."""

    proxy_url: str | None = None
    """If provided, the URL of the proxy server to use for the requests. E.g. "socks5://localhost:8080"."""

    # SLURM
    partition: str | None = None
    """Default SLURM partition to submit jobs to."""

    constraints: str | None = None
    """Default SLURM constraints for job submission."""

    environment: list[str] = []
    """Default SLURM environment variables for job submission."""

    # general
    dry_run: bool = False
    """If True, do not actually submit jobs, but simulate the submission process. Recommended for testing/debugging only."""


default = _Defaults()


class SLURMJobState(str, Enum):
    """
    Enum representing the possible states of a SLURM job.

    see https://slurm.schedmd.com/job_state_codes.html
    """

    BOOT_FAIL = "boot_fail"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    DEADLINE = "deadline"
    FAILED = "failed"
    NODE_FAIL = "node_fail"
    OUT_OF_MEMORY = "out_of_memory"
    PENDING = "pending"
    PREEMPTED = "preempted"
    RUNNING = "running"
    SUSPENDED = "suspended"
    TIMEOUT = "timeout"

    @staticmethod
    def select(name: str) -> "SLURMJobState":
        """Selects the SLURMJobState by its name, case insensitive."""
        for state in SLURMJobState:
            if state.value == name.lower():
                return state
        raise ValueError(f"Unknown SLURM job state: {name}")

    def __repr__(self) -> str:
        return f"SLURMJobState.{self.name}"

    def __str__(self) -> str:
        return self.value


def init_defaults(
    *,
    url: str,
    api_version: str,
    user_name: str,
    user_token: str,
    partition: str | None = None,
    constraints: str | None = None,
    environment: list[str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int | None = None,
    proxy_url: str | None = None,
    dry_run: bool = False,
) -> None:
    """
    Sets the default parameters for SLURM REST requests.

    Arguments:
        url: URL to SLURM server (e.g. `https://example.com/sapi`).
        api_version: API version to use for SLURM requests (e.g. `v0.0.40`).
        user_name: User name for SLURM authentication.
        user_token: User token for SLURM authentication.
        partition: Default SLURM partition to submit jobs to.
        environment: Default SLURM environment variables for job submission.
        headers: Additional headers to include in SLURM requests.
        timeout: Default request timeout in seconds.
        proxy_url: If provided, the URL of the proxy server to use for the requests. E.g. "socks5://localhost:8080".
    """

    # check formats
    if environment is not None:
        for env_var in environment:
            if "=" not in env_var:
                raise ValueError(
                    f"Invalid environment variable format: '{env_var}'. Expected format: 'KEY=VALUE'."
                )

    # REST
    default.url = url
    default.api_version = api_version
    default.user_name = user_name
    default.user_token = user_token
    default.headers = headers or {}
    default.timeout = timeout
    default.proxy_url = proxy_url
    # SLURM
    default.partition = partition
    default.constraints = constraints
    default.environment = environment or []
    # general
    default.dry_run = dry_run


async def request(
    method: RequestMethod,
    *,
    midpoint: str,
    endpoint: str,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    headers: dict[str, str] | None = None,
    body: JSON = {},
    timeout: int | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> str | JSON:
    """
    Sends a request to the SLURM server.

    Arguments:
        midpoint: The SLURM midpoint to send the request to (e.g. "slurm" or "slurmdb").
        endpoint: The SLURM endpoint to send the request to (e.g. "job/submit").

    Returns:
        response: `<method> /<midpoint>/<api_version>/<endpoint>` response data (see https://slurm.schedmd.com/rest_api.html)

    Raises:
        RuntimeError: no URL or API version is provided.
    """

    url = url or default.url
    api_version = api_version or default.api_version
    user_name = user_name or default.user_name
    user_token = user_token or default.user_token
    headers = headers or default.headers
    proxy_url = proxy_url or default.proxy_url
    dry_run = dry_run if dry_run is not None else default.dry_run

    if url is None:
        raise RuntimeError(
            "SLURM request URL is not set. Specify 'url' or call 'init_defaults' first."
        )
    if api_version is None:
        raise RuntimeError(
            "SLURM API version is not set. Specify 'api_version' or call 'init_defaults' first."
        )

    full_url = f"{url}/{midpoint}/{api_version}/{endpoint}"
    full_headers = (
        {
            "Content-Type": "application/json",
        }
        | ({"X-SLURM-USER-NAME": user_name} if user_name else {})
        | ({"X-SLURM-USER-TOKEN": user_token} if user_token else {})
        | headers
    )

    try:
        response = await _rest_request(
            method,
            url=full_url,
            headers=full_headers,
            body=body,
            **(dict(timeout=timeout) if timeout is not None else {}),
            proxy_url=proxy_url,
            dry_run=dry_run,
        )
        return response
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to send {method.name} request to {full_url}'."
        ) from e


async def ping(
    *,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> str | JSON:
    """
    Pings the SLURM server to check if it is reachable.

    Returns:
        response: `GET /slurm/<api_version>/ping` response data (see https://slurm.schedmd.com/rest_api.html)
    """
    return await request(
        RequestMethod.GET,
        midpoint="slurm",
        endpoint="ping",
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
    )


async def diagnose(
    *,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> str | JSON:
    """Diagnoses the SLURM server by checking its status and configuration.

    Returns:
        response: `slurm/<api_version>/diag` response data (see https://slurm.schedmd.com/rest_api.html)
    """
    return await request(
        RequestMethod.GET,
        midpoint="slurm",
        endpoint="diag",
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
    )


async def jobs_list(
    *,
    state: set[SLURMJobState] | None = None,
    name_regex: str | None = None,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> str | JSON:
    """Lists all jobs for the authenticated user.

    Arguments:
        state: If provided, filters jobs by their state. (If job has at least one matching state, it is included.)
        name_regex: If provided, filters jobs by their name. (If job matches the regular expression, it is included. Syntax: Python 're' module.)

    Returns:
        response: `GET /slurmdb/<api_version>/jobs` response data (see https://slurm.schedmd.com/rest_api.html).
    """

    user_name = user_name or default.user_name
    dry_run = dry_run if dry_run is not None else default.dry_run

    if user_name == "":
        raise RuntimeError(
            "SLURM user name is not set. Specify 'user_name' or call 'init_defaults' first."
        )

    response = await request(
        RequestMethod.GET,
        midpoint="slurmdb",
        endpoint="jobs",
        body={"users": user_name},
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
    )

    if dry_run:
        return {}

    # manual filtering due to bug in SLURM REST API
    jobs: list[dict[str, Any]] = response["jobs"]  # type: ignore
    if state:
        select: Callable[[dict[str, Any]], bool] = lambda j: bool(
            set(s.lower() for s in j["state"]["current"]) & set(s.value for s in state)
        )
        jobs = list(j for j in jobs if select(j))
    if name_regex:
        pattern = re.compile(name_regex)
        select: Callable[[dict[str, Any]], bool] = lambda j: bool(
            pattern.search(j["name"])
        )
        jobs = list(j for j in jobs if select(j))
    response["jobs"] = jobs  # type: ignore
    return response


async def jobs_list_ids(
    *,
    state: set[SLURMJobState] | None = None,
    name_regex: str | None = None,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> list[int]:
    """Lists all job IDs for the authenticated user.

    Arguments:
        state: If provided, filters jobs by their state. (If job has at least one matching state, it is included.)
        name_regex: If provided, filters jobs by their name. (If job matches the regular expression, it is included. Syntax: Python 're' module.)

    Returns:
        job_ids:  list of job IDs
    """

    dry_run = dry_run if dry_run is not None else default.dry_run

    response = await jobs_list(
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
        state=state,
        name_regex=name_regex,
    )

    if dry_run:
        return []

    jobs: list[dict[str, Any]] = response["jobs"]  # type: ignore
    job_ids: list[int] = [int(job["job_id"]) for job in jobs]
    return job_ids


async def jobs_list_names(
    *,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
    state: set[SLURMJobState] | None = None,
    name_regex: str | None = None,
) -> list[str]:
    """Lists all job names for the authenticated user.

    Arguments:
        state: If provided, filters jobs by their state. (If job has at least one matching state, it is included.)
        name_regex: If provided, filters jobs by their name. (If job matches the regular expression, it is included. Syntax: Python 're' module.)

    Returns:
        job_names: list of job names
    """

    dry_run = dry_run if dry_run is not None else default.dry_run

    response = await jobs_list(
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
        state=state,
        name_regex=name_regex,
    )

    if dry_run:
        return []

    jobs: list[dict[str, Any]] = response["jobs"]  # type: ignore
    job_names: list[str] = [job["name"] for job in jobs]
    return job_names


async def jobs_list_names_and_current_states(
    *,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
    state: set[SLURMJobState] | None = None,
    name_regex: str | None = None,
) -> list[tuple[str, tuple[SLURMJobState]]]:
    """Lists all job names and their current state for the authenticated user.

    Arguments:
        state: If provided, filters jobs by their state. (If job has at least one matching state, it is included.)
        name_regex: If provided, filters jobs by their name. (If job matches the regular expression, it is included. Syntax: Python 're' module.)

    Returns:
        jobs: list of job names and their current state
    """

    dry_run = dry_run if dry_run is not None else default.dry_run

    response = await jobs_list(
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
        state=state,
        name_regex=name_regex,
    )

    if dry_run:
        return []

    jobs: list[dict[str, Any]] = response["jobs"]  # type: ignore
    extract: Callable[[dict[str, Any]], tuple[str, tuple[SLURMJobState]]] = (
        lambda job: (
            job["name"],
            tuple(map(SLURMJobState.select, job["state"]["current"])),
        )  # type: ignore
    )
    job_names: list[tuple[str, tuple[SLURMJobState]]] = [extract(job) for job in jobs]
    return job_names


async def job_submit(
    *,
    name: str,
    working_directory: str,
    script: str,
    partition: str | None = None,
    constraints: str | None = None,
    time_limit: int | None = None,
    tasks_per_node: int | None = None,
    environment: list[str] | None = None,
    stdin: str | None = None,
    stdout: str | None = None,
    stderr: str | None = None,
    dependency: str | None = None,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> tuple[int | None, str | JSON]:
    """Submits a new SLURM job with the given job script.

    Arguments:
        name: The name of the job.
        working_directory: The working directory for the job.
        script: The BASH job script to execute.
        partition: The partition to submit the job to.
        constraints: The constraints for the job.
        time_limit: The time limit for the job in minutes.
        tasks_per_node: The number of tasks per node.
        environment: A list of environment variables to set for the job. Syntax: "KEY=VALUE".
        stdout: The file to write standard output to.
        stderr: The file to write standard error to.
        dependency: The job dependency specification. e.g. "afterok:12345".

    Returns:
        response: the job ID (or `None` if not available) and the `POST /slurm/<api_version>/job/submit` response data (see https://slurm.schedmd.com/rest_api.html).
    """

    partition = partition or default.partition
    constraints = constraints or default.constraints
    environment = environment or default.environment
    dry_run = dry_run if dry_run is not None else default.dry_run

    body: JSON = {
        "job": {
            "name": name,
            "partition": partition,
            "current_working_directory": working_directory,
            "script": script,
        }
    }

    if constraints is not None:
        body["job"]["constraints"] = constraints  # type: ignore
    if time_limit is not None:
        body["job"]["time_limit"] = {"set": True, "number": time_limit}  # type: ignore
    if tasks_per_node is not None:
        body["job"]["tasks_per_node"] = tasks_per_node  # type: ignore
    if environment is not None:
        body["job"]["environment"] = environment  # type: ignore
    if stdin is not None:
        body["job"]["standard_input"] = stdin  # type: ignore
    if stdout is not None:
        body["job"]["standard_output"] = stdout  # type: ignore
    if stderr is not None:
        body["job"]["standard_error"] = stderr  # type: ignore
    if dependency is not None:
        body["job"]["dependency"] = dependency  # type: ignore

    response = await request(
        RequestMethod.POST,
        midpoint="slurm",
        endpoint="job/submit",
        body=body,
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
    )

    if dry_run:
        return None, {}

    job_id: str = response.get("job_id", None)  # type: ignore
    return int(job_id) if job_id is not None else None, response


async def job_status(
    job_id: int,
    *,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> str | JSON | None:
    """Retrieves the status of a SLURM job by its ID.

    Note: Uses the 'slurmdb' instead of the 'slurm' midpoint which is more suitable for job status queries once jobs are no longer alive.

    Arguments:
        job_id: The ID of the job.

    Returns:
        response: `GET /slurmdb/<api_version>/job/{job_id}` response data or `None` if the job is not found (see https://slurm.schedmd.com/rest_api.html)
    """
    try:
        return await request(
            RequestMethod.GET,
            midpoint="slurmdb",
            endpoint=f"job/{job_id}",
            url=url,
            api_version=api_version,
            user_name=user_name,
            user_token=user_token,
            proxy_url=proxy_url,
            dry_run=dry_run,
        )
    except ClientResponseError as e:
        if e.status == 404:
            return None
        else:
            raise e


async def job_current_state_and_reason(
    job_id: int,
    *,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> tuple[set[SLURMJobState], str] | None:
    """Retrieves the status of a SLURM job by its ID.

    Note: Uses the 'slurmdb' instead of the 'slurm' midpoint which is more suitable for job status queries once jobs are no longer alive.

    Arguments:
        job_id: The ID of the job.

    Returns:
        job_states: job's current state and reason or `None` if the job is not found
    """

    dry_run = dry_run if dry_run is not None else default.dry_run

    response = await job_status(
        job_id=job_id,
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
    )

    if dry_run:
        return None

    if response is None:
        return None
    else:
        job: dict[str, Any] = response["jobs"][0]  # type: ignore
        return (
            set(map(SLURMJobState.select, job["state"]["current"])),
            job["state"]["reason"],
        )


async def job_cancel(
    job_id: int,
    *,
    url: str | None = None,
    api_version: str | None = None,
    user_name: str | None = None,
    user_token: str | None = None,
    proxy_url: str | None = None,
    dry_run: bool | None = None,
) -> str | JSON:
    """Cancels a SLURM job by its ID.

    Arguments:
        job_id: The ID of the job.

    Returns:
        response: `DELETE /slurm/<api_version>/job/{job_id}` response data (see https://slurm.schedmd.com/rest_api.html).
    """
    return await request(
        RequestMethod.DELETE,
        midpoint="slurm",
        endpoint=f"job/{job_id}",
        url=url,
        api_version=api_version,
        user_name=user_name,
        user_token=user_token,
        proxy_url=proxy_url,
        dry_run=dry_run,
    )
