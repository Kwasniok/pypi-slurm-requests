# slurm-requests

[![PyPI - Version](https://img.shields.io/pypi/v/slurm-requests.svg)](https://pypi.org/project/slurm-requests)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/slurm-requests.svg)](https://pypi.org/project/slurm-requests)

-----

Asynchronous SLURM REST requests with proxy support.

## Installation

```console
pip install slurm-requests
```

## Usage

```python
import asyncio

import slurm_requests as slurm


async def main():

    # set defaults to avoid repetition
    slurm.init_defaults(
        url="https://example.com/sapi",
        api_version="v0.0.40",
        user_name="example_user",
        user_token="example_token",
        partition="example_partition",
        # constraints="GPU",
        environment=["EXAMPLE_VAR=example_value"],
        # headers={"X-Example-Header": "example_value"},
        # proxy_url="socks5://localhost:8080",
    )

    # check connection + credentials
    await slurm.ping()
    await slurm.diagnose()

    # submit
    job_id, _ = await slurm.job_submit(
        name="example_job",
        working_directory="/home/example_user/slurm",
        script="#!/usr/bin/bash\necho Hello, SLURM!",
        # time_limit=60,
        # dependency="afterok:123456",
    )
    assert job_id is not None

    # check state
    response = await slurm.job_current_state_and_reason(job_id=job_id)
    assert response is not None
    state, reason = response
    print(f"Job {job_id} is currently in state '{state}' due to reason '{reason}'.")

    # cancel
    await slurm.job_cancel(job_id=job_id)

    # advanced: overwrite a default (works for all functions)
    await slurm.ping(user_name="dummy", dry_run=True)


if __name__ == "__main__":
    asyncio.run(main())

```

## License

`slurm-requests` is distributed under the terms of the [CC-BY-SA-4.0](http://creativecommons.org/licenses/by-sa/4.0) license.
