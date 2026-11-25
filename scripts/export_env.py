#!/usr/bin/env python3
"""Export deployed AWS CloudFormation outputs to local .env after `make deploy-aws`."""

from __future__ import annotations

import os
from pathlib import Path


def export_stack_outputs() -> dict[str, str]:
    """Fetch CloudFormation stack outputs via Boto3 and update .env."""
    import boto3

    region = os.getenv("AWS_REGION", "us-east-1")
    cfn = boto3.client("cloudformation", region_name=region)
    stacks = [
        "EvmKinesisDataLakeStack",
        "EvmRedshiftWarehouseStack",
        "EvmQuickSightBiStack",
    ]

    exported: dict[str, str] = {}
    for stack_name in stacks:
        resp = cfn.describe_stacks(StackName=stack_name)
        for out in resp["Stacks"][0].get("Outputs", []):
            exported[out["OutputKey"]] = out["OutputValue"]

    env_path = Path(__file__).resolve().parent.parent / ".env"
    lines = [f"{k}={v}" for k, v in sorted(exported.items())]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return exported


if __name__ == "__main__":
    result = export_stack_outputs()
    print(f"Exported {len(result)} CloudFormation outputs to .env")
