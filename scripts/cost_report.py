"""Monthly cost report.

On real AWS this would query Cost Explorer / the Cost & Usage Report filtered by
the `Project=deedstream` tag. This project is measured entirely on LocalStack
(free / local), which does NOT emit billing data, so the monthly cost is
UNMEASURED / BLOCKED. We refuse to invent a number.
"""

from __future__ import annotations

import json
import os

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")


def main():
    report = {
        "cost_per_month_usd": None,
        "status": "UNMEASURED/BLOCKED",
        "reason": (
            "Measured on LocalStack (emulated AWS), which is free and emits no "
            "billing data. A real monthly cost requires deploying to a real AWS "
            "account and reading Cost Explorer for tag Project=deedstream."
        ),
        "how_to_measure_on_real_aws": [
            "sam deploy --tags Project=deedstream",
            "aws ce get-cost-and-usage --time-period Start=<m-start>,End=<m-end> "
            "--granularity MONTHLY --metrics UnblendedCost "
            "--filter '{\"Tags\":{\"Key\":\"Project\",\"Values\":[\"deedstream\"]}}'",
        ],
    }
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, "cost.json"), "w") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
