from __future__ import annotations
import argparse
import asyncio
import json

from .api import run_demo_flow, run_plan_flow


def main() -> None:
    parser = argparse.ArgumentParser(description="MultiAgentHub CLI")
    sub = parser.add_subparsers(dest="cmd")

    p_demo = sub.add_parser("demo", help="Run the demo flow")
    p_demo.add_argument("--query", default="climate change impacts")
    p_plan = sub.add_parser("plan", help="Generate a plan for a goal")
    p_plan.add_argument("--goal", default="Research topic")

    args = parser.parse_args()
    if args.cmd == "demo":
        result = asyncio.run(run_demo_flow(args.query))
        print(json.dumps(result, indent=2))
    elif args.cmd == "plan":
        result = asyncio.run(run_plan_flow(args.goal))
        print(json.dumps(result, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
