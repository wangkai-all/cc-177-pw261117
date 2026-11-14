#!/usr/bin/env python3
"""Entry point: python -I run.py [--port 8000] [--db data/intake.db]"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from intake_workbench.server import serve  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="入仓工作台")
    parser.add_argument("--db", default="data/intake.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--timeout", type=int, default=300,
                        help="等待区超时秒数 (默认 300)")
    args = parser.parse_args()
    serve(db_path=args.db, host=args.host, port=args.port,
          timeout_seconds=args.timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
