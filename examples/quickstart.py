"""Record one explicit host observation; inspect it with `fr turns FILE`."""

import argparse
from pathlib import Path

from mcp_flightrecorder import Recorder


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording", type=Path, help="New SQLite file (parent directory must exist)")
    path = parser.parse_args().recording
    recorder = Recorder(
        path,
        capture="content",
        coverage={
            "producers": {"model": None, "host": "quickstart", "mcp": None},
            "model_complete": False,
        },
    )
    try:
        with recorder.turn("Prepare context") as turn:
            if turn.receipt.status != "accepted":
                raise RuntimeError(f"Turn not captured: {turn.receipt.reason}")
            result = recorder.observe(
                "host.tool_result",
                scope=recorder.capture_scope(),
                payload={"text": "Host-prepared context"},
                facts={"text_paths": ["/text"]},
            )
            if result.status != "accepted":
                raise RuntimeError(f"Observation not captured: {result.reason}")
    finally:
        closed = recorder.close()
    if not closed.writer_stopped or not closed.portable:
        raise RuntimeError(f"Recording not portable: {closed.reason}")
    print(path)


if __name__ == "__main__":
    main()
