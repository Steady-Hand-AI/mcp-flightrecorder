"""A deterministic, SDK-free host using explicit local execution and replay.

Run with installed mcp-flightrecorder 0.5.23; no API key or network service is used.
Use the native MCP/OpenAI recipes in docs/easy-integration.md for those clients.
"""
import argparse
import json
from pathlib import Path

from mcp_flightrecorder.easy import AgentRecorder


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='New recording path (parent must exist)')
    parser.add_argument('--disabled', action='store_true', help='Run the same host without a recording')
    args = parser.parse_args()
    messages = [{'role': 'user', 'content': 'Echo alpha'}]
    with AgentRecorder(None if args.disabled else args.output, capture='content',
                       all_model_calls_recorded=True) as recorder:
        conversation = recorder.conversation('example')
        with conversation.turn('Echo alpha'):
            with conversation.model_call({'messages': messages}) as call:
                # A real HTTP host sends this payload here and passes its exposed JSON to done.
                response = {'name': 'echo', 'arguments': {'text': 'alpha'}}
                call.done(response)
            selected = call.choice(response['name'], response['arguments'])
            with selected.run(local=True) as attempt:
                result = attempt.call(lambda text: text, **response['arguments'])
            messages.append(attempt.answer({'role': 'tool', 'content': result}, key='answer-1'))
        with conversation.turn('Continue'):
            with conversation.model_call({'messages': messages}, message_keys=[None, 'answer-1']) as call:
                call.done({'content': result})
        conversation.close()
    print(json.dumps({'answer': result, 'recording': None if args.disabled else str(args.output.resolve())}))


if __name__ == '__main__':
    main()
