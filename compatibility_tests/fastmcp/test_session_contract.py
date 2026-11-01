"""Native session cases in the separate resolved FastMCP environment.

The existing public behavioral cases and owned stdio peer use this environment's
fixture configuration, standard asyncio runner and labelled recording fixture.
"""
import pytest

from mcp_flightrecorder import Recorder

from ..native.test_session import (
    test_session_surface_shared_receipts_and_unawaited_ownership,
    test_session_listing_distinguishes_null_empty_and_explicit_null_params,
    test_session_explicit_pages_keep_typed_params_order_and_metadata,
    test_session_default_call_keeps_sdk_schema_refresh_inside_one_operation,
    test_session_all_nondefaults_forward_identity_and_native_progress,
    test_session_native_terminal_categories_preserve_original_outcome,
    test_session_native_intermediate_empty_and_denied_results_stay_distinct,
    test_session_real_missing_content_wire_preserves_sdk_validation_error,
    test_session_labelled_projection_doubles_preserve_unknown_and_old_contract,
    test_session_invocation_scope_and_execution_snapshot_survive_later_completion,
)


@pytest.fixture
def recording(evidence_path):
    recorder = Recorder(evidence_path / "recording.sqlite", capture="content",
                        coverage={"producers": {"model": None,
                            "host": "integration.session-contract",
                            "mcp": "mcp-session/2.3.0"}, "model_complete": False})
    try:
        yield recorder, evidence_path / "recording.sqlite"
    finally:
        closed = recorder.close(timeout=5)
        assert closed.writer_stopped, "Each case must reap its own recorder writer even after a failed assertion."
