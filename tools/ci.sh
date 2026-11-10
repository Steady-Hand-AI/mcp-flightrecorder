#!/usr/bin/env bash
# Exercise committed public source and installed distributions in a fresh directory.
set -euo pipefail

profile=${1:?Usage: bash tools/ci.sh core|integration}
case "$profile" in
    core|integration) ;;
    *) echo 'Expected core or integration' >&2; exit 2 ;;
esac
unset PYTHONPATH PYTHONHOME PYTHONOPTIMIZE PYTEST_ADDOPTS PYTEST_DISABLE_PLUGIN_AUTOLOAD
export PYTHONDONTWRITEBYTECODE=1
python=$(command -v "${MFR_PYTHON:-python3}")
repo=$(git rev-parse --show-toplevel)
work=$(mktemp -d "${MFR_CI_PARENT:-${RUNNER_TEMP:-/tmp}}/mfr-ci-XXXXXX")
"$python" -I -B - "$repo" "$work" <<'PYTHON'
from pathlib import Path
import sys
assert not Path(sys.argv[2]).resolve().is_relative_to(Path(sys.argv[1]).resolve()), "Working directory must be outside the checkout"
PYTHON
chmod 0711 "$work"
umask 022
export TMPDIR="$work"
source_dir="$work/source"
mkdir "$source_dir"
git -C "$repo" archive --format=tar HEAD | tar -xf - -C "$source_dir"
printf 'Committed source: %s\nWorking directory: %s\n' "$(git -C "$repo" rev-parse HEAD)" "$work"

"$python" -I -B - <<'PYTHON'
import sqlite3
import sys
assert sys.implementation.name == "cpython"
assert sys.version_info[:2] >= (3, 11)
assert sys.version_info.releaselevel == "final"
assert getattr(sys, "_is_gil_enabled", lambda: True)()
assert not sys.flags.optimize
print(sys.executable, sys.version, "SQLite", sqlite3.sqlite_version)
PYTHON

"$python" -I -B "$source_dir/tools/check_package.py" --source "$source_dir" --report "$work/source-report.json"
cp -R "$source_dir" "$work/build-source"
"$python" -I -B -m venv "$work/build-env"
build_python="$work/build-env/bin/python"
"$build_python" -I -B -m pip install build 'setuptools>=77.0.3' packaging
"$build_python" -I -B -m build --no-isolation --wheel --sdist --outdir "$work/dist" "$work/build-source"
wheel="$work/dist/mcp_flightrecorder-0.5.19-py3-none-any.whl"
sdist="$work/dist/mcp_flightrecorder-0.5.19.tar.gz"
"$python" -I -B "$source_dir/tools/check_package.py" --source "$source_dir" --wheel "$wheel" --sdist "$sdist" --report "$work/package-report.json"
MFR_GUARD_SOURCE="$source_dir" MFR_GUARD_WHEEL="$wheel" MFR_GUARD_SDIST="$sdist" \
    "$python" -I -B "$source_dir/tools/test_package_guard.py"

"$python" -I -B -m venv "$work/test-env"
test_python="$work/test-env/bin/python"
"$test_python" -I -B -m pip install "$wheel"
requirements=(-r "$source_dir/requirements/test.txt")
if [[ "$profile" == integration ]]; then
    requirements+=(-r "$source_dir/requirements/integration.txt")
fi
"$test_python" -I -B -m pip install "${requirements[@]}"
"$test_python" -I -B -m pip check
"$test_python" -I -B -m pip freeze > "$work/resolved-dependencies.txt"
cat "$work/resolved-dependencies.txt"
cd "$work"
"$test_python" -I -B "$source_dir/tools/installed_smoke.py" --source "$source_dir" --output "$work/smoke" --profile "$profile"

cd "$source_dir"
if [[ "$profile" == core ]]; then
    tests=(
        tests/test_admission.py tests/test_async_shutdown.py tests/test_bindings.py
        tests/test_capture_values.py tests/test_comparisons.py tests/test_contract.py
        tests/test_core.py tests/test_deferred.py tests/test_duplicate_transition.py
        tests/test_easy.py tests/test_lifecycle_ownership.py tests/test_inspection_diagnostics.py
        tests/test_failures.py tests/test_finalization.py tests/test_fixture.py
        tests/test_inspection_logging.py tests/test_inspection_query.py
        tests/test_inspection_snapshot.py tests/test_logging.py
        tests/test_scope_validation.py tests/test_shutdown.py tests/test_scale_regressions.py
    )
else
    tests=(
        tests/test_integration_fixture.py tests/test_integration_history.py
        tests/test_tool_roundtrip.py
        tests/test_easy_sdk.py
        tests/test_mcp_client.py tests/test_openai_chat.py
        tests/test_inspection_checks.py tests/test_inspection_commands.py
        tests/test_inspection_content.py tests/test_inspection_inputs.py
        tests/test_inspection_navigation.py tests/test_inspection_slice.py
    )
fi
"$test_python" -I -B -m pytest -n 4 -p no:cacheprovider --junitxml="$work/tests.xml" "${tests[@]}"

# One lane also proves that the sdist alone can rebuild an installable wheel.
if [[ "$profile" == core ]] && "$python" -I -B -c 'import sys; sys.exit(sys.version_info[:2] != (3, 11))'; then
    mkdir "$work/from-sdist"
    "$python" -I -B - "$sdist" "$work/from-sdist" <<'PYTHON'
import sys
import tarfile
with tarfile.open(sys.argv[1]) as archive:
    archive.extractall(sys.argv[2], filter="data")
PYTHON
    "$build_python" -I -B -m build --no-isolation --wheel --outdir "$work/rebuilt-dist" "$work/from-sdist/mcp_flightrecorder-0.5.19"
    rebuilt_wheel="$work/rebuilt-dist/mcp_flightrecorder-0.5.19-py3-none-any.whl"
    "$python" -I -B "$source_dir/tools/check_package.py" --source "$source_dir" --wheel "$rebuilt_wheel" --sdist "$sdist" --report "$work/rebuilt-report.json"
    "$python" -I -B -m venv "$work/rebuilt-env"
    "$work/rebuilt-env/bin/python" -I -B -m pip install "$rebuilt_wheel"
    "$work/rebuilt-env/bin/python" -I -B -m pip check
    cd "$work"
    "$work/rebuilt-env/bin/python" -I -B "$source_dir/tools/installed_smoke.py" --source "$source_dir" --output "$work/rebuilt-smoke" --profile core
fi
