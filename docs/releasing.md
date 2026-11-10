# Releasing

**Version and tag `0.5.19` · Python 3.11+ · SQLite schema 6.**

The manual [publish workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/.github/workflows/publish.yml) prepares one wheel/sdist pair and can promote that pair through TestPyPI, PyPI and GitHub. Source installation remains documented in the [README](../README.md). The existence of this workflow or a candidate artifact does not establish that a registry release has occurred.

## Modes and identity

`workflow_dispatch` accepts these inputs:

| Input | Required value |
|---|---|
| `mode` | `validate-only` (default), or explicit `staged-release` |
| `expected_commit` | The full lowercase 40-character reviewed commit SHA currently on `main` |
| `release_version` | `0.5.19` |
| `planned_tag` | `0.5.19`, without a `v` prefix |

Dispatch either mode from `main` after the required exact-commit main-push CI has succeeded. Both modes bind the checkout, exported source and package metadata to the supplied identity. `validate-only` builds, checks and stores the public candidate artifact; it does not upload to a registry, create or move a tag, create a GitHub Release, or change repository settings.

`staged-release` additionally requires an existing annotated `0.5.19` tag that peels to the exact reviewed commit. Create that tag only under the release maintainer's separate authorization. Main, tag, workflow and candidate identity are checked again after approval waits and before consequential stages. If the source identity changes, stop and review the new state.

Both `validate-only` and `staged-release` require a completed successful **main push** run of CI workflow ID `403832745` at `.github/workflows/ci.yml`, for the exact commit in `Steady-Hand-AI/mcp-flightrecorder`. Its required jobs are:

- `core / Python 3.11`
- `core / Python 3.14`
- `integration / Python 3.14`

The check binds the selected CI run ID and attempt in the manifest and verifies GitHub Actions app identity `15368`. A PR merge run, another workflow, a stale commit, a similarly named status, a missing job or unverifiable API response is not a substitute.

## One candidate, four files

The build starts from a clean tracked-source export. It runs the source/archive guard, strict Twine checks, installed core smoke outside the source tree, and a fresh safe sdist extraction/rebuild. The rebuilt wheel must have the same normalized member names and bytes, including relevant metadata, as the original wheel; archive timestamp differences are allowed. The comparison wheel is a validation output, not a replacement for the original pair.

The candidate artifact contains exactly these files in one flat directory:

| File | Purpose |
|---|---|
| `mcp_flightrecorder-0.5.19-py3-none-any.whl` | Original installable wheel |
| `mcp_flightrecorder-0.5.19.tar.gz` | Original source distribution |
| `manifest.json` | Repository, commit/tree, workflow, version/tag, CI run/attempt and package filenames, sizes and SHA256 values |
| `SHA256SUMS` | Checksums for the two distributions |

Both modes upload this candidate Actions artifact for inspection. Every later job downloads the artifact from the same workflow run and verifies its complete file set, identities, sizes and hashes before use. No stage rebuilds or replaces the candidate. Runner-local reports, environments, recordings and raw test output are not candidate or release assets.

The first `testpypi` environment gate occurs before any registry upload. The release reviewer inspects this run's candidate, verifies its manifest/checksums and authorizes those exact bytes before approving the gate. A prior validate-only run cannot authorize a later run's newly built pair. The `pypi` approval preserves the same approved pair after TestPyPI verification.

## Maintainer setup

Configure these environments with protected-branch deployment restrictions. The workflow itself admits only `main`.

| GitHub environment | Required approval | Capability |
|---|---|---|
| `testpypi` | Release reviewer | TestPyPI Trusted Publishing |
| `pypi` | Release reviewer | PyPI Trusted Publishing |
| `github-release` | No additional review gate | Finalize GitHub Release after PyPI verification |

Set up a pending or existing Trusted Publisher in each registry with these exact values:

| Setting | TestPyPI | PyPI |
|---|---|---|
| Project name | `mcp-flightrecorder` | `mcp-flightrecorder` |
| GitHub owner | `Steady-Hand-AI` | `Steady-Hand-AI` |
| Repository | `mcp-flightrecorder` | `mcp-flightrecorder` |
| Workflow filename | `publish.yml` | `publish.yml` |
| Environment | `testpypi` | `pypi` |

The registry account owner completes account-side setup in their normal browser. No registry password or API token belongs in source, workflow inputs, chat, logs or artifacts. The workflow uses OIDC; only registry jobs receive `id-token: write`. Only GitHub Release finalization receives `contents: write`. Checkout does not persist credentials. Missing account setup or permission prevents that stage from executing; local package and release-helper checks can still proceed.

## Optional validation and staged publication

Before either dispatch, integrate the approved commit through the normal repository process and verify successful main-push CI for the resulting full SHA. Replace the placeholder with that reviewed commit on `main` and verify the workflow inputs. `validate-only` is optional; it cannot bypass main-push CI for a local or branch candidate:

```sh
release_commit='<FULL_40_CHARACTER_REVIEWED_COMMIT>'
gh workflow run publish.yml --repo Steady-Hand-AI/mcp-flightrecorder --ref main \
    -f mode=validate-only -f expected_commit="$release_commit" \
    -f release_version=0.5.19 -f planned_tag=0.5.19
```

If validate-only was run, inspect its four-file candidate and results in [Actions](https://github.com/Steady-Hand-AI/mcp-flightrecorder/actions). With the reviewed main identity and required CI still valid, registry/environment setup complete, and the annotated tag correct, an authorized maintainer can request staged publication. This run builds its own candidate for approval:

```sh
release_commit='<FULL_40_CHARACTER_REVIEWED_COMMIT>'
gh workflow run publish.yml --repo Steady-Hand-AI/mcp-flightrecorder --ref main \
    -f mode=staged-release -f expected_commit="$release_commit" \
    -f release_version=0.5.19 -f planned_tag=0.5.19
```

Inspect and approve the candidate produced by this staged run at the `testpypi` gate. The ordered stages are:

1. Inspect TestPyPI's exact-version state, upload only missing approved files, then anonymously verify the full filename set, hashes, sizes and downloaded bytes. Install the downloaded wheel in a fresh environment and run the installed core smoke.
2. After successful TestPyPI verification and `pypi` approval, repeat the same checks and installation against PyPI using the same approved pair.
3. After successful PyPI verification, verify the GitHub tag, release title, curated [release notes](release-notes.md) and four asset bytes. Verify the complete draft before publishing it, then verify the published state. Release notes come from the reviewed source; they are not a fifth asset.

TestPyPI is mandatory. Registry propagation checks use bounded retries; exhausted retries leave a failed or partial state rather than a success claim. Per-version concurrency serializes release mutations without cancelling an in-progress release.

## Resume and conflicts

A partial run may upload only missing expected registry files or GitHub assets after proving every already-present expected item is byte-identical to the approved manifest. An identical completed state is success only after complete verification. Unexpected filenames, different bytes, a moved/wrong tag or conflicting release metadata stop the process.

Do not delete or overwrite a registry release, move a tag, replace public assets, or use blind skip-existing behavior to turn a conflict into a pass. Reconcile the observed state with the release maintainer. A failed check prevents downstream mutations; retain the observed partial state and rerun only under the applicable exact-commit and exact-artifact authorization.

## Local verification and limits

The [testing guide](testing.md) covers the installed package, source/archive guard and selected CI suites. Release-specific helper tests use synthetic service responses to exercise identity failures, artifact tampering, partial resume, conflicts, registry installation failure and downstream ordering without publishing anything. Fresh build checks include strict Twine validation, original/rebuilt wheel parity and installed smoke.

Run the release helper tests from the repository in a fresh environment with the declared [release toolchain](../requirements/release.txt):

```sh
release_python=$(command -v "${MFR_PYTHON:-python3}") || exit 1
release_work=$(mktemp -d /tmp/mfr-release-XXXXXX)
"$release_python" -I -B -m venv "$release_work/env"
"$release_work/env/bin/python" -I -B -m pip install -r requirements/release.txt
TMPDIR="$release_work" "$release_work/env/bin/python" -I -B \
    -m unittest discover -s tools -p 'test_release*.py' -v
```

Tests that inspect a real candidate pair can use `MFR_GUARD_SOURCE`, `MFR_GUARD_WHEEL` and `MFR_GUARD_SDIST` pointing to the pristine export and distributions built in the [testing recipe](testing.md#1-qualify-the-sdk-free-core). Keep these paths outside the checkout. The publish workflow owns release helper execution with its run identity and permissions; local tests do not provide a publication bypass.

A local helper test is not evidence of a live registry upload, an OIDC exchange, a hosted release workflow or a published GitHub Release. Report those stages only after observing the authorized run. The release workflow changes no capture, inspection, schema or optional SDK contract.
