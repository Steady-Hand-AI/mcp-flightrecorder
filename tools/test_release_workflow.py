"""Check the actual release graph and reject unsafe workflow mutations."""
import copy
from pathlib import Path
import re
import shlex
import unittest

import yaml


class ContractError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ContractError(message)


class WorkflowLoader(yaml.SafeLoader):
    pass


# GitHub uses YAML booleans true/false; YAML 1.1 must not turn the key `on` into True.
WorkflowLoader.yaml_implicit_resolvers = {
    key: [(tag, pattern) for tag, pattern in values if tag != "tag:yaml.org,2002:bool"]
    for key, values in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
WorkflowLoader.add_implicit_resolver("tag:yaml.org,2002:bool", re.compile(r"^(?:true|false)$", re.I), list("tTfF"))


def _mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node)
        require(key not in result, "duplicate workflow key")
        result[key] = loader.construct_object(value_node)
    return result


WorkflowLoader.add_constructor("tag:yaml.org,2002:map", _mapping)

PINS = {
    "actions/checkout": "fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09",
    "actions/setup-python": "ece7cb06caefa5fff74198d8649806c4678c61a1",
    "actions/upload-artifact": "ea165f8d65b6e75b540449e92b4886f43607fa02",
    "actions/download-artifact": "634f93cb2916e3fdff6788551b99b062d0335ce0",
    "pypa/gh-action-pypi-publish": "dc37677b2e1c63e2034f94d8a5b11f265b73ba33",
}
ORDER = ("build", "testpypi_publish", "testpypi_verify", "pypi_publish", "pypi_verify", "github_release")
EDGES = {
    "build": set(), "testpypi_publish": {"build"}, "testpypi_verify": {"build", "testpypi_publish"},
    "pypi_publish": {"build", "testpypi_verify"}, "pypi_verify": {"build", "pypi_publish"},
    "github_release": {"build", "pypi_verify"},
}
READ = {"contents": "read", "actions": "read", "checks": "read"}
GATE = "inputs.mode == 'staged-release'"
ARTIFACT_ID = "${{ needs.build.outputs.artifact_id }}"
ARTIFACT_DIGEST = "${{ needs.build.outputs.artifact_digest }}"
MANIFEST_DIGEST = "${{ needs.build.outputs.manifest_sha256 }}"


def needs(job):
    value = job.get("needs", [])
    return {value} if isinstance(value, str) else set(value)


def action_steps(job, action):
    return [step for step in job["steps"] if step.get("uses", "").split("@")[0] == action]


def helper_step(job, command):
    prefix = f"python -I -B tools/release.py {command}"
    steps = [step for step in job["steps"] if step.get("run", "").startswith(prefix)]
    require(len(steps) == 1, "missing or duplicated release helper")
    return steps[0]


def validate_workflow(workflow):
    require(re.search(r"\$\{\{\s*secrets\b", str(workflow)) is None, "stored credential fallback is forbidden")
    require(set(workflow["on"]) == {"workflow_dispatch"}, "publication must be manual only")
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    require(set(inputs) == {"mode", "release_version", "expected_commit", "planned_tag"}, "incorrect release inputs")
    require(inputs["mode"].get("type") == "choice" and inputs["mode"].get("default") == "validate-only"
            and inputs["mode"].get("options") == ["validate-only", "staged-release"], "unsafe mode default")
    require(all(item.get("required") is True for item in inputs.values()), "release identity must be required")
    require(inputs["release_version"].get("default") == inputs["planned_tag"].get("default") == "0.5.18",
            "incorrect version/tag defaults")
    require(workflow.get("permissions") == READ, "global permissions must be read only")
    require(workflow.get("defaults") == {"run": {"working-directory": "source"}},
            "helper commands must run in the separate source checkout")
    require(workflow.get("concurrency") == {"group": "publish-${{ inputs.release_version }}", "cancel-in-progress": False},
            "release mutations must be serialized without cancellation")
    for name, value in {"RELEASE_MODE": "mode", "RELEASE_VERSION": "release_version", "EXPECTED_COMMIT": "expected_commit",
                        "PLANNED_TAG": "planned_tag"}.items():
        require(workflow["env"].get(name) == "${{ inputs." + value + " }}", "release inputs must use environment data")
    jobs = workflow["jobs"]
    require(set(jobs) == set(ORDER), "unexpected release graph")
    for name in ORDER:
        job = jobs[name]
        require(set(job) <= {"name", "needs", "if", "runs-on", "timeout-minutes", "environment", "permissions", "steps", "outputs"},
                "unmodelled release job behavior")
        require(needs(job) == EDGES[name], "required promotion dependency missing")
        require(job.get("if") == (None if name == "build" else GATE), "incorrect explicit staged-release gate")
        expected_permissions = READ | ({"id-token": "write"} if name.endswith("_publish") else {})
        if name == "github_release":
            expected_permissions = READ | {"contents": "write"}
        require(job.get("permissions", READ) == expected_permissions, "incorrect job permission boundary")
        expected_environment = {"testpypi_publish": "testpypi", "pypi_publish": "pypi", "github_release": "github-release"}.get(name)
        require(job.get("environment") == expected_environment, "incorrect publishing environment")
        require(type(job.get("timeout-minutes")) is int and 0 < job["timeout-minutes"] <= 20, "missing job bound")
        require(len(job["steps"]) == (5 if name == "build" or name.endswith("_publish") else 4),
                "unmodelled release step")
        for step in job["steps"]:
            require("continue-on-error" not in step, "release failures must propagate")
            if "uses" in step:
                action, _, revision = step["uses"].partition("@")
                require(action in PINS and re.fullmatch(r"[0-9a-f]{40}", revision)
                        and revision == PINS[action], "unverified action identity")
            if "run" in step:
                require("${{" not in step["run"], "workflow expressions must not become shell code")
            require("${{ secrets." not in str(step), "stored credential fallback is forbidden")
        checkout = action_steps(job, "actions/checkout")
        require(len(checkout) == 1 and checkout[0].get("with", {}).get("persist-credentials") is False
                and checkout[0]["with"].get("ref") == "${{ github.sha }}"
                and checkout[0]["with"].get("path") == "source", "checkout must bind the dispatch SHA without persisted credentials")
        setup = action_steps(job, "actions/setup-python")
        require(len(setup) == 1 and setup[0].get("with") == {"python-version": "3.12"}, "incorrect release interpreter setup")
        if name == "build":
            outputs = job.get("outputs")
            require(outputs == {"manifest_sha256": "${{ steps.candidate.outputs.manifest_sha256 }}",
                                "artifact_id": "${{ steps.upload.outputs.artifact-id }}",
                                "artifact_digest": "${{ steps.upload.outputs.artifact-digest }}"}, "candidate outputs lack immutable identities")
            upload = action_steps(job, "actions/upload-artifact")
            require(len(upload) == 1 and upload[0].get("if") is None and upload[0].get("id") == "upload",
                    "both modes must retain exactly one candidate artifact")
            require(upload[0].get("with") == {
                "name": "mcp-flightrecorder-0.5.18-${{ github.run_id }}-${{ github.run_attempt }}",
                "path": "${{ runner.temp }}/release-build/bundle/", "if-no-files-found": "error",
                "include-hidden-files": False, "retention-days": 7,
            }, "candidate artifact must contain only the verified bundle directory")
            require(not action_steps(job, "actions/download-artifact") and not action_steps(job, "pypa/gh-action-pypi-publish"),
                    "build cannot publish or reuse an unbound artifact")
            helper = helper_step(job, "build")
            require(helper.get("id") == "candidate", "manifest output must come from candidate validation")
            require(helper.get("env") == {"GH_TOKEN": "${{ github.token }}"}, "incorrect build credential scope")
            require(shlex.split(helper["run"]) == ["python", "-I", "-B", "tools/release.py", "build", "--work", "$RUNNER_TEMP/release-build"],
                    "unmodelled build command")
            install = [step for step in job["steps"] if "run" in step and step is not helper]
            require(len(install) == 1 and install[0]["run"] ==
                    'python -m pip --isolated install --index-url https://pypi.org/simple -r requirements/release.txt > "$RUNNER_TEMP/release-toolchain.log" 2>&1',
                    "unmodelled toolchain installation")
            continue
        require(not action_steps(job, "actions/upload-artifact"), "promotion cannot replace the candidate artifact")
        downloads = action_steps(job, "actions/download-artifact")
        require(len(downloads) == 1 and downloads[0].get("with") == {
            "artifact-ids": ARTIFACT_ID, "path": "${{ runner.temp }}/bundle", "merge-multiple": True,
        }, "promotion must download the same-run immutable artifact ID")
        command = "github-release" if name == "github_release" else "registry-plan" if name.endswith("_publish") else "registry-verify"
        helper = helper_step(job, command)
        require(helper.get("env") == {"GH_TOKEN": "${{ github.token }}", "MANIFEST_SHA256": MANIFEST_DIGEST,
                                      "RELEASE_ARTIFACT_ID": ARTIFACT_ID, "RELEASE_ARTIFACT_DIGEST": ARTIFACT_DIGEST},
                "promotion helper must revalidate externally bound artifact identities")
        require('--manifest-sha256 "$MANIFEST_SHA256"' in helper["run"], "helper must verify the manifest digest")
        publish = action_steps(job, "pypa/gh-action-pypi-publish")
        if name.endswith("_publish"):
            require(len(publish) == 1 and publish[0].get("if") == "steps.prepare.outputs.missing_count != '0'"
                    and helper.get("id") == "prepare", "publisher must upload missing approved files only")
            settings = {"packages-dir": "registry/upload/", "skip-existing": False,
                        "verbose": False, "print-hash": True}
            if name == "testpypi_publish":
                settings["repository-url"] = "https://test.pypi.org/legacy/"
            require(publish[0].get("with") == settings, "unsafe publisher credentials or file selection")
            require('--work "$GITHUB_WORKSPACE/registry"' in helper["run"],
                    "publisher files must be outside checkout and inside the Docker workspace mount")
            require(job["steps"].index(helper) < job["steps"].index(publish[0]), "identity checks must precede publication")
        else:
            require(not publish, "verification cannot publish")
        if name != "github_release":
            require(f"--registry {name.split('_')[0]} " in helper["run"], "stage must verify its own registry")
        expected_command = ["python", "-I", "-B", "tools/release.py", command]
        if name != "github_release":
            expected_command.extend(["--registry", name.split("_")[0]])
        expected_command.extend(["--bundle", "$RUNNER_TEMP/bundle", "--manifest-sha256", "$MANIFEST_SHA256"])
        if name != "github_release":
            work = "$GITHUB_WORKSPACE/registry" if name.endswith("_publish") else "$RUNNER_TEMP/registry-verify"
            expected_command.extend(["--work", work])
        require(shlex.split(helper["run"]) == expected_command, "unmodelled promotion command")
    return workflow


def simulate_jobs(workflow, mode, outcomes=None):
    """Model the restricted job-if forms plus GitHub's implicit success dependency gate."""
    outcomes = outcomes or {}
    states, pending = {}, dict(workflow["jobs"])
    while pending:
        progressed = False
        for name, job in list(pending.items()):
            dependencies = needs(job)
            if not dependencies <= states.keys():
                continue
            gate = job.get("if")
            if gate is None:
                enabled = True
            elif gate == GATE:
                enabled = mode == "staged-release"
            elif gate == "always()":
                enabled = True
            else:
                raise ContractError("unmodelled job condition")
            successful_dependencies = all(states[dependency] == "success" for dependency in dependencies)
            states[name] = outcomes.get(name, "success") if enabled and (successful_dependencies or gate == "always()") else "skipped"
            del pending[name]
            progressed = True
        require(progressed, "cyclic or missing workflow dependency")
    return states


class ReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / ".github/workflows/publish.yml"
        cls.actual = yaml.load(path.read_text(), Loader=WorkflowLoader)

    def mutation(self, mutate, reason):
        changed = copy.deepcopy(self.actual)
        mutate(changed)
        with self.assertRaisesRegex(ContractError, reason):
            validate_workflow(changed)
        return changed

    def test_actual_workflow_contract(self):
        validate_workflow(self.actual)

    def test_validate_only_reaches_candidate_storage_and_no_mutating_job(self):
        states = simulate_jobs(self.actual, "validate-only")
        self.assertEqual({name for name, status in states.items() if status == "success"}, {"build"})
        self.assertEqual(len(action_steps(self.actual["jobs"]["build"], "actions/upload-artifact")), 1)
        self.assertEqual(simulate_jobs(self.actual, "staged-release"), dict.fromkeys(ORDER, "success"))

    def test_failed_cancelled_or_skipped_stage_blocks_all_downstream_stages(self):
        for index, name in enumerate(ORDER):
            for outcome in ("failure", "cancelled", "skipped"):
                with self.subTest(stage=name, outcome=outcome):
                    states = simulate_jobs(self.actual, "staged-release", {name: outcome})
                    self.assertEqual(states[name], outcome)
                    self.assertTrue(all(states[later] == "skipped" for later in ORDER[index + 1:]))

    def test_changed_mode_trigger_or_gate_is_refused(self):
        self.mutation(lambda w: w["on"].update(push={}), "manual only")
        self.mutation(lambda w: w["on"]["workflow_dispatch"]["inputs"]["mode"].update(default="staged-release"), "mode default")
        changed = self.mutation(lambda w: w["jobs"]["testpypi_publish"].pop("if"), "staged-release gate")
        self.assertEqual(simulate_jobs(changed, "validate-only")["testpypi_publish"], "success")
        changed = self.mutation(lambda w: w["jobs"]["github_release"].update({"if": "always()"}), "staged-release gate")
        self.assertEqual(simulate_jobs(changed, "staged-release", {"pypi_verify": "failure"})["github_release"], "success")

    def test_removed_verification_edge_would_bypass_failure_and_is_refused(self):
        changed = self.mutation(lambda w: w["jobs"]["pypi_publish"].update(needs="build"), "promotion dependency")
        states = simulate_jobs(changed, "staged-release", {"testpypi_verify": "failure"})
        self.assertEqual(states["pypi_publish"], "success")
        self.mutation(lambda w: w["jobs"]["github_release"].update(needs=["build", "pypi_publish"]), "promotion dependency")

    def test_permissions_environment_and_cancellation_mutations_are_refused(self):
        self.mutation(lambda w: w["permissions"].update({"id-token": "write"}), "global permissions")
        self.mutation(lambda w: w["jobs"]["testpypi_verify"].update(permissions=READ | {"contents": "write"}), "permission boundary")
        self.mutation(lambda w: w["jobs"]["pypi_publish"].update(environment="testpypi"), "publishing environment")
        self.mutation(lambda w: w["concurrency"].update({"cancel-in-progress": True}), "serialized")

    def test_artifact_source_identity_and_public_inventory_mutations_are_refused(self):
        def download_by_name(w):
            action_steps(w["jobs"]["pypi_publish"], "actions/download-artifact")[0]["with"] = {"name": "latest"}
        self.mutation(download_by_name, "immutable artifact ID")
        self.mutation(lambda w: action_steps(w["jobs"]["pypi_verify"], "actions/download-artifact")[0]["with"].update({"run-id": "1"}), "immutable artifact ID")
        self.mutation(lambda w: w["jobs"]["build"]["outputs"].pop("artifact_digest"), "immutable identities")
        self.mutation(lambda w: helper_step(w["jobs"]["github_release"], "github-release")["env"].update(MANIFEST_SHA256="self-reported"), "externally bound")
        self.mutation(lambda w: action_steps(w["jobs"]["build"], "actions/upload-artifact")[0]["with"].update(path="${{ runner.temp }}/**"), "bundle directory")
        self.mutation(lambda w: action_steps(w["jobs"]["build"], "actions/upload-artifact")[0].update({"if": GATE}), "both modes")

    def test_action_checkout_and_shell_injection_mutations_are_refused(self):
        self.mutation(lambda w: action_steps(w["jobs"]["build"], "actions/checkout")[0].update(uses="actions/checkout@v5"), "action identity")
        self.mutation(lambda w: action_steps(w["jobs"]["build"], "actions/checkout")[0]["with"].update(ref="${{ inputs.expected_commit }}"), "dispatch SHA")
        self.mutation(lambda w: action_steps(w["jobs"]["pypi_publish"], "actions/checkout")[0]["with"].update({"persist-credentials": True}), "dispatch SHA")
        self.mutation(lambda w: helper_step(w["jobs"]["build"], "build").update(run="python -I -B tools/release.py build --version ${{ inputs.release_version }}"), "shell code")

    def test_publisher_fallback_skip_and_failure_suppression_mutations_are_refused(self):
        self.mutation(lambda w: action_steps(w["jobs"]["pypi_publish"], "pypa/gh-action-pypi-publish")[0]["with"].update({"skip-existing": True}), "unsafe publisher")
        self.mutation(lambda w: action_steps(w["jobs"]["pypi_publish"], "pypa/gh-action-pypi-publish")[0]["with"].update(password="${{ secrets.PYPI_TOKEN }}"), "credential fallback")
        self.mutation(lambda w: helper_step(w["jobs"]["pypi_verify"], "registry-verify").update({"continue-on-error": True}), "failures must propagate")
        self.mutation(lambda w: helper_step(w["jobs"]["pypi_verify"], "registry-verify").update(run='python -I -B tools/release.py registry-verify --registry testpypi --manifest-sha256 "$MANIFEST_SHA256"'), "own registry")

    def test_publisher_can_read_the_verified_staging_directory_in_docker(self):
        for name in ("testpypi_publish", "pypi_publish"):
            job = self.actual["jobs"][name]
            command = helper_step(job, "registry-plan")["run"]
            self.assertIn('--work "$GITHUB_WORKSPACE/registry"', command)
            self.assertEqual(action_steps(job, "pypa/gh-action-pypi-publish")[0]["with"]["packages-dir"], "registry/upload/")
        self.mutation(lambda w: helper_step(w["jobs"]["pypi_publish"], "registry-plan").update(
            run='python -I -B tools/release.py registry-plan --registry pypi --bundle "$RUNNER_TEMP/bundle" --manifest-sha256 "$MANIFEST_SHA256" --work "$RUNNER_TEMP/registry"'), "Docker workspace mount")

    def test_duplicate_yaml_keys_are_not_silently_accepted(self):
        with self.assertRaisesRegex(ContractError, "duplicate workflow key"):
            yaml.load("permissions:\n  contents: read\n  contents: write\n", Loader=WorkflowLoader)

    def test_unknown_commands_and_job_failure_masking_are_refused(self):
        self.mutation(lambda w: w["jobs"]["build"]["steps"].append({"run": "unreviewed-command"}), "unmodelled release step")
        self.mutation(lambda w: helper_step(w["jobs"]["build"], "build").update(
            run='python -I -B tools/release.py build --work "$RUNNER_TEMP/release-build"; unreviewed-command'), "unmodelled build command")
        self.mutation(lambda w: w["jobs"]["pypi_verify"].update({"continue-on-error": True}), "unmodelled release job")


if __name__ == "__main__":
    unittest.main(verbosity=2)
