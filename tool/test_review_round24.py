"""Review of 2026-10-15: an outside audit of c8e43a4 (its tests, adapted), and what checking it
found. Each test fails on cab048a for the reason its docstring gives; a test named
test_control_* passes there and shows the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round24 -v
"""
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import unittest

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))


# ------------------------------------------------------------------ testgrid_config.sh: the configs to build

# The commands testgrid_config.sh runs, stood in: "$python" -I testgrid_config.py STEP ..., git
# and docker. Each call is logged (python's by its step, git's and docker's by their command);
# FAKE_CASE says what recipes prints and which step fails.
FAKE = r'''import json, os, signal, sys

name, args, case = sys.argv[1], sys.argv[2:], os.environ["FAKE_CASE"]
if name == "python":
    step = args[2]
elif name == "git":
    step = "git " + next(a for a in args if a in ("init", "remote", "config", "sparse-checkout", "fetch", "checkout", "log"))
else:
    step = "docker " + args[0]
with open(os.environ["FAKE_CALLS"], "a") as f:
    f.write(step + "\n")
if step == "uploads" and case == "uploads fails":
    sys.exit(1)
elif step == "recipes":
    if case in ("one config", "extract fails", "recipes fails part-way", "recipes is killed"):
        print("aaaaaaaaaaaa-bbbbbbbbbbbb " + "c" * 40, flush=True)
    if case in ("recipes fails", "recipes fails part-way"):
        sys.exit(1)
    if case == "recipes is killed":
        os.kill(os.getpid(), signal.SIGKILL)
elif step == "job":
    sys.stdout.write("registry.invalid/configurator:v1\0/configurator\0--oneshot\0")
elif step == "extract":
    if case == "extract fails":
        sys.exit("no dashboards")
    print("{}")
elif step == "merge":
    if case == "merge fails":
        sys.exit(1)
    with open(args[args.index("--failed") + 1]) as f:
        failed = [line.split("\t")[0] for line in f]
    print(json.dumps({"built": [os.path.basename(a) for a in args[6:]], "failed": failed}))
elif step == "git init":
    os.makedirs(args[-1])
elif step == "git log":
    print(1792000000)
elif step == "docker image":
    print("registry.invalid/configurator@sha256:" + "0" * 64)
elif step == "docker run":
    print("dashboards: []")
'''
SETUP = ["uploads", "recipes", "git init", "git remote", "git config", "git sparse-checkout"]
BUILD = ["git fetch", "git checkout", "git log", "job", "docker pull", "docker image", "docker run", "extract"]
EARLIER = '{"written by": "an earlier run"}\n'


class ConfigsToBuild(unittest.TestCase):
    """tool/testgrid_config.sh run whole, with python (testgrid_config.py's steps), git and docker
    stood in (FAKE). bash, set -e, timeout and the files are real."""

    def run_case(self, case):
        """(the script's run, as subprocess.run returns it; the file it was to write; the steps it ran)"""
        with tempfile.TemporaryDirectory() as tmp:
            fake = os.path.join(tmp, "fake.py")
            with open(fake, "w") as f:
                f.write(FAKE)
            bindir = os.path.join(tmp, "bin")
            os.mkdir(bindir)
            for name in ("python", "git", "docker"):
                with open(os.path.join(bindir, name), "w") as f:
                    f.write(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -S {shlex.quote(fake)} {name} "$@"\n')
                os.chmod(os.path.join(bindir, name), 0o755)
            out, calls = os.path.join(tmp, "testgrid-config.json"), os.path.join(tmp, "calls")
            with open(out, "w") as f:
                f.write(EARLIER)
            env = dict(os.environ, PATH=bindir + os.pathsep + os.environ["PATH"], PYTHON=os.path.join(bindir, "python"),
                       CONFIG_SECONDS="480", FAKE_CASE=case, FAKE_CALLS=calls)
            done = subprocess.run(["bash", os.path.join(HERE, "testgrid_config.sh"), out], env=env, capture_output=True,
                                  text=True, timeout=120)
            with open(out) as f, open(calls) as g:
                return done, f.read(), g.read().split("\n")[:-1]

    def check_stops(self, case):
        done, written, steps = self.run_case(case)
        self.assertNotEqual(done.returncode, 0, done.stdout + done.stderr)  # fails: 0
        self.assertEqual(written, EARLIER)  # fails: replaced by what it merged
        self.assertEqual(steps, ["uploads", "recipes"])  # fails: it went on to git init ... merge

    def test_recipes_failing_stops_the_script(self):
        """testgrid_config.sh:36 read the configs to build as `mapfile -t recipes < <("$python" ...
        recipes ...)`. set -e does not see a command in a process substitution fail, so when
        `recipes` failed before printing a line, the script went on with an empty list: it built
        nothing, wrote the runs it had listed with none of their configs ("built 0 of 0
        configs"), and exited 0, so the step showed no failure."""
        self.check_stops("recipes fails")

    def test_recipes_failing_part_way_stops_the_script(self):
        """The same, with the list cut short: the configs printed before the failure were built,
        the rest were left out as if there were no more ("built 1 of 1 configs")."""
        self.check_stops("recipes fails part-way")

    def test_recipes_killed_stops_the_script(self):
        """The same, with `recipes` killed (SIGKILL, as the kernel's OOM killer does)."""
        self.check_stops("recipes is killed")

    def test_control_uploads_failing_stops_the_script(self):
        done, written, steps = self.run_case("uploads fails")
        self.assertNotEqual(done.returncode, 0)
        self.assertEqual((written, steps), (EARLIER, ["uploads"]))

    def test_control_nothing_to_build_still_writes_the_config(self):
        done, written, steps = self.run_case("no configs")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("built 0 of 0 configs", done.stdout)
        self.assertEqual(json.loads(written), {"built": [], "failed": []})
        self.assertEqual(steps, SETUP + ["merge"])

    def test_control_one_config_is_built(self):
        done, written, steps = self.run_case("one config")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("built 1 of 1 configs", done.stdout)
        self.assertEqual(json.loads(written), {"built": ["extract-aaaaaaaaaaaa-bbbbbbbbbbbb.json"], "failed": []})
        self.assertEqual(steps, SETUP + BUILD + ["merge"])

    def test_control_a_config_that_cannot_be_built_is_reported(self):
        done, written, steps = self.run_case("extract fails")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("built 0 of 1 configs of the upload job's runs (1 could not be built)", done.stdout)
        self.assertIn("(aaaaaaaaaaaa-bbbbbbbbbbbb) cannot be built: extract: no dashboards", done.stderr)
        self.assertEqual(json.loads(written), {"built": [], "failed": ["aaaaaaaaaaaa-bbbbbbbbbbbb"]})
        self.assertEqual(steps, SETUP + BUILD + ["merge"])

    def test_control_merge_failing_keeps_the_earlier_config(self):
        done, written, steps = self.run_case("merge fails")
        self.assertNotEqual(done.returncode, 0)
        self.assertEqual((written, steps), (EARLIER, SETUP + ["merge"]))


# ------------------------------------------------------------------ the workflows' shells, and follow.sh's checks

WORKFLOWS = os.path.join(HERE, "..", ".github", "workflows")


def workflow(name):
    with open(os.path.join(WORKFLOWS, name)) as f:
        return yaml.safe_load(f)


def github_shell(flow, job, step):
    """The command GitHub runs a step's script with on Linux (workflow syntax, jobs.<job_id>.steps[*].shell):
    bash -e when no shell is given, bash --noprofile --norc -eo pipefail for shell: bash. The shell
    is the step's, else that of its job's defaults.run, else the workflow's: a job's defaults.run
    replaces the workflow's whole, not key by key (actions/runner, src/Runner.Worker/JobExtension.cs)."""
    run = (job.get("defaults") or {}).get("run")
    if run is None:
        run = (flow.get("defaults") or {}).get("run") or {}
    shell = step.get("shell") or run.get("shell")
    return {None: ["bash", "-e"], "bash": ["bash", "--noprofile", "--norc", "-eo", "pipefail"]}[shell]


def run_step(flow, job, step, cwd, env):
    """Run a step's script as GitHub does: written to a file, run by its shell, in `cwd`."""
    with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as f:
        f.write(step["run"])
    try:
        return subprocess.run([*github_shell(flow, job, step), f.name], cwd=cwd, env=dict(os.environ, **env),
                              capture_output=True, text=True, timeout=120)
    finally:
        os.remove(f.name)


def step_of(flow, job, test):
    return next(s for s in flow["jobs"][job]["steps"] if test(s))


# (an oracle checker stood in: it passes what it reads on, as for a dump that wrote all it had)
PASS_ON = "import shutil, sys\nshutil.copyfileobj(sys.stdin, sys.stdout)\n"


class Shells(unittest.TestCase):
    def test_every_run_step_runs_under_pipefail(self):
        """Without `shell: bash`, GitHub runs a step's script with bash -e, without -o pipefail
        (with it: bash --noprofile --norc -eo pipefail), and no workflow set one: in each of
        their 25 run steps, a pipeline failed only when its last command did."""
        steps = 0
        for name in sorted(n for n in os.listdir(WORKFLOWS) if n.endswith((".yml", ".yaml"))):
            flow = workflow(name)
            for job_id, job in flow["jobs"].items():
                for step in job.get("steps", []):
                    if "run" in step:
                        steps += 1
                        label = step.get("name") or step.get("id") or step["run"].split("\n")[0]
                        with self.subTest(workflow=name, job=job_id, step=label):
                            self.assertIn("pipefail", github_shell(flow, job, step))  # fails: bash -e
        self.assertGreater(steps, 0)

    def dump_step(self, title, dump):
        """The oracle job's step `title`, with the Go dumps it reads (schemadump, unidump) stood in
        by `dump` (a shell script), and the checkers they are piped to by PASS_ON."""
        flow = workflow("oracle.yml")
        step = step_of(flow, "oracle", lambda s: s.get("name") == title)
        with tempfile.TemporaryDirectory() as tmp:
            tool = os.path.join(tmp, "tool")
            os.makedirs(os.path.join(tool, "oracle"))
            os.makedirs(os.path.join(tmp, "oracle"))
            for checker in ("gotypes.py", "gounicode.py"):
                with open(os.path.join(tool, "oracle", checker), "w") as f:
                    f.write(PASS_ON)
            with open(os.path.join(tool, "testgrid_types.json"), "w") as f:
                f.write("{}\n")
            for name in ("schemadump", "unidump"):
                with open(os.path.join(tmp, "oracle", name), "w") as f:
                    f.write(dump)
                os.chmod(os.path.join(tmp, "oracle", name), 0o755)
            return run_step(flow, flow["jobs"]["oracle"], step, tool, {"RUNNER_TEMP": tmp})

    def test_the_go_types_step_fails_when_schemadump_fails(self):
        """oracle.yml:67: `schemadump | python3 -I oracle/gotypes.py > types.json`, then cmp: under
        bash -e only the pipeline's last command counted, so a schemadump that failed after writing
        its types (a panic as it exits, say) passed the step."""
        done = self.dump_step("The Go types are the ones testgrid_types.json holds", "#!/bin/sh\necho '{}'\nexit 2\n")
        self.assertNotEqual(done.returncode, 0, done.stdout + done.stderr)  # fails: 0

    def test_the_unicode_step_fails_when_unidump_fails(self):
        """oracle.yml:72: `unidump | python3 -I oracle/gounicode.py check ...`: the same."""
        done = self.dump_step("Go's unicode tables are the ones go_unicode.json.gz holds", "#!/bin/sh\necho '{}'\nexit 2\n")
        self.assertNotEqual(done.returncode, 0, done.stdout + done.stderr)  # fails: 0

    def test_control_the_dump_steps_pass_when_the_dumps_do(self):
        for title in ("The Go types are the ones testgrid_types.json holds",
                      "Go's unicode tables are the ones go_unicode.json.gz holds"):
            with self.subTest(step=title):
                done = self.dump_step(title, "#!/bin/sh\necho '{}'\n")
                self.assertEqual(done.returncode, 0, done.stdout + done.stderr)

    def pinned_step(self, job_id, pinned):
        """(the job's step `pinned`, run on the pinned.json `pinned`: its exit status, and what it
        wrote to $GITHUB_OUTPUT, None if nothing)"""
        flow = workflow("oracle.yml")
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "oracle"))
            with open(os.path.join(tmp, "oracle", "pinned.json"), "w") as f:
                f.write(pinned)
            out = os.path.join(tmp, "outputs")
            done = run_step(flow, flow["jobs"][job_id], step_of(flow, job_id, lambda s: s.get("id") == "pinned"), tmp,
                            {"GITHUB_OUTPUT": out})
            if not os.path.exists(out):
                return done, None
            with open(out) as f:
                return done, f.read()

    def test_the_go_version_steps_fail_without_one(self):
        """oracle.yml:45 and :136 wrote `echo "go=$(python3 ...)" >> "$GITHUB_OUTPUT"`: the step's
        status was echo's, so a pinned.json without "go" (or with an empty one) wrote go= and
        passed, and setup-go, given no version, used the Go the runner has."""
        for job_id in ("oracle", "follow"):
            for pinned in ('{"testgrid_commit": "f15bfa4808a4"}\n', '{"go": ""}\n'):
                with self.subTest(job=job_id, pinned=pinned):
                    done, written = self.pinned_step(job_id, pinned)
                    self.assertNotEqual(done.returncode, 0, done.stdout + done.stderr)  # fails: 0
                    self.assertIsNone(written)  # fails: "go=\n"

    def test_control_the_go_version_steps_write_the_pinned_go(self):
        for job_id in ("oracle", "follow"):
            with self.subTest(job=job_id):
                done, written = self.pinned_step(job_id, '{"go": "1.26.5"}\n')
                self.assertEqual(done.returncode, 0, done.stderr)
                self.assertEqual(written, "go=1.26.5\n")


class FollowChecks(unittest.TestCase):
    """follow.sh's checks of the Go types and of Go's unicode tables (types, unicode), run by its
    own check() under its own shell options, with the dumps stood in and the checkers by PASS_ON."""

    def check(self, name, dump):
        """(what check() reports failed after running the check `name`: [] or [name])"""
        with open(os.path.join(HERE, "oracle", "follow.sh")) as f:
            script = f.read()
        options = "\n".join(re.findall(r"^set .*$", script.split('\ncheck "')[0], re.M))  # (those before the checks)
        functions = [re.search(rf"^{f}\(\) \{{.*?\}}$", script, re.M | re.S).group(0) for f in ("check", name)]
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "oracle"))
            for checker in ("gotypes.py", "gounicode.py"):
                with open(os.path.join(tmp, "oracle", checker), "w") as f:
                    f.write(PASS_ON)
            with open(os.path.join(tmp, "testgrid_types.json"), "w") as f:
                f.write('{"about": "pinned", "types": 1}\n')
            fake = os.path.join(tmp, "dump")
            with open(fake, "w") as f:
                f.write(dump)
            os.chmod(fake, 0o755)
            done = subprocess.run(["bash", "-c", "\n".join([
                options, f"work={shlex.quote(tmp)}", f"SCHEMADUMP={shlex.quote(fake)}",
                f"UNIDUMP={shlex.quote(fake)}", "failed=()", *functions, f"check {name} {name}",
                'echo "failed: ${failed[*]}"'])], cwd=tmp, capture_output=True, text=True, timeout=120)
        self.assertEqual(done.returncode, 0, done.stderr)
        return done.stdout.split("\n")[-2].split()[1:]

    def test_the_types_check_fails_when_schemadump_fails(self):
        """follow.sh:51-56: types() ran the dump, then the comparison, and check() runs it as an if's
        condition, where set -e does not apply: a schemadump that failed after writing the types
        went on to the comparison, whose status was the function's, so the check passed and the
        summary did not list it as failed. When nothing else stopped follow.sh, it went on to
        propose the commit and dumped the types again outside a condition (:107): only a dump
        that failed there too stopped it."""
        self.assertEqual(self.check("types", "#!/bin/sh\necho '{\"about\": \"new\", \"types\": 1}'\nexit 2\n"),
                         ["types"])  # fails: []

    def test_control_the_types_check_passes_on_the_same_types(self):
        self.assertEqual(self.check("types", "#!/bin/sh\necho '{\"about\": \"new\", \"types\": 1}'\n"), [])

    def test_control_the_types_check_fails_on_other_types(self):
        self.assertEqual(self.check("types", "#!/bin/sh\necho '{\"about\": \"new\", \"types\": 2}'\n"), ["types"])

    def test_control_the_unicode_check_fails_when_unidump_fails(self):
        # (one pipeline: under pipefail, its status is the function's, condition or not)
        self.assertEqual(self.check("unicode", "#!/bin/sh\necho '{}'\nexit 2\n"), ["unicode"])


if __name__ == "__main__":
    unittest.main()
