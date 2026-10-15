"""Review of 2026-10-15: an outside audit of c8e43a4, its tests adapted. Each test fails on
cab048a for the reason its docstring gives; a test named test_control_* passes there and shows
the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round24 -v
"""
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
