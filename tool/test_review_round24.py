"""Review of 2026-10-15: an outside audit of c8e43a4 (its tests, adapted), and what checking it
found. Each test fails on cab048a for the reason its docstring gives; a test named
test_control_* passes there and shows the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round24 -v
"""
import contextlib
import io
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zlib
from unittest import mock

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected here)
import test_testgrid_config as ttc  # noqa: E402
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import HOUR, ArchiveWorld, obj  # noqa: E402


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


# ------------------------------------------------------------------ the commit step: files GitHub would refuse

LIMIT = 95 << 20  # (the commit step's limit; GitHub blocks files over 100 MiB)
OLD = "archive/runs/2026-10/2026-10-14T000000Z/job/2110000000000000000"  # (archived by an earlier run)
NEW = "archive/runs/2026-10/2026-10-15T000000Z/job/2110558713260544000"  # (archived by this run)


class CommitStep(unittest.TestCase):
    """The archive job's commit step, run as GitHub runs it, in a local repository whose origin
    is a bare one, with the repository's .gitignore: git, the files, the push and the bundle
    are real."""

    def setUp(self):
        self.enterContext(mock.patch.dict(os.environ))
        for name in [n for n in os.environ if n.startswith("GIT_")]:
            del os.environ[name]  # (a GIT_DIR, say, would send these git commands elsewhere)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        with open(os.path.join(self.tmp, "gitconfig"), "w") as f:
            f.write("[user]\n\tname = test\n\temail = test@example.com\n")
        self.env = {"GIT_CONFIG_GLOBAL": os.path.join(self.tmp, "gitconfig"), "GIT_CONFIG_NOSYSTEM": "1",
                    "XDG_CONFIG_HOME": self.tmp}  # (and not the user's git/ignore or git/attributes)
        self.origin, self.repo = os.path.join(self.tmp, "origin.git"), os.path.join(self.tmp, "repo")
        self.git(self.tmp, "init", "-q", "--bare", "-b", "main", self.origin)
        self.git(self.tmp, "init", "-q", "-b", "main", self.repo)
        shutil.copy(os.path.join(HERE, "..", ".gitignore"), self.repo)
        self.write(f"{OLD}/build-log.txt.gz", b"the log as archived")
        self.write(f"{OLD}/meta.json", b'{"log": "as archived"}\n')
        for page in ("INDEX.md", "index.json", "weeks/2026-W42.md", "weeks/2026-W42.json", "charts/trend-light.svg",
                     "state.json"):
            self.write(f"archive/{page}", b"as last committed\n")
        self.git(self.repo, "add", "-A")
        self.git(self.repo, "commit", "-q", "-m", "base")
        self.git(self.repo, "remote", "add", "origin", self.origin)
        self.git(self.repo, "push", "-q", "-u", "origin", "main")
        self.base = self.git(self.origin, "rev-parse", "main")

    def git(self, cwd, *args):
        return subprocess.run(["git", *args], cwd=cwd, env=dict(os.environ, **self.env), check=True, capture_output=True,
                              text=True).stdout.strip()

    def write(self, rel, data=b"", size=None):
        path = os.path.join(self.repo, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
            if size is not None:
                f.truncate(size)  # (sparse: no disk written)

    def read(self, rel):
        with open(os.path.join(self.repo, rel), "rb") as f:
            return f.read()

    def commit(self):
        flow = workflow("red-build-logs.yml")
        return run_step(flow, flow["jobs"]["archive"], step_of(flow, "archive", lambda s: s.get("id") == "commit"),
                        self.repo, dict(self.env, GITHUB_TOKEN="not-a-token", RUNNER_TEMP=self.tmp,
                                        GITHUB_REF_NAME="main", GITHUB_OUTPUT=os.path.join(self.tmp, "outputs")))

    def pushed(self):
        """{path: size} of the files on origin's main."""
        listing = self.git(self.origin, "ls-tree", "-r", "-l", "main")
        return {line.split("\t")[1]: int(line.split()[3]) for line in listing.splitlines()}

    def check_refused(self, done):
        self.assertNotEqual(done.returncode, 0, done.stdout + done.stderr)  # fails: 0
        self.assertEqual(self.git(self.origin, "rev-parse", "main"), self.base)  # fails: a commit pushed

    def test_an_archived_log_grown_too_large_stops_the_commit(self):
        """red-build-logs.yml:171-181: a stored log over 95 MiB was put back as last committed
        (git checkout HEAD), while its meta.json, which describes the new copy, was committed:
        that left the archive with a log its meta.json does not describe (cross_check: "local
        md5 ... != meta ..."). Nothing is committed now, and both files stay as the fetch wrote
        them."""
        self.write(f"{OLD}/build-log.txt.gz", size=LIMIT + 1)
        self.write(f"{OLD}/meta.json", b'{"log": "fetched again"}\n')
        done = self.commit()
        self.check_refused(done)
        self.assertIn(f"::error::{OLD}/build-log.txt.gz ({LIMIT + 1} bytes) is larger than 95 MiB", done.stdout)
        self.assertEqual(os.path.getsize(os.path.join(self.repo, OLD, "build-log.txt.gz")), LIMIT + 1)
        self.assertEqual(self.read(f"{OLD}/meta.json"), b'{"log": "fetched again"}\n')

    def test_new_logs_too_large_stop_the_commit(self):
        """The same for the builds this run archived: their logs were deleted and their meta.json
        committed, so the archive held builds whose meta.json lists a log that is not there
        (cross_check: "cannot verify: FileNotFoundError"). Each file too large is named."""
        other = NEW[:-1] + "1"
        for build in (NEW, other):
            self.write(f"{build}/build-log.txt.gz", size=LIMIT + 1)
            self.write(f"{build}/meta.json", b'{"log": "new"}\n')
        done = self.commit()
        self.check_refused(done)
        for build in (NEW, other):
            self.assertIn(f"::error::{build}/build-log.txt.gz ({LIMIT + 1} bytes) is larger than 95 MiB", done.stdout)
            self.assertEqual(os.path.getsize(os.path.join(self.repo, build, "build-log.txt.gz")), LIMIT + 1)

    def test_a_temporary_file_git_ignores_does_not_stop_the_commit(self):
        """red-build-logs.yml:182: `find archive -type f -size +95M` also found the temporary files
        of a fetch cut off part-way (.<name>.<random>.part, which .gitignore leaves out: a
        download is written whole, gzipped, before it is split into parts), so a fetch that
        timed out during a large download stopped the commit of everything else it archived."""
        self.write(f"{OLD}/.build-log.txt.gz.k2j4h5.part", size=LIMIT + 1)
        self.write("archive/weeks/.2026-W42.json.p9q8r7.part", size=LIMIT + 1)
        self.write(f"{NEW}/build-log.txt.gz", b"a new log")
        self.write(f"{NEW}/meta.json", b'{"log": "new"}\n')
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)  # fails: 1, larger than 95 MiB
        self.assertNotIn("::warning::", done.stdout)
        files = self.pushed()
        self.assertIn(f"{NEW}/build-log.txt.gz", files)
        self.assertFalse([name for name in files if name.endswith(".part")])
        self.assertTrue(os.path.exists(os.path.join(self.repo, OLD, ".build-log.txt.gz.k2j4h5.part")))  # (the next fetch's)

    def test_a_page_too_large_puts_back_only_itself(self):
        """red-build-logs.yml:180 gave `git checkout -q HEAD --` the page's name as a pathspec, so a page
        named like a glob (the fetch names none so) put back the pages it matched too, and this
        run's copy of one was lost."""
        self.write("archive/weeks/2026-W4?.md", b"as last committed\n")
        self.git(self.repo, "add", "-A")
        self.git(self.repo, "commit", "-q", "-m", "a page named like a glob")
        self.git(self.repo, "push", "-q")
        self.write("archive/weeks/2026-W4?.md", size=LIMIT + 1)
        self.write("archive/weeks/2026-W42.md", b"this run's\n")
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        files = self.pushed()
        self.assertEqual(files["archive/weeks/2026-W4?.md"], len(b"as last committed\n"))
        self.assertEqual(files["archive/weeks/2026-W42.md"], len(b"this run's\n"))  # fails: 18, put back too

    def test_a_new_page_too_large_leaves_out_only_itself(self):
        """The same for a page this run made: `git checkout -q HEAD --` put back the page it matched,
        and kept the new one, too large, so the commit stopped."""
        self.write("archive/weeks/2026-W4?.json", size=LIMIT + 1)
        self.write("archive/weeks/2026-W42.json", b"this run's\n")
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)  # fails: 1
        files = self.pushed()
        self.assertNotIn("archive/weeks/2026-W4?.json", files)
        self.assertEqual(files.get("archive/weeks/2026-W42.json"), len(b"this run's\n"))

    def test_control_a_page_too_large_keeps_the_version_last_committed(self):
        # (INDEX.md, index.json, a week's page and a chart: every run writes them anew; one this run made is left out)
        for page in ("INDEX.md", "index.json", "weeks/2026-W43.json", "charts/trend-light.svg"):
            self.write(f"archive/{page}", size=LIMIT + 1)
        self.write(f"{NEW}/build-log.txt.gz", b"a new log")
        self.write(f"{NEW}/meta.json", b'{"log": "new"}\n')
        self.write("archive/state.json", b"this run's\n")
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(done.stdout.count("::warning::"), 4, done.stdout)
        files = self.pushed()
        last = len(b"as last committed\n")
        self.assertEqual((files["archive/INDEX.md"], files["archive/index.json"], files["archive/charts/trend-light.svg"]),
                         (last, last, last))
        self.assertNotIn("archive/weeks/2026-W43.json", files)
        self.assertEqual(files["archive/state.json"], len(b"this run's\n"))
        self.assertIn(f"{NEW}/build-log.txt.gz", files)
        self.assertEqual(self.read("archive/INDEX.md"), b"as last committed\n")
        self.assertFalse(os.path.exists(os.path.join(self.repo, "archive/weeks/2026-W43.json")))

    def test_control_the_state_too_large_stops_the_commit(self):
        self.write("archive/state.json", size=LIMIT + 1)
        self.check_refused(self.commit())

    def test_control_a_new_build_is_pushed(self):
        self.write(f"{NEW}/build-log.txt.gz", b"a new log")
        self.write(f"{NEW}/meta.json", b'{"log": "new"}\n')
        self.write("archive/INDEX.md", b"this run's\n")
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertNotIn("::warning::", done.stdout)
        files = self.pushed()
        self.assertEqual((files[f"{NEW}/build-log.txt.gz"], files[f"{NEW}/meta.json"], files["archive/INDEX.md"]),
                         (len(b"a new log"), len(b'{"log": "new"}\n'), len(b"this run's\n")))
        self.assertEqual(self.git(self.repo, "status", "--porcelain"), "")

    def test_control_files_removed_are_committed_removed(self):
        # (a log now stored in parts, and a week no longer listed: a file removed has no size to check)
        os.remove(os.path.join(self.repo, OLD, "build-log.txt.gz"))
        self.write(f"{OLD}/build-log.txt.gz.000", b"the log, in parts")
        self.write(f"{OLD}/meta.json", b'{"log": "in parts"}\n')
        os.remove(os.path.join(self.repo, "archive/weeks/2026-W42.md"))
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        files = self.pushed()
        self.assertIn(f"{OLD}/build-log.txt.gz.000", files)
        self.assertNotIn(f"{OLD}/build-log.txt.gz", files)
        self.assertNotIn("archive/weeks/2026-W42.md", files)

    def test_control_a_log_of_95_mib_is_committed(self):
        self.write(f"{NEW}/build-log.txt.gz", size=LIMIT)
        self.write(f"{NEW}/meta.json", b'{"log": "new"}\n')
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(self.pushed()[f"{NEW}/build-log.txt.gz"], LIMIT)

    def test_control_a_build_folder_without_meta_json_is_left_out(self):
        # (a download cut off part-way: removed first, so its size stops nothing)
        cut = NEW[:-1] + "1"
        self.write(f"{cut}/build-log.txt.gz", size=LIMIT + 1)
        self.write(f"{NEW}/build-log.txt.gz", b"a new log")
        self.write(f"{NEW}/meta.json", b'{"log": "new"}\n')
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.repo, cut)))
        self.assertIn(f"{NEW}/meta.json", self.pushed())

    def test_control_a_push_refused_leaves_a_bundle(self):
        hook = os.path.join(self.origin, "hooks", "pre-receive")
        with open(hook, "w") as f:
            f.write("#!/bin/sh\nexit 1\n")
        os.chmod(hook, 0o755)
        self.write(f"{NEW}/build-log.txt.gz", b"a new log")
        self.write(f"{NEW}/meta.json", b'{"log": "new"}\n')
        self.check_refused(self.commit())
        bundle = os.path.join(self.tmp, "unpushed-archive.bundle")
        self.git(self.repo, "bundle", "verify", bundle)
        with open(os.path.join(self.tmp, "outputs")) as f:
            self.assertEqual(f.read(), f"bundle={bundle}\n")

    def test_control_nothing_new_is_not_committed(self):
        done = self.commit()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("Nothing new to commit.", done.stdout)
        self.assertEqual(self.git(self.origin, "rev-parse", "main"), self.base)


# ------------------------------------------------------------------ INDEX.md: the runs table

class RunsTable(unittest.TestCase):
    def runs_section(self):
        """INDEX.md's section on the recent runs, for an archive of one run."""
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            frl.write_json(os.path.join(root, rel, "run.json"), {"run": "x", "started": time.time(), "outcomes": {}})
            frl.write_index(root, time.time())
            with open(os.path.join(root, "INDEX.md")) as f:
                return f.read().split("## Runs in the last")[1].split("\n## ")[0]

    def test_the_fetchs_result_is_called_the_collection_result(self):
        """INDEX.md's runs table headed the fetch's own result "Result", which reads as the run's
        verdict on the archive; the verification that follows the fetch (cross_check.py) is not
        in it."""
        runs = self.runs_section()
        self.assertIn("| Run (UTC) | New builds | Updated | Collection result |", runs)  # fails: Result
        self.assertIn("Collection result is how the run's fetch went:", runs)
        note = runs.split("\n\n")[1]
        for value in ("ok", "interrupted", "problems", "no run report"):  # (each the cell can show)
            self.assertRegex(note, rf"\b{value}\b")
        self.assertIn("It does not include the verification that follows the fetch "
                      "([cross_check.py](../tool/cross_check.py))", note)
        # (the note, a blank line, then the table: its header, which may gain columns, the line under it, a row)
        table = re.search(r"\.\n\n(\| Run \(UTC\) \| New builds \| Updated \| Collection result \|.*)"
                          r"\n(\|---\|.*)\n(\| \[.*)", runs)
        self.assertIsNotNone(table, runs)
        header, line, row = table.groups()
        self.assertEqual(line.count("|"), header.count("|"))  # (GFM shows no table when they differ)
        self.assertEqual(row.count("|"), header.count("|"))
        self.assertIn(" | 0 | 0 | ok |", runs)

    def test_each_runs_verification_is_shown(self):
        """INDEX.md showed no run's verification (cab048a kept none: see VerificationRecord)."""
        now = time.time()
        # (what each run folder holds as verification.json, and the cell expected for it; "{v}" is the file)
        cases = [(None, "not recorded"), ("not JSON", "unreadable"), ("null", "unreadable"),
                 ({"outcome": "ok"}, "[ok]({v})"), ({"outcome": "problems", "problem_count": 3}, "[3 problems]({v})"),
                 ({"outcome": "problems", "problem_count": 1}, "[1 problem]({v})"),
                 ({"outcome": "problems"}, "[problems]({v})"), ({"outcome": "problems", "problem_count": True},
                                                                "[problems]({v})"),
                 ({"outcome": "problems", "problem_count": 0}, "[problems]({v})"),
                 ({"outcome": "incomplete"}, "[incomplete]({v})"),
                 ({"outcome": "timed out", "problem_count": 1}, "[timed out]({v})"),
                 ({"outcome": "interrupted"}, "[interrupted]({v})"), ({"outcome": "stopped"}, "[stopped]({v})"),
                 ({"outcome": "passed"}, "unreadable"), ("elsewhere.json", "unreadable"), ("nowhere", "unreadable"),
                 ("a folder", "unreadable")]
        with tempfile.TemporaryDirectory() as root:
            want = {}
            for i, (v, cell) in enumerate(cases):
                rel = frl.new_run_dir(root, now - (len(cases) + 1 - i) * 3600)[1]
                frl.write_json(os.path.join(root, rel, "run.json"), {"run": "x", "started": now, "outcomes": {}})
                path = os.path.join(root, rel, "verification.json")
                if v in ("elsewhere.json", "nowhere"):
                    frl.write_json(os.path.join(root, rel, "elsewhere.json"), {"outcome": "ok"})
                    os.symlink(v, path)  # (a link, to a file or to nothing: never followed)
                elif v == "a folder":
                    os.mkdir(path)
                elif v is not None:
                    with open(path, "w") as f:
                        f.write(v if isinstance(v, str) else json.dumps(v))
                want[rel] = cell.format(v=f"{rel}/verification.json")
            cut = frl.new_run_dir(root, now - 3600)[1]  # (cut off before its report, but verified)
            frl.write_json(os.path.join(root, cut, "verification.json"), {"outcome": "ok"})
            want[cut] = f"[ok]({cut}/verification.json)"
            current = frl.new_run_dir(root, now)[1]
            want[current] = "pending"
            frl.write_index(root, now, {"run": "x", "dir": current, "started": now, "tabs": [], "outcomes": {},
                                        "errors": []})
            with open(os.path.join(root, "INDEX.md")) as f:
                runs = f.read().split("## Runs in the last")[1].split("\n## ")[0]
        self.assertIn("| Run (UTC) | New builds | Updated | Collection result | Verification |", runs)  # fails: none
        rows = {line.split("](")[1].split("/)")[0]: [c.strip() for c in line.strip("|").split("|")]
                for line in runs.splitlines() if line.startswith("| [")}
        self.assertEqual({rel: cells[-1] for rel, cells in rows.items()}, want)
        self.assertEqual(rows[cut][-2], "no run report: the run was cut off")
        note = runs.split("\n\n")[2]  # (after the one on Collection result)
        self.assertTrue(note.startswith("Verification is the outcome"), note)
        for value in ("ok", "problems", "timed out", "interrupted", "stopped", "incomplete", "pending", "not recorded",
                      "unreadable"):  # (each the cell can show)
            self.assertRegex(note, rf"\b{value}\b")
        self.assertIn("ran before verifications were recorded", note)  # (as every run did before --record)


class OneProblem(unittest.TestCase):
    def test_a_run_with_one_problem_says_so(self):
        """INDEX.md's runs table said "1 problems" (run_rows)."""
        now = time.time()
        with tempfile.TemporaryDirectory() as root:
            for i, errors in enumerate((1, 2)):
                rel = frl.new_run_dir(root, now - (3 - i) * 3600)[1]
                frl.write_json(os.path.join(root, rel, "run.json"),
                               {"run": "x", "started": now, "outcomes": {},
                                "errors": [{"build": None, "error": "boom"}] * errors})
            rows = frl.run_rows(root, report.day_of(now - 86400))
        cells = [[c.strip() for c in row.strip("|").split("|")] for row in rows]
        self.assertEqual([c[3] for c in cells], ["2 problems", "1 problem"])  # fails: 1 problems


# ------------------------------------------------------------------ run.json: the code, and the GitHub Actions run

GITHUB = {"GITHUB_REPOSITORY": "owner/repo", "GITHUB_RUN_ID": "18500000000", "GITHUB_RUN_ATTEMPT": "2"}


class RunReportCode(ArchiveWorld):
    """A fetch's run.json, made on GitHub Actions. cab048a's named neither the commit that ran nor
    the GitHub Actions run, so each test fails there."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        with mock.patch.dict(os.environ, GITHUB):
            self.assertEqual(self.fetch(), 0)

    def test_the_run_report_names_the_code_and_the_github_run(self):
        run = self.runs()[-1]
        self.assertEqual(run.get("tool"), frl.tool_version())  # fails: no tool_version (and run.json has no "tool")
        self.assertEqual(run.get("github"), {"repository": "owner/repo", "run_id": "18500000000", "run_attempt": "2"})

    def test_outside_github_actions_no_run_is_named(self):
        with mock.patch.dict(os.environ):
            os.environ.pop("GITHUB_RUN_ID", None)
            self.assertIsNone(frl.github_run())  # fails: no github_run


class ToolVersion(unittest.TestCase):
    """frl.tool_version(), for a fetch_red_logs.py in a checkout made here (its git, with no
    config but the repository's)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.addCleanup(frl.tool_version.cache_clear)  # fails: no tool_version
        os.makedirs(os.path.join(self.tmp, "tool"))
        self.env = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                    "GIT_CEILING_DIRECTORIES": os.path.dirname(self.tmp)}

    def write(self, rel, text):
        with open(os.path.join(self.tmp, rel), "w") as f:
            f.write(text)

    def git(self, *args):
        return subprocess.run(["git", "-C", self.tmp, "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
                              env=dict(os.environ, **self.env), check=True, capture_output=True, text=True).stdout.strip()

    def version(self):
        frl.tool_version.cache_clear()
        with mock.patch.object(frl, "__file__", os.path.join(self.tmp, "tool", "fetch_red_logs.py")), \
                mock.patch.dict(os.environ, self.env):
            return frl.tool_version()

    def test_the_commit_checked_out_and_whether_tool_differs_from_it(self):
        self.write("tool/fetch_red_logs.py", "as committed\n")
        self.write("README.md", "as committed\n")
        self.assertEqual(self.version(), {"commit": None, "modified": None})  # (not a checkout)
        self.git("init", "-q")
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "x")
        self.assertEqual(self.version(), {"commit": self.git("rev-parse", "HEAD"), "modified": False})
        self.write("README.md", "changed, outside tool/\n")
        self.assertEqual(self.version()["modified"], False)
        self.write("tool/new.py", "not committed\n")
        self.assertEqual(self.version()["modified"], True)
        os.remove(os.path.join(self.tmp, "tool", "new.py"))
        self.write("tool/fetch_red_logs.py", "changed\n")
        self.assertEqual(self.version()["modified"], True)
        self.git("checkout", "-q", "--", "tool")
        self.assertEqual(self.version()["modified"], False)

    def test_a_file_name_that_is_not_utf8(self):
        # (git status prints it as it is with core.quotePath false)
        self.git("init", "-q")
        self.git("config", "core.quotePath", "false")
        self.write("tool/fetch_red_logs.py", "as committed\n")
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "x")
        with open(os.path.join(os.fsencode(self.tmp), b"tool", b"\xff.txt"), "w") as f:
            f.write("not committed\n")
        self.assertEqual(self.version(), {"commit": self.git("rev-parse", "HEAD"), "modified": True})


# ------------------------------------------------------------------ verification.json: each run's verification, kept

class VerificationRecord(ArchiveWorld):
    """cross_check.py --record, on a run that archived one red build. cab048a kept no record of a
    verification: its outcome was only in the run's log on GitHub Actions (and in whether the run
    failed), and cross_check.py had no --record, so each test but the control fails there."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        with mock.patch.dict(os.environ, GITHUB):
            self.assertEqual(self.fetch(), 0)
        self.path = os.path.join(self.root, frl.run_dirs(self.root)[-1], "verification.json")

    def verify(self, *extra):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), mock.patch.dict(os.environ, GITHUB):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "200",
                            *extra])
        return code, out.getvalue()

    def recorded(self):
        return frl.read_json(self.path)

    def test_a_verification_is_recorded_in_the_runs_folder(self):
        code, out = self.verify("--record")
        self.assertEqual(code, 0, out)
        v = self.recorded()
        self.assertEqual(v["tool"], frl.tool_version())  # (see ToolVersion)
        self.assertEqual(v["github"], {"repository": "owner/repo", "run_id": "18500000000", "run_attempt": "2"})
        run = self.runs()[-1]
        self.assertEqual((v["run"], v["outcome"], v["problem_count"], v["problems"], v["note_count"], v["notes"]),
                         (run["run"], "ok", 0, [], 0, []))
        self.assertLessEqual(run["finished"], v["started"])
        self.assertLessEqual(v["started"], v["finished"])
        self.assertEqual([n for n in os.listdir(os.path.dirname(self.path)) if n.endswith(".part")], [])
        t0 = run["started"]
        self.assertEqual(v["scope"], {"window": {"from": t0 - 200 * 3600, "to": t0}, "tabs": 1, "builds": 1,
                                      "compared_with_gcs": 1, "sevenths": [int(t0 // 86400) % 7], "differ": 0,
                                      "not_settled": 0, "red_on_testgrid": 1, "red_not_settled": 0, "gcs_builds": 0,
                                      "judged_by_rule": 0})

    def test_it_says_incomplete_until_the_checks_end(self):
        # (so a verification killed part-way, which writes nothing more, never reads as passed; it
        # shows the scope learned so far)
        seen, inventory = [], cc.inventory

        def looking(*args):
            seen.append(self.recorded())
            return inventory(*args)
        with mock.patch.object(cc, "inventory", looking):
            code, out = self.verify("--record")
        self.assertEqual((code, seen[0]["outcome"], self.recorded()["outcome"]), (0, "incomplete", "ok"), out)
        self.assertEqual(seen[0]["scope"]["builds"], 1)

    def test_problems_are_recorded(self):
        os.remove(os.path.join(self.bdir(), "build-log.txt"))
        code, out = self.verify("--record")
        v = self.recorded()
        self.assertEqual((code, v["outcome"], v["problem_count"]), (1, "problems", 1), out)
        self.assertIn("build-log.txt: cannot verify: FileNotFoundError", v["problems"][0])

    def test_notes_are_recorded(self):
        # (a copy not settled yet that GCS no longer holds: noted, as the next fetch compares it again)
        meta = os.path.join(self.bdir(), "meta.json")
        frl.write_json(meta, dict(frl.read_json(meta), settled=False, final=False))
        self.srv.route(self.base + "/build-log.txt", obj(b"a later upload\n", "2"))
        code, out = self.verify("--record")
        v = self.recorded()
        self.assertEqual((code, v["outcome"], v["note_count"]), (0, "ok", 1), out)
        self.assertIn("(not settled yet: the next fetch compares it again)", v["notes"][0])

    def test_problems_past_the_first_hundred_are_counted(self):
        run_json = os.path.join(os.path.dirname(self.path), "run.json")
        frl.write_json(run_json, dict(frl.read_json(run_json),
                                      errors=[{"build": f"job/{i}", "error": "boom"} for i in range(150)]))
        code, out = self.verify("--record")
        v = self.recorded()
        self.assertEqual((code, v["outcome"], v["problem_count"], len(v["problems"])), (1, "problems", 150, 100))
        self.assertEqual(v["problems"][-1], "the run reported an error for job/99: boom")

    def test_a_timeout_is_recorded(self):
        inventory = cc.inventory
        with mock.patch("os._exit") as exit_:
            def slow(*args):  # (until the watchdog has timed the verification out)
                give_up = time.monotonic() + 60
                while not exit_.called and time.monotonic() < give_up:
                    time.sleep(0.01)
                return inventory(*args)
            with mock.patch.object(cc, "inventory", slow):
                code, out = self.verify("--record", "--timeout-minutes", "0.01")  # (0.6 seconds)
        exit_.assert_called_once_with(1)
        v = self.recorded()
        self.assertEqual((code, v["outcome"], v["problem_count"], v["problems"]),
                         (1, "timed out", 1, ["verification did not finish within 0.01 minutes"]), out)

    def test_an_interrupt_is_recorded(self):
        def interrupt(*args):
            raise KeyboardInterrupt

        def exit_(code):
            raise SystemExit(code)
        with mock.patch.object(cc, "inventory", interrupt), mock.patch("os._exit", exit_), \
                self.assertRaises(SystemExit) as stopped:
            self.verify("--record")
        self.assertEqual(stopped.exception.code, 130)
        v = self.recorded()
        self.assertEqual((v["outcome"], v["problem_count"], v["problems"]), ("interrupted", 1, ["interrupted"]))

    def interrupted_by(self, signum, before):
        """(how the verification ends when `signum` comes during the checks, its handler set to
        `before` beforehand: the exit code, and the handlers of SIGTERM and SIGINT after)"""
        for s in (signal.SIGTERM, signal.SIGINT):
            self.addCleanup(signal.signal, s, signal.signal(s, before))

        def signalled(*args):  # (the handler called as the signal calls it)
            signal.getsignal(signum)(signum, None)

        def exit_(code):
            raise SystemExit(code)
        with mock.patch.object(cc, "inventory", signalled), mock.patch("os._exit", exit_), \
                self.assertRaises(SystemExit) as stopped:
            self.verify("--record")
        return stopped.exception.code, {signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}

    def test_sigterm_interrupts_it_too(self):
        # (the runner ends a step with SIGINT, then SIGTERM)
        def elsewhere(*args):
            pass
        code, handlers = self.interrupted_by(signal.SIGTERM, elsewhere)
        self.assertEqual(code, 130)
        self.assertEqual((self.recorded()["outcome"], handlers), ("interrupted", {elsewhere}))  # (put back)

    def test_sigint_interrupts_it_where_it_was_ignored(self):
        # (as in a shell's background job, where Python starts with SIGINT ignored)
        code, handlers = self.interrupted_by(signal.SIGINT, signal.SIG_IGN)
        self.assertEqual(code, 130)
        self.assertEqual((self.recorded()["outcome"], handlers), ("interrupted", {signal.SIG_IGN}))

    def test_an_error_that_stops_the_verification_is_recorded(self):
        with mock.patch.object(cc, "inventory", side_effect=RuntimeError("boom")):
            code, out = self.verify("--record")
        v = self.recorded()
        self.assertEqual((code, v["outcome"], v["problems"]), (1, "stopped", ["verification stopped: RuntimeError: boom"]))

    def test_a_write_that_fails_is_made_up_for_by_a_later_one(self):
        # (only the last write counts: the file then says what happened; here the first write of
        # the scope fails)
        write_json, outcomes = frl.write_json, []

        def flaky(path, data):
            if path == self.path:
                outcomes.append(data["outcome"])
                if len(outcomes) == 1:
                    raise PermissionError("not now")
            return write_json(path, data)
        with mock.patch.object(frl, "write_json", flaky):
            code, out = self.verify("--record")
        self.assertIn(f"note: cannot write {self.path}: PermissionError: not now", out)
        self.assertNotIn("PROBLEM", out)
        self.assertEqual((code, self.recorded()["outcome"], outcomes[-1]), (0, "ok", "ok"), out)

    def test_a_record_that_cannot_be_ended_fails_the_verification(self):
        write_json = frl.write_json

        def read_only_at_the_end(path, data):
            if path == self.path and data["outcome"] != "incomplete":
                raise PermissionError("read-only")
            return write_json(path, data)
        with mock.patch.object(frl, "write_json", read_only_at_the_end):
            code, out = self.verify("--record")
        self.assertIn(f"PROBLEM: this verification is not recorded in {self.path}: PermissionError: read-only\n", out)
        self.assertEqual((code, self.recorded()["outcome"], out.splitlines()[-1]), (1, "incomplete", "1 problem(s)"), out)

    def test_a_record_that_cannot_be_made_fails_the_verification(self):
        # (and leaves nothing: not an empty file, nor a temp file)
        with mock.patch("os.link", side_effect=PermissionError("no links here")):
            code, out = self.verify("--record")
        self.assertIn(f"PROBLEM: this verification is not recorded in {self.path}: PermissionError: no links here\n", out)
        self.assertEqual((code, out.splitlines()[-1]), (1, "1 problem(s)"), out)
        self.assertEqual([n for n in os.listdir(os.path.dirname(self.path)) if "verification" in n], [])

    def test_a_record_made_meanwhile_by_another_is_kept(self):
        # (one that cannot make its own writes nothing more, so another verification's stays)
        def another_first(src, dst):
            with open(dst, "w") as f:
                f.write('{"outcome": "ok", "by": "another verification"}\n')
            raise PermissionError("no links here")
        with mock.patch("os.link", another_first):
            code, out = self.verify("--record")
        self.assertEqual((code, self.recorded()), (1, {"outcome": "ok", "by": "another verification"}), out)

    def test_an_interrupt_exits_even_when_recording_it_fails(self):
        def interrupt(*args):
            raise KeyboardInterrupt

        def exit_(code):
            raise SystemExit(code)
        with mock.patch.object(cc, "inventory", interrupt), mock.patch.object(cc.Record, "end", side_effect=OSError("stuck")), \
                mock.patch("os._exit", exit_), self.assertRaises(SystemExit) as stopped:
            self.verify("--record")
        self.assertEqual(stopped.exception.code, 130)

    def test_a_record_interrupted_as_it_is_made_says_incomplete(self):
        write_new_json = frl.write_new_json

        def then_interrupted(path, data):
            write_new_json(path, data)
            raise KeyboardInterrupt

        def exit_(code):
            raise SystemExit(code)
        with mock.patch.object(frl, "write_new_json", then_interrupted), mock.patch("os._exit", exit_), \
                self.assertRaises(SystemExit) as stopped:
            self.verify("--record")
        self.assertEqual(stopped.exception.code, 130)
        self.assertEqual(self.recorded()["outcome"], "incomplete")

    def test_a_record_already_there_is_kept(self):
        with open(self.path, "w") as f:
            f.write('{"outcome": "ok", "by": "an earlier verification"}\n')
        code, out = self.verify("--record")
        self.assertEqual(code, 0, out)
        self.assertEqual(self.recorded(), {"outcome": "ok", "by": "an earlier verification"})
        self.assertIn("is there already, and is kept: this verification is not recorded", out)

    def test_a_link_there_is_kept_too(self):
        os.symlink("nowhere", self.path)
        code, out = self.verify("--record")
        self.assertEqual(code, 0, out)
        self.assertEqual(os.readlink(self.path), "nowhere")
        self.assertIn("is there already, and is kept: this verification is not recorded", out)

    def test_a_report_outside_a_run_folder_is_not_recorded(self):
        # (one of the old layout, say: a verification.json next to it would read as another)
        flat = os.path.join(self.root, "runs", "20261001T120000.000000Z.json")
        shutil.copy(os.path.join(os.path.dirname(self.path), "run.json"), flat)
        code, out = self.verify("--record", "--run", flat)
        self.assertEqual(os.listdir(os.path.join(self.root, "runs")).count("verification.json"), 0)
        self.assertIn("is not the run.json of a run folder: this verification is not recorded", out)

    def test_only_a_run_folders_own_run_json_is_recorded(self):
        folder = os.path.dirname(self.path)
        shutil.copy(os.path.join(folder, "run.json"), os.path.join(folder, "other.json"))
        like = folder + "-extra"  # (named like a run folder, but not one)
        shutil.copytree(folder, like)
        for run in (os.path.join(folder, "other.json"), os.path.join(like, "run.json")):
            with self.subTest(run=run):
                code, out = self.verify("--record", "--run", run)
                self.assertFalse(os.path.lexists(os.path.join(os.path.dirname(run), "verification.json")))
                self.assertIn("is not the run.json of a run folder: this verification is not recorded", out)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder)
        code, out = self.verify("--record", "--run", "run.json")  # (the run's own, from its folder)
        self.assertEqual((code, self.recorded()["outcome"]), (0, "ok"), out)

    def test_control_without_record_nothing_is_written(self):
        code, out = self.verify()
        self.assertEqual(code, 0, out)
        self.assertFalse(os.path.lexists(self.path))


class RecordedVerification(unittest.TestCase):
    """--record's parts on their own: the workflow's verify step, and the watchdog's thread. Each test
    fails on cab048a, which had neither --record nor cc.Record, for the reason its docstring gives."""

    def test_the_workflow_records_each_verification(self):
        """(cab048a's verify step had no --record)"""
        flow = workflow("red-build-logs.yml")
        self.assertIn("--record", step_of(flow, "archive", lambda s: s.get("id") == "verify")["run"].split())  # fails

    def test_a_timeout_after_the_verification_ended_changes_nothing(self):
        """(the watchdog's thread can fire between the end of the checks and its cancel: it must
        neither exit then nor later, when the timer that waits on its recording would fire, nor print;
        cab048a: no cc.Record)"""
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            record = cc.Record(True)  # fails: no cc.Record
            record.start(os.path.join(root, rel, "run.json"), "x", [], [])
            self.assertTrue(record.end("ok"))
            out = io.StringIO()
            with mock.patch("os._exit") as exit_, mock.patch.object(cc, "STUCK_SECONDS", 0.05), \
                    contextlib.redirect_stdout(out):
                cc.timed_out(1, record)
                time.sleep(0.3)
            exit_.assert_not_called()
            self.assertEqual(out.getvalue(), "")
            self.assertEqual(frl.read_json(os.path.join(root, rel, "verification.json"))["outcome"], "ok")

    def test_the_watchdog_exits_even_when_recording_fails(self):
        """(cab048a: no cc.Record)"""
        record, out = cc.Record(True), io.StringIO()
        before = set(threading.enumerate())
        with mock.patch.object(record, "end", side_effect=RuntimeError("no record")), contextlib.redirect_stdout(out), \
                mock.patch("os._exit", side_effect=SystemExit) as exit_, self.assertRaises(SystemExit):
            cc.timed_out(1, record)
        armed = [t for t in set(threading.enumerate()) - before if isinstance(t, threading.Timer) and not t.finished.is_set()]
        for t in armed:  # (so that none fires later, into another test)
            t.cancel()
        self.assertEqual(armed, [])  # (the timer that waits on the recording is cancelled too)
        exit_.assert_called_once_with(1)
        self.assertEqual(out.getvalue(), "  PROBLEM: verification did not finish within 1 minutes\n")

    def test_the_watchdog_exits_even_when_recording_hangs(self):
        """(and says why first, flushed: os._exit drops what a pipe was not given yet, as it would drop
        what was printed before; cab048a: no cc.Record)"""
        class Flushed(io.StringIO):  # (what a pipe holds when os._exit comes: what was flushed)
            flushed = ""

            def flush(self):
                self.flushed = self.getvalue()
        record, stuck, out, left = cc.Record(True), threading.Event(), Flushed(), []
        with mock.patch.object(record, "end", side_effect=lambda *args: stuck.wait(60) and False), \
                mock.patch.object(cc, "STUCK_SECONDS", 0.05), \
                mock.patch("os._exit", side_effect=lambda code: left.append(out.flushed)) as exit_, \
                contextlib.redirect_stdout(out):
            watchdog = threading.Thread(target=cc.timed_out, args=(1, record), daemon=True)
            watchdog.start()
            give_up = time.monotonic() + 30
            while not left and time.monotonic() < give_up:  # (not exit_.called: mock sets it before the side effect)
                time.sleep(0.01)
            stuck.set()  # (then the recording ends, as if the main thread had ended it first)
            watchdog.join(30)
        exit_.assert_called_once_with(1)
        line = "  PROBLEM: verification did not finish within 1 minutes (and recording it did not end within 0.05 seconds)\n"
        self.assertEqual((left, out.getvalue()), ([line], line))  # (and nothing more once the recording ends)

    def test_the_watchdog_exits_even_when_it_cannot_print(self):
        """(a stdout that cannot be written to: the exit comes all the same, from timed_out() and from
        exit_stuck(); cab048a: timed_out() printed first, and had no exit_stuck())"""
        closed = io.StringIO()
        closed.close()
        for name, call in (("timed_out", lambda exit_: cc.timed_out(1)), ("exit_stuck", lambda exit_: cc.exit_stuck("x", exit_))):
            with self.subTest(name), contextlib.redirect_stdout(closed), mock.patch("os._exit") as exit_, \
                    self.assertRaises(ValueError):
                call(exit_)
            exit_.assert_called_once_with(1)

    def test_no_watchdog_is_left_when_cross_check_cannot_start(self):
        """main() sets its signal handlers before it starts its watchdog, so where they cannot be set
        (a thread other than the main one) it stops with no watchdog left to end the process later.
        (cab048a set none, and ran to its end there: no ValueError.)"""
        before, failed = set(threading.enumerate()), []

        def elsewhere():
            try:
                cc.main(["--archive", "nowhere", "--timeout-minutes", "10"])
            except ValueError as e:
                failed.append(e)
        with mock.patch.object(cc, "timed_out"):  # (a watchdog left would end nothing)
            thread = threading.Thread(target=elsewhere)
            thread.start()
            thread.join(30)
            left = [t for t in set(threading.enumerate()) - before if isinstance(t, threading.Timer)]
            for t in left:
                t.cancel()
        self.assertEqual(len(failed), 1)  # fails: 0
        self.assertEqual(left, [])

    def test_notes_past_the_first_hundred_are_counted(self):
        """(cab048a: no cc.Record)"""
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            record = cc.Record(True)
            record.start(os.path.join(root, rel, "run.json"), "x", [], [f"note {i}" for i in range(150)])
            self.assertTrue(record.end("ok"))
            v = frl.read_json(os.path.join(root, rel, "verification.json"))
        self.assertEqual((v["note_count"], len(v["notes"]), v["notes"][-1]), (150, 100, "note 99"))

    def test_a_write_json_cannot_hold_fails_too(self):
        """(a value json refuses, as NaN: the file keeps what was last written; cab048a: no cc.Record)"""
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as root, contextlib.redirect_stdout(out):
            rel = frl.new_run_dir(root, time.time())[1]
            record = cc.Record(True)
            record.start(os.path.join(root, rel, "run.json"), "x", [], [])
            record.scope(tabs=float("nan"))
            self.assertTrue(record.end("ok"))
            v = frl.read_json(os.path.join(root, rel, "verification.json"))
        self.assertEqual((record.failed, v["outcome"], v["scope"]), (True, "incomplete", {}))
        self.assertIn("note: cannot write", out.getvalue())


# ------------------------------------------------------------------ the sevenths of the archive compared with GCS each day

class Sevenths(unittest.TestCase):
    def test_the_sevenths_compared_since_the_run_before(self):
        """(cab048a computed them inside cross_check.check: it has no frl.gcs_sevenths)"""
        with tempfile.TemporaryDirectory() as root:
            day = 20_000 * 86400
            for started in (day - 10 * 86400, day - 3 * 86400 + 5, day + 7200):
                rel = frl.new_run_dir(root, started)[1]
                frl.write_json(os.path.join(root, rel, "run.json"), {"run": rel.rsplit("/", 1)[1], "started": started})
            rel = frl.new_run_dir(root, day - 86400)[1]  # (a report with a start no run can have: passed over)
            frl.write_json(os.path.join(root, rel, "run.json"), {"run": rel.rsplit("/", 1)[1], "started": 0})
            with mock.patch("time.time", return_value=day + 86400):
                self.assertEqual(frl.gcs_sevenths(root, day + 3600), {(20_000 - d) % 7 for d in range(3)})  # fails
                self.assertEqual(frl.gcs_sevenths(root, day + 7200), {(20_000 - d) % 7 for d in range(3)})  # (not itself)
                self.assertEqual(frl.gcs_sevenths(root, day + 9000), {20_000 % 7})  # (a run earlier that day)
                self.assertEqual(frl.gcs_sevenths(root, day - 10 * 86400 - 60), {(20_000 - 11) % 7})  # (no run before)
                self.assertEqual(frl.gcs_sevenths(root, day + 30 * 86400), set(range(7)))  # (a week or more after)

    def test_a_build_is_sampled_by_a_hash_every_process_agrees_on(self):
        # (the fetch and cross_check.py sample apart, in two processes: crc32 of "job/build", not hash())
        for key, seventh in (("ci-kind-dra/2104314601444216832", 5), ("job/1", 3), ("job/2", 6)):
            self.assertEqual([s for s in range(7) if frl.sampled(key, {s})], [seventh], key)  # fails: no frl.sampled


# ------------------------------------------------------------------ final builds, compared with GCS once a week

class FinalBuilds(ArchiveWorld):
    """A build final since its first fetch (it finished 3 days before), whose log GCS has replaced
    since. The next fetch's sevenths of the archive (frl.gcs_sevenths, which cross_check.py
    compares too) hold it or not. cab048a never compared a final build again; only cross_check.py
    did, once a week, and failed the run each time ("GCS changed since it was archived"), while no
    run fetched it again."""

    def setUp(self):
        super().setUp()
        self.calls = []
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"first upload\n", "1", self.calls))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["final"])
        self.srv.route(self.base + "/build-log.txt", obj(b"a later upload\n", "2", self.calls))

    def fetch_in(self, sevenths):
        with mock.patch.object(frl, "gcs_sevenths", return_value=sevenths, create=True):  # (cab048a has none)
            return self.fetch()

    def test_a_final_build_in_the_runs_sevenths_is_fetched_again(self):
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"refreshed": [self.key]})  # fails: already-archived
        self.assertEqual(self.read("build-log.txt"), b"a later upload\n")
        m = self.meta()
        self.assertEqual((m["refreshed"], m["refreshed_by_run"], m["final"]), (["build-log.txt"], run["run"], True))
        self.assertEqual(self.check()[0], 0)  # (cross_check.py compares it too: the same now)

    def test_a_final_build_is_compared_in_its_own_seventh_only(self):
        own = zlib.crc32(self.key.encode()) % 7  # (as cross_check.py samples it: by "job/build")
        self.assertEqual(self.fetch_in(set(range(7)) - {own}), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertEqual(self.fetch_in({own}), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})  # fails: already-archived

    def test_the_fetch_samples_a_build_by_its_job_and_build(self):
        # (as cross_check.py does; whichever seventh its folder or number would fall in)
        asked, sampled = [], frl.sampled  # fails: no frl.sampled

        def spy(key, sevenths):
            asked.append(key)
            return sampled(key, sevenths)
        with mock.patch.object(frl, "sampled", spy):
            self.assertEqual(self.fetch_in({0}), 0)  # (one seventh: the build may be in it or not)
        self.assertEqual(asked, [self.key])

    def test_one_no_longer_on_testgrid_too(self):
        # (it scrolled off: found among the archived builds, not among those TestGrid shows red)
        self.show([str(int(self.build) + (1 << 22))], {"job.Overall": ([1], [""])})
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["rechecked"]), ({"refreshed": [self.key]}, 1))  # fails: ({}, 0)
        self.assertEqual(self.read("build-log.txt"), b"a later upload\n")

    def test_a_damaged_final_build_no_longer_on_testgrid_is_repaired_in_its_seventh(self):
        # (cross_check.py reports the damage on every run; before, only a build TestGrid still showed was
        # fetched again)
        self.show([str(int(self.build) + (1 << 22))], {"job.Overall": ([1], [""])})
        with open(os.path.join(self.bdir(), "build-log.txt"), "ab") as f:
            f.write(b"damage")
        self.assertEqual(self.fetch_in(set()), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {})  # (outside its seventh: not looked at)
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})  # fails: {}
        self.assertEqual(self.read("build-log.txt"), b"a later upload\n")
        self.assertEqual(self.check()[0], 0)

    def test_the_fetch_and_cross_check_compare_the_same_sevenths(self):
        # (each for the run it makes or verifies, with the run before three days back: several sevenths)
        seen, sevenths = [], frl.gcs_sevenths  # fails: no frl.gcs_sevenths

        def spy(root, t0):
            seen.append((t0, sevenths(root, t0)))
            return seen[-1][1]
        run_json = os.path.join(self.root, frl.run_dirs(self.root)[-1], "run.json")
        frl.write_json(run_json, dict(frl.read_json(run_json), started=time.time() - 3 * DAY))
        with mock.patch.object(frl, "gcs_sevenths", spy):
            self.fetch()
            self.check()
        started = self.runs()[-1]["started"]
        self.assertEqual([t0 for t0, _ in seen], [started, started])
        self.assertEqual(seen[0][1], seen[1][1])
        self.assertGreater(len(seen[0][1]), 1)

    def test_a_final_build_that_cannot_be_compared_is_an_error_of_the_run(self):
        # (archived 20 days before: a build not final would stop being compared after 14 days of errors)
        meta = os.path.join(self.bdir(), "meta.json")
        frl.write_json(meta, dict(self.meta(), archived_at=time.time() - 20 * DAY))
        self.srv.route(self.base + "/build-log.txt", (500, {}, b""))
        before = self.meta()
        self.assertEqual(self.fetch_in(set(range(7))), 1)  # fails: 0, not compared
        run = self.runs()[-1]
        self.assertIn("comparing the archived copy with GCS failed", run["errors"][0]["error"])
        self.assertNotIn("stopped-comparing", run["outcomes"])
        self.assertEqual(self.meta(), before)  # (compared again by the next run that compares its seventh)
        self.assertNotIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])
        self.assertEqual(self.read("build-log.txt"), b"first upload\n")

    def test_a_note_that_it_was_not_compared_goes_once_it_is(self):
        frl.write_json(os.path.join(self.bdir(), "meta.json"),
                       dict(self.meta(), archived_at=time.time() - 20 * DAY,
                            note="not compared with GCS after 14 days of errors: HTTPError: 500"))
        self.srv.route(self.base + "/build-log.txt", obj(b"first upload\n", "1", self.calls))
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertNotIn("note", self.meta())  # fails: kept

    def test_control_another_note_stays(self):
        note = "GCS had neither finished.json nor build-log.txt 96h after the build started"
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), note=note))
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertEqual(self.meta()["note"], note)

    def test_control_a_note_that_is_not_text_stays(self):
        # (a meta.json edited by hand, "note": null: compared, it is neither dropped nor an error)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), note=None))
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertIsNone(self.meta()["note"])

    def test_control_the_flag_is_never_stored(self):
        # (it goes to archive() for one call: not into a build's record, meta.json or state.json)
        new = str(int(self.build) + (1 << 22))
        self.show([new, self.build], {"job.Overall": ([12, 12], ["", ""])})
        self.srv.route(f"/bucket/logs/job/{new}/build-log.txt", obj(b"new\n"))
        self.srv.route(f"/bucket/logs/job/{new}/finished.json", (200, {}, {"result": "FAILURE", "timestamp": int(time.time())}))
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        for path in [os.path.join(self.bdir(b), "meta.json") for b in (self.build, new)] + [
                os.path.join(self.root, "state.json")]:
            with open(path) as f:
                self.assertNotIn("compare_final", f.read(), path)

    def test_control_a_final_build_outside_them_is_not_compared(self):
        hits = self.srv.hits(self.base + "/finished.json")
        self.assertEqual(self.fetch_in(set()), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertEqual((self.calls, self.srv.hits(self.base + "/finished.json")), (["GET"], hits))
        self.assertEqual(self.read("build-log.txt"), b"first upload\n")

    def test_control_a_final_build_gcs_no_longer_has_stays_as_it_is(self):
        for name in ("build-log.txt", "finished.json"):
            self.srv.route(f"{self.base}/{name}", (404, {}, b""))
        before = self.meta()
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertEqual((self.meta(), self.read("build-log.txt")), (before, b"first upload\n"))


class FinalBuildsOtherwise(ArchiveWorld):
    """Final builds of other kinds, compared in the run's sevenths (cab048a compared none again, so
    each test but the control fails there)."""

    def fetch_in(self, sevenths):
        with mock.patch.object(frl, "gcs_sevenths", return_value=sevenths, create=True):  # (cab048a has none)
            return self.fetch()

    def test_one_archived_without_a_log_gets_a_late_one(self):
        # (cross_check.py found the log in GCS, once a week, and failed the run: "GCS now has a
        # build-log.txt that is not archived")
        self.finished(ago=3 * DAY)
        self.assertEqual(self.fetch(), 0)
        self.assertIsNone(self.meta()["log"])
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), archived_at=time.time() - 15 * DAY))
        self.assertEqual(self.fetch_in(set()), 0)
        self.assertTrue(self.meta()["final"])
        self.srv.route(self.base + "/build-log.txt", obj(b"late log\n"))
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"log-recovered": [self.key]})  # fails: already-archived
        self.assertEqual(self.read("build-log.txt"), b"late log\n")
        self.assertEqual(self.check()[0], 0)

    def test_one_never_settled_settles_once_compared(self):
        # (final without a finished.json, 14 days after it was archived: compared, it takes the one now there)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "1"))
        self.assertEqual(self.fetch(), 0)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), archived_at=time.time() - 15 * DAY))
        self.assertEqual(self.fetch_in(set()), 0)
        self.assertEqual((self.meta()["settled"], self.meta()["final"]), (False, True))
        self.finished(ago=3 * DAY)
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertEqual((self.meta()["settled"], self.meta()["final"]), (True, True))  # fails: (False, True)

    def test_control_one_not_final_still_gives_up_in_the_runs_sevenths(self):
        # (in the sevenths but not final: after 14 days of errors it stops being compared, as before)
        self.finished(ago=3 * 3600)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), archived_at=time.time() - 15 * DAY))
        self.srv.route(self.base + "/build-log.txt", (500, {}, b""))
        self.assertEqual(self.fetch_in(set(range(7))), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"stopped-comparing": [self.key]})

    def test_control_one_not_final_is_compared_outside_the_runs_sevenths_too(self):
        # (off TestGrid and not final: compared on every run, whichever sevenths the run compares)
        self.finished(ago=3 * 3600)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "1"))
        self.assertEqual(self.fetch(), 0)
        self.show([str(int(self.build) + (1 << 22))], {"job.Overall": ([1], [""])})
        self.srv.route(self.base + "/build-log.txt", obj(b"log, longer\n", "2"))
        self.assertEqual(self.fetch_in(set()), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})


# ------------------------------------------------------------------ a held build whose tab a run cannot read

class HeldWhileItsTabCannotBeRead(ttc.ConfigWorld):
    """A build held because TestGrid shows its tab, which the latest config known lacks (its
    config since then is not known), at a run that cannot read the tab's table, or its
    dashboard's summary. cab048a took only the tabs it read as shown: the tab's config since
    then was none (TestGrid had no such tab), so the build was judged not red and no longer
    watched."""

    history, judged = tch.ConfigOverTime.history, tch.ConfigOverTime.judged

    def held(self):
        self.configs = [(self.now - 30 * DAY, {"dash": {"t": ttc.entry(""), "u": ttc.entry("")}}),
                        (self.now - 40 * HOUR, {"dash": {"u": ttc.entry("")}})]  # (TestGrid still shows t)
        self.assertEqual(self.judged(*self.configs)["outcomes"], {"held": [f"job/{self.b}"]})

    def test_it_stays_held_while_its_table_cannot_be_read(self):
        self.held()
        # (u's table read: the summary was read, and t is shown as listed there, not as one of the tabs
        # a dashboard whose summary cannot be read listed when last read)
        self.srv.summary("dash", {"t": "FAILING", "u": "PASSING"})
        self.srv.table("dash", "u", self.srv.httpd.routes[frl.table_url("", "dash", "t")])
        self.srv.table("dash", "t", (500, {}, b""))
        self.fetch_with(self.history(*self.configs))
        self.assertEqual(self.runs()[-1]["outcomes"], {"held": [f"job/{self.b}"]})  # fails: not-red
        self.assertIn(f"job/{self.b}", self.state()["watch"])

    def test_it_stays_held_while_its_dashboards_summary_cannot_be_read(self):
        self.held()
        self.srv.route("/dash/summary", (500, {}, b""))
        self.fetch_with(self.history(*self.configs))
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"held": [f"job/{self.b}"]})  # fails: not-red
        self.assertIn("has no tab dash#t", " ".join(run["warnings"]))  # (the tabs it listed when last read)

    def test_control_a_tab_unlisted_when_last_read_is_not_taken_as_shown(self):
        # (a summary read, listing u and a name the run cannot use; then one that cannot be read)
        self.held()
        self.srv.summary("dash", {"u": "FAILING", "a\tb": "FAILING"})
        self.fetch_with(self.history(*self.configs))
        self.assertTrue(self.state()["tabs"]["dash#t"]["missing_since"])
        self.assertNotIn("has no tab dash#t", " ".join(self.runs()[-1]["warnings"]))
        self.srv.route("/dash/summary", (500, {}, b""))
        self.fetch_with(self.history(*self.configs))
        self.assertNotIn("has no tab dash#t", " ".join(self.runs()[-1]["warnings"]))

    def test_control_a_tab_of_a_dashboard_the_run_does_not_read_is_not_taken_as_shown(self):
        # (other#x, as a tab of a dashboard no longer read: the configs built since lack it, and
        # state.json keeps it; taken as shown, every run would warn, and fail)
        state = self.state()
        state["tabs"]["other#x"] = dict(state["tabs"]["dash#t"])
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.assertEqual(self.fetch_with(self.config("")), 0)
        self.assertNotIn("has no tab", " ".join(self.runs()[-1]["warnings"]))


if __name__ == "__main__":
    unittest.main()
