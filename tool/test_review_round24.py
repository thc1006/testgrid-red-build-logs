"""Review of 2026-10-15: an outside audit of c8e43a4 (its tests, adapted), and what checking it
found. Each test fails on cab048a for the reason its docstring gives; a test named
test_control_* passes there and shows the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round24 -v
"""
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import ArchiveWorld, obj  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
