"""What changed between two versions of TestGrid's module in the code the port follows:
relevant_diff.py OLD_DIR NEW_DIR prints one line per change (nothing when none did).

The port (testgrid_rule.py, testgrid_config.py) follows pkg/updater (GCS reading
and the cells), pkg/tabulator (a tab's row filter), util/gcs, metadata,
metadata/junit, internal/result (the order of statuses), the TestGroup and
DashboardTab fields and the statuses in pb, and the Go version and the modules
whose behaviour it ports (the Pod type, quantities, GCS's object reader): as go.mod
names them, and as the Bazel build TestGrid's production images are made with does
(WORKSPACE's Go, repos.bzl's modules, .bazelversion)."""
import os
import re
import sys

PACKAGES = ["pkg/updater", "pkg/tabulator", "util/gcs", "metadata", "metadata/junit", "internal/result"]
FILES = ["pb/config/config.proto", "pb/test_status/test_status.proto", ".bazelversion"]
MODULES = ["k8s.io/api", "k8s.io/apimachinery", "cloud.google.com/go/storage"]


def sources(root, package):
    """The package's Go files (not its tests), by name."""
    d = os.path.join(root, package)
    if not os.path.isdir(d):
        return {}
    out = {}
    for name in sorted(os.listdir(d)):
        if name.endswith(".go") and not name.endswith("_test.go"):
            with open(os.path.join(d, name), "rb") as f:
                out[name] = f.read()
    return out


def read(root, name):
    path = os.path.join(root, name)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return f.read()


def go_mod(root):
    text = (read(root, "go.mod") or b"").decode("utf-8", "replace")
    out = {}
    for m in re.finditer(r"(?m)^(go|toolchain)\s+(\S+)", text):
        out[m.group(1)] = m.group(2)
    for module in MODULES:
        m = re.search(rf"(?m)^\s*(?:require\s+)?{re.escape(module)}\s+(\S+)", text)
        out[module] = m.group(1) if m else None
    return out


def bazel(root):
    """The Go version and the modules' versions TestGrid's Bazel build uses."""
    out = {}
    workspace = (read(root, "WORKSPACE") or b"").decode("utf-8", "replace")
    m = re.search(r'go_register_toolchains\(\s*version\s*=\s*"([^"]+)"', workspace)
    out["go"] = m.group(1) if m else None
    repos = (read(root, "repos.bzl") or b"").decode("utf-8", "replace")
    for module in MODULES:
        m = re.search(rf'importpath\s*=\s*"{re.escape(module)}",(?:(?!go_repository\().)*?version\s*=\s*"([^"]+)"',
                      repos, re.S)
        out[module] = m.group(1) if m else None
    return out


def changes(old, new):
    out = []
    for package in PACKAGES:
        a, b = sources(old, package), sources(new, package)
        out += [f"{package}/{name}" for name in sorted(set(a) | set(b)) if a.get(name) != b.get(name)]
    out += [name for name in FILES if read(old, name) != read(new, name)]
    a, b = go_mod(old), go_mod(new)
    out += [f"go.mod: {key} {a.get(key)} -> {b.get(key)}" for key in sorted(set(a) | set(b)) if a.get(key) != b.get(key)]
    a, b = bazel(old), bazel(new)
    out += [f"Bazel: {key} {a.get(key)} -> {b.get(key)}" for key in sorted(set(a) | set(b)) if a.get(key) != b.get(key)]
    return out


if __name__ == "__main__":
    for line in changes(sys.argv[1], sys.argv[2]):
        print(line)
