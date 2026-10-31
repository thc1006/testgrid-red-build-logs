"""Go's unicode tables for goregexp.py (tool/go_unicode.json.gz), from unidump's output on stdin:
gounicode.py check FILE [--any-go]: whether FILE holds them (from any Go, with --any-go);
gounicode.py write FILE: write them there (gzip, the same bytes for the same tables)."""
import gzip
import json
import sys


def main(argv):
    mode, path = argv[0], argv[1]
    dumped = json.load(sys.stdin)
    if mode == "write":
        with gzip.GzipFile(path, "wb", compresslevel=9, mtime=0) as f:
            f.write(json.dumps(dumped, sort_keys=True, separators=(",", ":")).encode())
        return 0
    with gzip.open(path, "rt", encoding="utf-8") as f:
        held = json.load(f)
    go, was = dumped.get("go") or "Go", held.get("go")  # (as "go1.26.9")
    if "--any-go" in argv:
        dumped.pop("go"), held.pop("go")
    if dumped != held:
        print(f"{path} does not hold {go}'s unicode tables (Unicode {dumped['unicode']}; "
              f"it holds Unicode {held['unicode']}{', from ' + was if was else ''})")
        return 1
    print(f"{path} holds {go}'s unicode tables (Unicode {dumped['unicode']})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
