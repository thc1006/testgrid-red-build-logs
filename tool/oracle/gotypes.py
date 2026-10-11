"""testgrid_types.json from schemadump's output, on stdout (write it to a new file, then
compare or move it: a redirect straight onto ../testgrid_types.json empties that file
before this runs):  schemadump | python3 -I gotypes.py > types.json"""
import json
import sys

with open(__file__.rsplit("/", 1)[0] + "/pinned.json") as f:
    pinned = json.load(f)
d = json.load(sys.stdin)
types = {}
for name, t in d["types"].items():
    k = {"kind": t["kind"]}
    if t.get("custom"):
        k["custom"] = t["custom"]
    for key in ("elem", "key", "caps"):
        if key in t:
            k[key] = t[key]
    if "fields" in t:
        k["fields"] = [[f["go"], f["json"], f["opts"], f["type"], int(f["anonymous"]), int(f["exported"])] for f in t["fields"]]
    types[name] = k
doc = {"about": (f"The Go types TestGrid decodes podinfo.json, started.json and finished.json into, as "
                 f"encoding/json sees them; written by tool/oracle/schemadump from TestGrid "
                 f"{pinned['testgrid_commit']} (Go {pinned['go']}). "
                 f"fields: [Go name, json name, options, type, embedded, exported]; caps: the "
                 f"capacities reflect.Value.Grow(1) gives a full slice, from 0 up."),
       "roots": d["roots"], "ascii_folds": d["ascii_folds"], "types": types}
sys.stdout.write(json.dumps(doc, separators=(",", ":"), sort_keys=True) + "\n")
