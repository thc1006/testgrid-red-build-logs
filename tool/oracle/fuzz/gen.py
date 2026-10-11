"""Random cases for the differential harness: started/finished/podinfo/junit inputs
that reach the Go behaviours testgrid_rule ports (and lots of plain ones)."""
import json

NOW = 0  # set by the driver
KEYS_S = ["timestamp", "node", "pull", "repos", "repo-commit", "metadata", "job-version", "repo-version", "Pending"]
KEYS_F = ["timestamp", "passed", "metadata", "result", "job-version", "revision", "repo-version", "Running"]


def jstr(s):
    return json.dumps(s)


def key_variant(rng, k):
    r = rng.random()
    if r < 0.6:
        return jstr(k)
    if r < 0.7:
        return jstr(k.upper())
    if r < 0.75:
        return jstr(k.replace("s", "ſ").replace("S", "ſ"))  # long s folds to S
    if r < 0.8:
        return jstr(k.replace("k", "K").replace("K", "K"))  # Kelvin sign folds to K
    if r < 0.88:
        return '"' + "".join(f"\\u{ord(c):04x}" if rng.random() < 0.3 else c for c in k) + '"'
    if r < 0.93:
        return jstr(k.title())
    return jstr(k + rng.choice(["", " ", "x", "é"]))


def number(rng):
    return rng.choice(["0", "-0", "1", "-1", "1.5", "1e2", "1E+2", "-1.0e-3", "9223372036854775807",
                       "9223372036854775808", "-9223372036854775808", "-9223372036854775809", "2147483647",
                       "2147483648", "-2147483649", "1e400", "-1e400", "1e-400", "123456789012345678901234567890",
                       str(rng.randint(-10**6, 10**6)), str(NOW - rng.randint(0, 40 * 86400))])


def scalar(rng):
    r = rng.random()
    if r < 0.3:
        return number(rng)
    if r < 0.55:
        return jstr(rng.choice(["", "x", "SUCCESS", "FAILURE", "success", "True", "Succeeded", "sidecar",
                                "2026-10-11T01:02:03Z", "é", "a\nb"]))
    if r < 0.65:
        return '"\\ud800x"' if rng.random() < 0.5 else '"\\ud83d\\ude00\\u00e9\\/\\b"'
    if r < 0.8:
        return rng.choice(["true", "false"])
    return "null"


def value(rng, depth=0):
    r = rng.random()
    if depth > 3 or r < 0.55:
        return scalar(rng)
    if r < 0.78:
        n = rng.randint(0, 4)
        return "{" + ",".join(f"{key_variant(rng, rng.choice(['a', 'b', 'x', 'nested']))}:{value(rng, depth + 1)}"
                              for _ in range(n)) + "}"
    return "[" + ",".join(value(rng, depth + 1) for _ in range(rng.randint(0, 4))) + "]"


def ws(rng):
    return rng.choice(["", "", "", " ", "\n", "\t \r\n"])


def obj(rng, items):
    return "{" + ws(rng) + ("," + ws(rng)).join(f"{k}{ws(rng)}:{ws(rng)}{v}" for k, v in items) + ws(rng) + "}"


def started_doc(rng):
    items = []
    if rng.random() < 0.9:
        ts = rng.choice([str(NOW - rng.randint(60, 20 * 3600))] * 6 + ["0", "-5", "5", str(NOW - 16 * 86400),
                                                                       f'"{NOW}"', f"{NOW}.0", "null", "1e9"])
        items.append((key_variant(rng, "timestamp"), ts))
    for k in rng.sample(KEYS_S, rng.randint(0, 4)):
        if k == "repos":
            v = rng.choice(['{"a": "b"}', '{"a": 1}', "[]", "null", value(rng)])
        elif k == "metadata":
            v = rng.choice(['{"a": {"b": [1, 2.5, "x"]}}', '{"n": 1e400}', value(rng)])
        elif k == "Pending":
            v = rng.choice(["true", "false", '"x"', "null"])
        else:
            v = rng.choice([jstr("v"), scalar(rng)])
        items.append((key_variant(rng, k), v))
    if rng.random() < 0.15 and items:  # duplicate a key
        items.append(rng.choice(items))
    return obj(rng, items)


def finished_doc(rng):
    items = []
    if rng.random() < 0.9:
        ts = rng.choice([str(NOW - rng.randint(300, 3 * 3600))] * 6 + ["0", "-1", "null", '"1"', "1.5",
                                                                       str(NOW - 30 * 3600)])
        items.append((key_variant(rng, "timestamp"), ts))
    if rng.random() < 0.7:
        items.append((key_variant(rng, "passed"), rng.choice(["true", "false", "null", '"true"', "1"])))
    if rng.random() < 0.6:
        items.append((key_variant(rng, "result"), rng.choice(['"SUCCESS"', '"FAILURE"', '""', '"success"', "1",
                                                             "null", '"ABORTED"'])))
    for k in rng.sample(KEYS_F, rng.randint(0, 3)):
        v = rng.choice([value(rng), jstr("x"), '{"k": "v"}']) if k != "Running" else rng.choice(["true", "1", "null"])
        items.append((key_variant(rng, k), v))
    if rng.random() < 0.15 and items:
        items.append(rng.choice(items))
    return obj(rng, items)


GOOD_TIMES = ["2026-10-11T01:02:03Z", "2026-10-11T01:02:03.123456789Z", "2026-10-11T01:02:03+08:00",
              "2024-02-29T01:02:03Z", "2026-10-11T01:02:03-00:00", "0000-01-01T00:00:00Z"]
ODD_TIMES = ["2026-10-11T1:02:03Z", "2026-10-11T01:02:03,5Z", "2026-10-11T01:02:03+24:00", "2026-10-11T01:02:03+25:00",
             "2026-10-11T01:02:03+23:60", "2026-10-11T01:02:03+23:61", "2026-02-29T01:02:03Z", "2026-13-01T00:00:00Z",
             "2026-10-11 01:02:03Z", "2026-10-11T01:02:03z", "2026-10-11T24:00:00Z", "2026-10-11T01:02:60Z",
             "2026-10-11T01:02:03", "2026-10-11T01:02:03.Z", "2026-1-11T01:02:03Z", "2026-10-11T01:02:03Zx", "", "x",
             "2026-04-31T00:00:00Z", "2026-10-11T01:02:03.5+08:00", "2026-10-11T9:02:03.5+08:00", "+026-10-11T01:02:03Z",
             "2026-10-11T01:02:03+0800", "2026-10-11T01:02:03+08:0", "1900-02-29T00:00:00Z", "2000-02-29T00:00:00Z"]
GOOD_QUANTITIES = ["1", "0", "100m", "1Gi", "1.5Gi", "1Pi", ".Ki", "3Ki", "123456789Ei", "1e3", "1E3", "+5", " 2 ",
                   "0.0001n", "1e-10", "1e-30", "000", "-0", "-000.5", "1.", ".", "-.", "999999999999999999",
                   "9999999999999999999", "123456789012345678901234567890", "1e99", "12345678901234Ki", "7Ei", "1Ti"]
ODD_QUANTITIES = [".Pi", "Ei", "Ki", "-Pi", "1e", "1ee3", "1Kix", "1.5.5", ".e-10", "\\u0031", "1 Gi", "x", "", "1e-99",
                  "123456789012345678901234567890e-30", "1.5e-31", "e5", "1e+", "--1", "1Mi1", "Ki2"]


def t(rng, bad=0.08):
    return rng.choice(ODD_TIMES if rng.random() < bad else GOOD_TIMES)


def q(rng, bad=0.08):
    return rng.choice(ODD_QUANTITIES if rng.random() < bad else GOOD_QUANTITIES)


def pod_doc(rng):
    phase = rng.choice(["Succeeded", "Failed", "Running", "Pending", "", "succeeded", "Unknown"])
    conds = []
    for _ in range(rng.randint(0, 3)):
        conds.append({"type": rng.choice(["PodScheduled", "Initialized", "Ready", "ContainersReady", "podscheduled"]),
                      "status": rng.choice(["True", "True", "False", "Unknown", "true"]),
                      "lastTransitionTime": t(rng), "message": rng.choice(["", "m"])})

    def cstatus(name):
        st = rng.choice([{"terminated": {"exitCode": rng.choice([0, 0, 0, 1, 137, -1]), "startedAt": t(rng),
                                         "finishedAt": t(rng), "message": "done"}},
                         {"terminated": {"exitCode": 0}}, {"running": {"startedAt": t(rng)}},
                         {"waiting": {"reason": "x"}}, {}, {"terminated": None},
                         {"terminated": {"exitCode": 0}, "running": {}},
                         {"terminated": {"exitCode": rng.choice(["0", 2147483648, 1.0, 2147483647, -2147483648])}}
                         if rng.random() < 0.1 else {"terminated": {"exitCode": 0}}])
        return {"name": name, "state": st, "ready": rng.choice([True, False]), "restartCount": rng.choice([0, 1]),
                "image": "i", "imageID": "x", "containerID": "c"}
    statuses = [cstatus(rng.choice(["test", "sidecar", "sidecar", "x", "Sidecar"])) for _ in range(rng.randint(0, 4))]
    init = [cstatus(rng.choice(["initupload", "place-entrypoint"])) for _ in range(rng.randint(0, 2))]
    port = rng.choice([8080, "http", 0, -1, 2147483647]) if rng.random() > 0.08 else rng.choice(
        [2147483648, 1.5, True, None, {}, [], "1e3", -2147483649])
    pod = {"metadata": {"name": "p", "namespace": "test-pods", "creationTimestamp": t(rng),
                        "managedFields": [{"manager": "m", "operation": "Update", "time": t(rng),
                                           "fieldsV1": rng.choice([{"f:x": {}}, "s", 1, None, [], True])}],
                        "labels": {"a": "b"}, "annotations": {"x": "y"}},
           "spec": {"containers": [{"name": "test", "image": "i", "command": ["a"],
                                    "resources": {"limits": {"cpu": q(rng)}, "requests": {"memory": q(rng)}},
                                    "livenessProbe": {"httpGet": {"port": port}, "periodSeconds": 3}}],
                    "volumes": [{"name": "v", "emptyDir": {"sizeLimit": q(rng)}}],
                    "overhead": {"cpu": q(rng)} if rng.random() < 0.3 else None},
           "status": {"phase": phase, "conditions": conds, "containerStatuses": statuses,
                      "initContainerStatuses": init, "startTime": t(rng), "podIP": "10.0.0.1"}}
    text = json.dumps({"pod": pod, "events": [{"x": 1}]})
    text = text.replace('"\\\\u0031"', '"\\u0031"')
    r = rng.random()
    if r < 0.15:  # a list three times: the Go slice keeps elements past its length
        dup = json.dumps(statuses)
        short = json.dumps(statuses[:rng.randint(0, len(statuses))])
        third = json.dumps([rng.choice([{"name": "y"}, {"name": "sidecar"}, {}, {"state": {}}])
                            for _ in range(len(statuses) + rng.randint(-1, 3))])
        text = text.replace('"containerStatuses": ' + dup,
                            '"containerStatuses": ' + dup + ', "containerStatuses": ' + short +
                            ', "containerStatuses": ' + third)
    elif r < 0.2:
        text = text.replace('"status": {"phase"', '"status": {"phase": "Failed"}, "status": {"phase"')
    elif r < 0.24:
        text = text.replace('"pod":', rng.choice(['"POD":', '"Pod":', '"pod":', '"po\\u0064":', '"pod ":']))
    elif r < 0.27:
        text = rng.choice(['{"pod": null}', '{"pod": {}}', '{}', '{"pod": {"status": null}}', '{"events": 1}',
                           '{"pod": {"status": {"containerStatuses": null}}}'])
    elif r < 0.3:
        text = text.replace('"restartCount": 0', '"restartCount": "0"')
    elif r < 0.33:
        text = text.replace('"phase"', '"PHASE"').replace('"containerStatuses"', '"ContainerStatuses"')
    elif r < 0.36:
        text = text.replace('"conditions": [', '"conditions": [{"type": "PodScheduled", "status": "False"}, ')
    return text


def mutate(rng, text):
    b = bytearray(text.encode("utf-8"))
    for _ in range(rng.randint(1, 3)):
        r = rng.random()
        if not b:
            break
        i = rng.randrange(len(b))
        if r < 0.3:
            del b[i]
        elif r < 0.6:
            b[i:i] = bytes([rng.choice(b'{}[]",:0e.-\\ \x00\x80\xff\t')])
        elif r < 0.8:
            b[i] = rng.randrange(256)
        else:
            del b[i:]
    return bytes(b)


def json_bytes(rng, gen):
    r = rng.random()
    if r < 0.04:
        return rng.choice([b"", b"  \n\t", b"null", b"[]", b"1", b'"x"', b"true", b"\xef\xbb\xbf{}", b"{} junk",
                           b"null x", b"1x", b"1.", b"1e", b"-", b"nul", b"{", b"[" * 10001 + b"]" * 10001,
                           b'{"metadata":' + b"[" * 9998 + b"]" * 9998 + b"}",
                           b'{"metadata":' + b"[" * 9999 + b"]" * 9999 + b"}"])
    text = gen(rng)
    if r < 0.25:
        return mutate(rng, text)
    if r < 0.32:
        return (ws(rng) + text + rng.choice(["", " ", "garbage", "}", "{", "\x00"])).encode()
    return text.encode("utf-8")


# ----- junit

def xattr(rng, name, value):
    q = rng.choice(['"', "'"])
    return f' {name}={q}{value}{q}'


def xtext(rng):
    return rng.choice(["", "x", "a &amp; b", "&lt;&gt;&quot;&apos;", "&#65;&#x42;", "line\r\nline\rx",
                       "<![CDATA[ <raw> & ]] > ]]>", "é", "]] >", "&#xD800;", "&#0;", "&#1114111;",
                       "&#1114112;", "&bogus;", "&#x41", "]]>", "\x01", "￿", "<!-- c -->", "<?pi x?>"])


def xnum(rng):
    if rng.random() < 0.8:
        return rng.choice(["1", "1.5", "0", "12.25", "", " 2.5 ", "3"])
    return rng.choice(["1", "1.5", " 2.5 ", "", "x", "1,5", "1e400", "-0", "inf", "+Infinity", "nan", "NaN", "+nan",
                       "0x1p3", "0x1.8p1", "0x1", "1_000", "_1", "1__0", "9223372036854775808", "-9223372036854775808",
                       " 3 ", " 3", "1e", ".", "+", "0x", "1.5e-400", "Infinityx", "infin", "INFINITY", "-inf",
                       "+1_0", "0x1_0p1", "1_e5", "1e1_0", "0x1p99999", "0X1P3", "0x1.fffffffffffff8p1023", "1e308", "2e308",
                       "0x.8p1", "0xp1", "_", "1_"])


def testcase(rng):
    attrs = xattr(rng, "name", rng.choice(["a", "b", "c", "job.Overall", "job.Pod", ""]))
    if rng.random() < 0.3:
        attrs += xattr(rng, "time", xnum(rng))
    kids = []
    for _ in range(rng.randint(0, 2)):
        kind = rng.choice(["failure", "error", "skipped", "skipped", "system-out", "properties", "unknown"])
        if kind == "skipped":
            msg = rng.choice(["", "skipped", "because", None])
            a = "" if msg is None else xattr(rng, "message", msg)
            kids.append(f"<skipped{a}>{rng.choice(['', '', 'why', '<x/>'])}</skipped>")
        elif kind == "properties":
            kids.append('<properties><property name="n" value="1"/><property name="m" value="x"></property></properties>')
        else:
            kids.append(f"<{kind}{xattr(rng, 'message', 'm') if rng.random() < 0.5 else ''}>{xtext(rng)}</{kind}>")
    return f"<testcase{attrs}>{''.join(kids)}</testcase>" if kids or rng.random() < 0.5 else f"<testcase{attrs}/>"


def suite(rng, depth=0):
    attrs = xattr(rng, "name", rng.choice(["s", "", "Kubernetes e2e suite", "job"])) if rng.random() < 0.8 else ""
    for a in rng.sample(["time", "failures", "tests", "skipped", "errors", "disabled", "timestamp"], rng.randint(0, 2)):
        attrs += xattr(rng, a, xnum(rng))
    kids = [testcase(rng) for _ in range(rng.randint(0, 4))]
    if depth < 2 and rng.random() < 0.3:
        kids.append(suite(rng, depth + 1))
    if rng.random() < 0.2:
        kids.append(rng.choice(["<other><testcase name='hidden'><failure/></testcase></other>", xtext(rng),
                                "<properties><property name='p' value='v'/></properties>"]))
    return f"<testsuite{attrs}>{''.join(kids)}</testsuite>"


def junit_doc(rng):
    r = rng.random()
    if r < 0.5:
        body = suite(rng)
    elif r < 0.85:
        body = "<testsuites>" + "".join(suite(rng) for _ in range(rng.randint(0, 3))) + "</testsuites>"
    else:
        body = rng.choice(["<other/>", "", "   ", "<!-- only a comment -->", "<testsuites/>", "<testsuite/>",
                           "<x:testsuite xmlns:x='u'></x:testsuite>", "<x:testsuite></y:testsuite>",
                           "<testsuite></testsuites>", "<testsuite>" * 5001 + "</testsuite>" * 5001,
                           "<testsuite>" * 4999 + "</testsuite>" * 4999, "<a:b:c/>", "<1a/>", "<testsuite a=b/>",
                           "<testsuite a='1'b='2'/>", "<testsuite a='<'/>", "</testsuite>"])
    prolog = rng.choice(["", "", '<?xml version="1.0" encoding="UTF-8"?>\n', "<?xml version='1.0' encoding='utf8'?>",
                         '<?xml version="1.1"?>', '<?xml encoding="latin1"?>', "<?xml encoding = 'latin1'?>",
                         '<?XML encoding="latin1"?>', "﻿", "<!DOCTYPE x [<!ENTITY a 'b'> <!-- c --> ]>",
                         "<!DOCTYPE x '>' >", "junk text ", "<!---->", "<!-- a -- b -->", "<![CDATA[x]]>",
                         "<?xml?>", "<? x?>", "<!>x>"])
    tail = rng.choice(["", "", "\n", "<<< garbage \xff", "<testsuite/>"])
    data = (prolog + body + tail).encode("utf-8")
    if rng.random() < 0.2:
        data = mutate(rng, data.decode("utf-8", "replace"))
    return data


JUNIT_NAMES = ["artifacts/junit_01.xml", "artifacts/junit.xml", "junit.xml", "artifacts/junit_runner.xml",
               "artifacts/foo/junit_e2e_20260101-1200_01.xml", "artifacts/test.xml", "artifacts/testAxml",
               "artifacts/junit/a.xml", "artifacts/./junit_02.xml", "artifacts/x/../junit_03.xml", "artifacts/junit.XML",
               "artifacts/notjunit.xml", "artifacts/build-log.txt", "junitx.xml", "artifacts/junit_0é.xml",
               "artifacts/../../other/junit.xml", "artifacts/.."]


def case(rng, i):
    c = {"id": str(i), "stop": NOW - 15 * 86400}
    if rng.random() < 0.92:
        c["started"] = json_bytes(rng, started_doc)
    if rng.random() < 0.8:
        c["finished"] = json_bytes(rng, finished_doc)
    if rng.random() < 0.6:
        c["podinfo"] = json_bytes(rng, pod_doc)
    arts = []
    for name in rng.sample(JUNIT_NAMES, rng.randint(0, 3)):
        a = {"name": name, "data": junit_doc(rng)}
        r = rng.random()
        if r < 0.05:
            a["size"] = 100_000_001
        elif r < 0.1:
            a["size"] = -1
        elif r < 0.12:
            a["size"] = 100_000_000
        arts.append(a)
    c["artifacts"] = arts
    return c
