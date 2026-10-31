"""Deterministic edge cases, one behaviour each (and golden vectors for the repo)."""
import json

NOW = 0


def cases():
    now = NOW
    stop = now - 15 * 86400
    ok_started = json.dumps({"timestamp": now - 3600}).encode()
    ok_passed = json.dumps({"timestamp": now - 600, "passed": True}).encode()
    pod_ok = json.dumps({"pod": {"status": {"phase": "Succeeded"}}}).encode()
    out = []

    def add(name, started=ok_started, finished=ok_passed, podinfo=pod_ok, artifacts=(), stop_=None, stop_ns=0,
            group=None):
        out.append({"id": name, "started": started, "finished": finished, "podinfo": podinfo,
                    "artifacts": list(artifacts), "stop": stop if stop_ is None else stop_,
                    **({"stop_ns": stop_ns} if stop_ns else {}), **({"group": group} if group is not None else {})})

    def junit(name, data, **kw):
        add(name, artifacts=[{"name": "artifacts/junit_01.xml", "data": data}], **kw)

    # --- started.json / finished.json as Go's Decoder reads them
    add("started-missing-finished-failed", started=None, finished=json.dumps({"timestamp": now - 60, "passed": False}).encode())
    add("started-empty", started=b"")
    add("started-whitespace", started=b" \t\r\n")
    add("started-null", started=b"null")
    add("started-null-garbage", started=b"null x")
    add("started-trailing-garbage", started=b'{"timestamp": %d} ]]' % (now - 3600))
    add("started-number-garbage", started=b"12x")
    add("started-bad-number", started=b"1.x")
    add("started-bom", started=b'\xef\xbb\xbf{"timestamp": %d}' % (now - 3600))
    add("started-ts-string", started=b'{"timestamp": "%d"}' % (now - 3600))
    add("started-ts-float", started=b'{"timestamp": %d.0}' % (now - 3600))
    add("started-ts-exp", started=b'{"timestamp": 1e9}')
    add("started-ts-zero", started=b'{"timestamp": 0}')
    add("started-ts-negative", started=b'{"timestamp": -5}')
    add("started-ts-old", started=b'{"timestamp": %d}' % (now - 16 * 86400))
    # too old: time.Unix(ts, 0).Before(stop), with stop's fraction of a second
    for d in (-1, 0, 1):
        for k in (0, 1, 256, 511):
            add(f"started-ts-stop{d:+d}-frac{k}", started=b'{"timestamp": %d}' % (stop + d), stop_ns=k * 1953125)
    add("started-ts-max", started=b'{"timestamp": 9223372036854775807}')
    add("started-ts-wraps", started=b'{"timestamp": 9223371974719179008}')
    add("started-ts-no-wrap", started=b'{"timestamp": 9223371974719179007}')
    add("started-ts-overflow", started=b'{"timestamp": 9223372036854775808}')
    add("started-ts-min", started=b'{"timestamp": -9223372036854775808}')
    add("started-ts-dup-last-wins", started=b'{"timestamp": 0, "timestamp": %d}' % (now - 3600))
    add("started-ts-dup-null", started=b'{"timestamp": %d, "timestamp": null}' % (now - 3600))
    add("started-key-upper", started=b'{"TIMESTAMP": %d}' % (now - 3600))
    add("started-key-long-s", started=('{"timeſtamp": %d}' % (now - 3600)).encode())
    add("started-key-escaped", started=b'{"\\u0074imestamp": %d}' % (now - 3600))
    add("started-key-exact-beats-fold", started=b'{"timestamp": %d, "TIMESTAMP": 0}' % (now - 3600))
    add("started-pending-string", started=b'{"timestamp": %d, "Pending": "x"}' % (now - 3600))
    add("started-pending-folded", started=b'{"timestamp": %d, "pending": true}' % (now - 3600))
    add("started-repos-number", started=b'{"timestamp": %d, "repos": {"a": 1}}' % (now - 3600))
    add("started-metadata-overflow", started=b'{"timestamp": %d, "metadata": {"a": [1e400]}}' % (now - 3600))
    add("started-metadata-underflow", started=b'{"timestamp": %d, "metadata": {"a": 1e-400}}' % (now - 3600))
    add("started-node-number", started=b'{"timestamp": %d, "node": 5}' % (now - 3600))
    add("started-array", started=b"[]")
    add("started-depth-10000", started=b'{"metadata":' + b"[" * 9999 + b"]" * 9999 + b"}")
    add("started-depth-10001", started=b'{"metadata":' + b"[" * 10000 + b"]" * 10000 + b"}")
    add("started-invalid-utf8-in-string", started=b'{"timestamp": %d, "node": "\xff\xfe"}' % (now - 3600))
    add("started-invalid-utf8-outside", started=b'{"timestamp": %d \xff}' % (now - 3600))
    add("started-control-in-string", started=b'{"node": "a\x01"}')
    add("started-lone-surrogate", started=b'{"timestamp": %d, "node": "\\ud800"}' % (now - 3600))
    add("finished-missing", finished=None)
    add("finished-empty", finished=b"")
    add("finished-partial", finished=b'{"timestamp": 1')
    add("finished-null", finished=b"null")
    add("finished-no-passed-success", finished=b'{"timestamp": %d, "result": "SUCCESS"}' % (now - 600))
    add("finished-no-passed-failure", finished=b'{"timestamp": %d, "result": "FAILURE"}' % (now - 600))
    add("finished-no-passed-no-result", finished=b'{"timestamp": %d}' % (now - 600))
    add("finished-passed-false-result-success", finished=b'{"timestamp": %d, "passed": false, "result": "SUCCESS"}' % (now - 600))
    add("finished-passed-null", finished=b'{"timestamp": %d, "passed": null, "result": "SUCCESS"}' % (now - 600))
    add("finished-passed-string", finished=b'{"timestamp": %d, "passed": "true"}' % (now - 600))
    add("finished-ts-zero", finished=b'{"timestamp": 0, "passed": true}')
    add("finished-ts-negative", finished=b'{"timestamp": -1, "passed": true}')
    add("finished-ts-null", finished=b'{"timestamp": null, "passed": true}')
    add("finished-ts-max", finished=b'{"timestamp": 9223372036854775807, "passed": true}')
    add("finished-ts-max-no-pod", finished=b'{"timestamp": 9223372036854775807, "passed": true}', podinfo=None)
    add("finished-running-field", finished=b'{"timestamp": %d, "passed": true, "Running": 1}' % (now - 600))
    add("finished-result-number", finished=b'{"timestamp": %d, "result": 1}' % (now - 600))
    add("finished-metadata-string", finished=b'{"timestamp": %d, "passed": true, "metadata": "x"}' % (now - 600))
    add("finished-ts-saturates", finished=b'{"timestamp": 9223371974719179007, "passed": true}', podinfo=None)
    add("started-ts-saturates", started=b'{"timestamp": 9223371974719179007}', finished=None, podinfo=None)
    add("running-in-time", finished=None, started=b'{"timestamp": %d}' % (now - 3600))
    add("running-past-deadline", finished=None, started=b'{"timestamp": %d}' % (now - 25 * 3600))
    add("running-past-deadline-no-pod", finished=None, podinfo=None, started=b'{"timestamp": %d}' % (now - 25 * 3600))
    add("nostart-and-error", started=None, finished=b"{")
    add("nostart-and-empty", started=None, finished=b"")
    add("ancient-and-error", started=b'{"timestamp": 5}', podinfo=b"[")

    # --- podinfo.json
    pods = {
        "pod-missing": None, "pod-empty": b"", "pod-null-pod": b'{"pod": null}', "pod-no-pod": b'{"events": []}',
        "pod-empty-pod": b'{"pod": {}}', "pod-failed-sidecar": json.dumps({"pod": {"status": {"phase": "Failed",
            "containerStatuses": [{"name": "sidecar", "state": {"terminated": {"exitCode": 1}}}]}}}).encode(),
        "pod-failed-test-terminated": json.dumps({"pod": {"status": {"phase": "Failed", "containerStatuses": [
            {"name": "test", "state": {"terminated": {"exitCode": 1}}}, {"name": "sidecar", "state": {"terminated": {"exitCode": 0}}}]}}}).encode(),
        "pod-running-container": json.dumps({"pod": {"status": {"phase": "Running", "containerStatuses": [
            {"name": "test", "state": {"running": {}}}, {"name": "sidecar", "state": {"terminated": {"exitCode": 0}}}]}}}).encode(),
        "pod-no-sidecar": json.dumps({"pod": {"status": {"phase": "Failed", "containerStatuses": [
            {"name": "test", "state": {"terminated": {"exitCode": 2}}}]}}}).encode(),
        "pod-unscheduled": json.dumps({"pod": {"status": {"conditions": [{"type": "PodScheduled", "status": "False"}]}}}).encode(),
        "pod-condition-last-wins": json.dumps({"pod": {"status": {"conditions": [{"type": "PodScheduled", "status": "False"},
            {"type": "PodScheduled", "status": "True"}]}}}).encode(),
        "pod-init-waiting": json.dumps({"pod": {"status": {"initContainerStatuses": [{"name": "i", "state": {"waiting": {}}}]}}}).encode(),
        "pod-exitcode-string": json.dumps({"pod": {"status": {"containerStatuses": [{"state": {"terminated": {"exitCode": "1"}}}]}}}).encode(),
        "pod-exitcode-int32-overflow": json.dumps({"pod": {"status": {"containerStatuses": [{"state": {"terminated": {"exitCode": 2147483648}}}]}}}).encode(),
        "pod-dup-status": b'{"pod": {"status": {"phase": "Failed"}, "status": {"containerStatuses": [{"name": "sidecar", "state": {"terminated": {"exitCode": 0}}}]}}}',
        "pod-slice-stale": b'{"pod": {"status": {"phase": "Failed", "containerStatuses": [{"name": "sidecar", "state": {"terminated": {"exitCode": 0}}}, {"name": "b", "state": {"running": {}}}], "containerStatuses": [{"name": "sidecar"}], "containerStatuses": [{"name": "sidecar"}, {"name": "c"}]}}}',
        "pod-slice-reset-empty": b'{"pod": {"status": {"phase": "Failed", "containerStatuses": [{"name": "sidecar"}, {"name": "b", "state": {"running": {}}}], "containerStatuses": [], "containerStatuses": [{"name": "sidecar"}, {"name": "c"}]}}}',
        "pod-time-lenient": b'{"pod": {"metadata": {"creationTimestamp": "2026-10-11T1:02:03,5+24:60"}}}',
        "pod-time-bad": b'{"pod": {"metadata": {"creationTimestamp": "2026-10-11T01:02:03+25:00"}}}',
        "pod-time-number": b'{"pod": {"metadata": {"creationTimestamp": 1}}}',
        "pod-time-null": b'{"pod": {"metadata": {"creationTimestamp": null, "deletionTimestamp": null}}}',
        "pod-time-feb29": b'{"pod": {"metadata": {"creationTimestamp": "2026-02-29T00:00:00Z"}}}',
        "pod-quantity-pi": b'{"pod": {"spec": {"containers": [{"resources": {"limits": {"cpu": ".Pi"}}}]}}}',
        "pod-quantity-ki": b'{"pod": {"spec": {"containers": [{"resources": {"limits": {"cpu": ".Ki"}}}]}}}',
        "pod-quantity-number": b'{"pod": {"spec": {"containers": [{"resources": {"limits": {"cpu": 1.5e3}}}]}}}',
        "pod-quantity-escape": b'{"pod": {"spec": {"containers": [{"resources": {"limits": {"cpu": "\\u0031"}}}]}}}',
        "pod-quantity-space": b'{"pod": {"spec": {"containers": [{"resources": {"limits": {"cpu": " 1\\u00a0"}}}]}}}',
        "pod-quantity-bool": b'{"pod": {"spec": {"containers": [{"resources": {"limits": {"cpu": true}}}]}}}',
        "pod-intorstring-float": b'{"pod": {"spec": {"containers": [{"livenessProbe": {"httpGet": {"port": 1.0}}}]}}}',
        "pod-intorstring-string": b'{"pod": {"spec": {"containers": [{"livenessProbe": {"httpGet": {"port": "x"}}}]}}}',
        "pod-intorstring-big": b'{"pod": {"spec": {"containers": [{"livenessProbe": {"httpGet": {"port": 2147483648}}}]}}}',
        "pod-fieldsv1-any": b'{"pod": {"metadata": {"managedFields": [{"fieldsV1": [1, {"a": 2}]}]}}}',
        "pod-unknown-field-bad-type": b'{"pod": {"nonsense": {"x": [1e400]}}}',
        "pod-labels-number": b'{"pod": {"metadata": {"labels": {"a": 1}}}}',
        "pod-array": b"[]",
    }
    for name, data in pods.items():
        add(name, podinfo=data)
        add(name + "-failed", podinfo=data, finished=json.dumps({"timestamp": now - 600, "passed": False}).encode())

    # --- junit
    junit("junit-failing-case", b'<testsuite name="s"><testcase name="a"><failure>x</failure></testcase><testcase name="b"/></testsuite>')
    junit("junit-error-case", b'<testsuites><testsuite><testcase name="a"><error/></testcase></testsuite></testsuites>')
    junit("junit-flaky", b'<testsuite><testcase name="a"><failure/></testcase><testcase name="a"/></testsuite>')
    junit("junit-two-fails", b'<testsuite><testcase name="a"><failure/></testcase><testcase name="a"><error/></testcase></testsuite>')
    junit("junit-skipped-plain", b'<testsuite><testcase name="a"><skipped/></testcase></testsuite>')
    junit("junit-skipped-message", b'<testsuite><testcase name="a"><skipped message="why"/></testcase></testsuite>')
    junit("junit-skipped-value", b'<testsuite><testcase name="a"><skipped message="skipped">why</skipped></testcase></testsuite>')
    junit("junit-skipped-and-failure", b'<testsuite><testcase name="a"><skipped/><failure/></testcase></testsuite>')
    junit("junit-overall-collision-pass", b'<testsuite name="job"><testcase name="Overall"/></testsuite>',
          finished=json.dumps({"timestamp": now - 600, "passed": False}).encode())
    junit("junit-overall-collision-fail", b'<testsuite name="job"><testcase name="Overall"><failure/></testcase></testsuite>')
    junit("junit-pod-collision", b'<testsuite name="job"><testcase name="Pod"/></testsuite>',
          podinfo=json.dumps({"pod": {"status": {"phase": "Failed", "containerStatuses": [{"name": "sidecar", "state": {"waiting": {}}}]}}}).encode())
    junit("junit-nested-names", b'<testsuites><testsuite name="a"><testsuite name="b"><testcase name="c"><failure/></testcase></testsuite><testcase name="d"/></testsuite></testsuites>')
    junit("junit-empty-names", b'<testsuite><testsuite><testcase><failure/></testcase></testsuite></testsuite>')
    junit("junit-empty-file", b"")
    junit("junit-whitespace", b"  \n")
    junit("junit-text-only", b"just text")
    junit("junit-other-root", b"<results/>")
    junit("junit-root-end-mismatch", b'<testsuite><testcase name="a"><failure/></testcase></testsuites>')
    junit("junit-root-prefix-mismatch", b'<x:testsuite><testcase name="a"><failure/></testcase></y:testsuite>')
    junit("junit-nested-end-mismatch", b'<testsuite><testcase name="a"><failure/></testcas></testsuite>')
    # a root that declares a namespace keeps that entry above the EOF marker when its end tag does not match
    junit("junit-root-ns-end-mismatch", b'<x:testsuite xmlns:x="u"><testcase name="a"><failure/></testcase></x:testsuites>')
    junit("junit-root-default-ns-end-mismatch", b'<testsuite xmlns="u"><testcase name="a"><failure/></testcase></testsuitx>')
    junit("junit-root-two-ns-end-mismatch", b'<testsuite xmlns="u" xmlns:y="v"><testcase name="a"/></testsuitx>')
    junit("junit-root-ns-end-match", b'<x:testsuite xmlns:x="u"><testcase name="a"><failure/></testcase></x:testsuite>')
    junit("junit-root-ns-self-closing", b'<testsuite xmlns="u"/>')
    junit("junit-nested-ns-end-match", b'<testsuite><testcase xmlns:y="v" name="a"><failure/></testcase></testsuite>')
    junit("junit-root-not-a-ns-decl", b'<testsuite xmlns:="u"><testcase name="a"><failure/></testcase></testsuitx>')
    junit("junit-truncated", b'<testsuite><testcase name="a"><failure/>')
    junit("junit-trailing-garbage", b'<testsuite><testcase name="a"/></testsuite><<<\xff')
    junit("junit-second-root-ignored", b'<testsuite/><testsuite><testcase name="a"><failure/></testcase></testsuite>')
    junit("junit-bad-time", b'<testsuite><testcase name="a" time="1,5"/></testsuite>')
    junit("junit-hex-time", b'<testsuite time="0x1p3"><testcase name="a" time="0x1.8p1"/></testsuite>')
    junit("junit-hex-no-exp", b'<testsuite time="0x1"/>')
    junit("junit-inf-time", b'<testsuite time="+Infinity"><testcase name="a" time="nan"/></testsuite>')
    junit("junit-signed-nan", b'<testsuite time="+nan"/>')
    junit("junit-infin", b'<testsuite time="infin"/>')
    junit("junit-overflow-time", b'<testsuite time="1e400"/>')
    junit("junit-max-float", b'<testsuite time="1.7976931348623157e308"/>')
    junit("junit-underscores", b'<testsuite time="1_000.5" tests="1_0"/>')
    junit("junit-bad-underscores", b'<testsuite time="1__0"/>')
    junit("junit-int-overflow", b'<testsuite failures="9223372036854775808"/>')
    junit("junit-int-min", b'<testsuite failures="-9223372036854775808"/>')
    junit("junit-int-space", b'<testsuite failures=" 3 " tests="\xc2\xa03\xc2\xa0"/>')
    junit("junit-int-empty", b'<testsuite failures="" time=""/>')
    junit("junit-int-blank", b'<testsuite failures=" "/>')
    junit("junit-xmlns-attr-matches", b'<testsuite xmlns:time="urn:x"/>')
    junit("junit-prefixed-attr", b'<testsuite x:failures="nope"/>')
    junit("junit-entities", b'<testsuite name="&lt;&amp;&#65;&#x42;"><testcase name="a&apos;"><failure/></testcase></testsuite>')
    junit("junit-bad-entity", b'<testsuite name="&bogus;"/>')
    junit("junit-entity-upper-x", b'<testsuite name="&#X41;"/>')
    junit("junit-entity-nul", b'<testsuite name="&#0;"/>')
    junit("junit-entity-surrogate", b'<testsuite name="&#xD800;"/>')
    junit("junit-entity-too-big", b'<testsuite name="&#1114112;"/>')
    junit("junit-cdata", b'<testsuite><testcase name="a"><failure><![CDATA[<x> & ]]></failure></testcase></testsuite>')
    junit("junit-cdata-eof", b'<testsuite><![CDATA[abc')
    junit("junit-unescaped-cdata-end", b'<testsuite>]]></testsuite>')
    junit("junit-cdata-end-in-attr", b'<testsuite name="]]>"/>')
    junit("junit-lt-in-attr", b'<testsuite name="<"/>')
    junit("junit-comment-dashes", b'<!-- a -- b --><testsuite/>')
    junit("junit-comment-empty", b'<!----><testsuite/>')
    junit("junit-pi-version", b'<?xml version="1.1"?><testsuite/>')
    junit("junit-pi-latin1", b'<?xml version="1.0" encoding="ISO-8859-1"?><testsuite/>')
    junit("junit-pi-utf8-word", b"<?xml version='1.0' encoding='utf8'?><testsuite/>")
    junit("junit-pi-UTF8", b"<?xml version='1.0' encoding='UTF8'?><testsuite/>")
    junit("junit-pi-spaced", b"<?xml version='1.0' encoding = 'latin1'?><testsuite/>")
    junit("junit-pi-xencoding", b"<?xml xencoding='latin1'?><testsuite/>")
    junit("junit-pi-upper-target", b'<?XML encoding="latin1"?><testsuite/>')
    junit("junit-pi-late", b'<testsuite><?xml encoding="latin1"?></testsuite>')
    junit("junit-doctype", b"<!DOCTYPE x [<!ENTITY a 'b'> <!-- c --> ]><testsuite/>")
    junit("junit-doctype-quote", b"<!DOCTYPE x '>' ><testsuite/>")
    junit("junit-doctype-lt", b"<!DOCTYPE x <> ><testsuite/>")
    junit("junit-directive-gt-first", b"<!>x><testsuite/>")
    junit("junit-bad-name", b"<1a/>")
    junit("junit-two-colons", b"<a:b:c/>")
    junit("junit-unquoted-attr", b"<testsuite a=b/>")
    junit("junit-no-space-attrs", b"<testsuite a='1'b='2'/>")
    junit("junit-dup-attrs", b"<testsuite failures='1' failures='x'/>")
    junit("junit-slash-space", b"<testsuite / >")
    junit("junit-invalid-utf8-text", b"<testsuite>\xff</testsuite>")
    junit("junit-invalid-utf8-after-root", b"<testsuite/>\xff")
    junit("junit-control-char", b"<testsuite>\x01</testsuite>")
    junit("junit-fffe-char", "<testsuite>￾</testsuite>".encode())
    junit("junit-bom", b"\xef\xbb\xbf<testsuite/>")
    junit("junit-cr-normalised", b'<testsuite name="a\r\nb\rc"/>')
    # a root suite at unmarshal depth 0, each nested one 2 deeper; Go's limit is 10000
    junit("junit-depth-5000-suites-ok", b"<testsuite>" * 5000 + b"</testsuite>" * 5000)
    junit("junit-depth-5001-suites-over", b"<testsuite>" * 5001 + b"</testsuite>" * 5001)
    junit("junit-depth-5000-suites-case-over", b"<testsuite>" * 5000 + b"<testcase name='deep'/>" + b"</testsuite>" * 5000)
    junit("junit-depth-4999-suites-failure-ok", b"<testsuite>" * 4999 + b"<testcase name='d'><failure/></testcase>" + b"</testsuite>" * 4999)
    junit("junit-unknown-deep", b"<testsuite>" + b"<x>" * 20000 + b"</x>" * 20000 + b"</testsuite>")
    junit("junit-property-children", b'<testsuite><properties><property name="a"><z/>t</property></properties><testcase name="a"><properties><property/></properties></testcase></testsuite>')
    junit("junit-system-out-children", b'<testsuite><testcase name="a"><system-out><failure/>x</system-out></testcase></testsuite>')
    junit("junit-hidden-case", b'<testsuite><other><testcase name="h"><failure/></testcase></other></testsuite>')
    junit("junit-testsuites-attrs-ignored", b'<testsuites failures="x"><testsuite/></testsuites>')
    for name, size in (("junit-size-limit", 100_000_000), ("junit-size-over", 100_000_001), ("junit-gzip-stored", -1)):
        add(name, artifacts=[{"name": "artifacts/junit.xml", "data": b"<testsuite/>", "size": size}])
    names = ["junit.xml", "artifacts/junit.xml", "artifacts/junit_e2e_20260101-1200_01.xml", "artifacts/test.xml",
             "artifacts/testXxml", "artifacts/junit/a.xml", "artifacts/./junit_2.xml", "artifacts/x/../junit_3.xml",
             "artifacts/../../junit.xml", "artifacts/junit.XML", "artifacts/notjunit.xml", "junitx.xml",
             "artifacts/junit_a b.xml", "artifacts/junit_%41.xml", "artifacts/junit?.xml", "artifacts/junit#.xml",
             "artifacts//junit.xml", "artifacts/../.", "artifacts/test.xml/..", "../../..//logs/job/1/junit_4.xml"]
    for n in names:
        add("name-" + n, artifacts=[{"name": n, "data": b'<testsuite><testcase name="x"><failure/></testcase></testsuite>'}])
    add("name-dot-target-exists", artifacts=[{"name": "artifacts/./junit_2.xml", "data": b"<x/>"},
                                             {"name": "artifacts/junit_2.xml", "data": b"<testsuite><testcase name='y'><failure/></testcase></testsuite>"}])
    # (back at the bucket's root, "//" names the folder again: Go 1.26.9 opens the object there)
    add("name-root-target-exists", artifacts=[{"name": "../../..//logs/job/1/junit_4.xml", "data": b"<x/>"},
                                              {"name": "junit_4.xml", "data": b"<testsuite><testcase name='y'><failure/></testcase></testsuite>"}])
    # --- a test group's cell options (as the configurator prints them)
    def junits(*files):
        return [{"name": n, "data": d.encode()} for n, d in files]
    two = ("artifacts/junit_01.xml", "<testsuite name='s'><testcase name='a'><failure/></testcase>"
           "<testcase name='a'/><testcase name='b'><skipped/></testcase></testsuite>")
    for name, podinfo in (("pod-ok", pod_ok), ("pod-missing", None)):
        add("opt-no-pod-" + name, podinfo=podinfo, artifacts=junits(two), group={"disable_prowjob_analysis": True})
        add("opt-no-pod-running-" + name, finished=None, podinfo=podinfo, artifacts=junits(two),
            group={"disable_prowjob_analysis": True})
    add("opt-split", artifacts=junits(two), group={"disable_merged_status": True})
    add("opt-split-overflow", artifacts=junits(("artifacts/junit.xml", "<testsuite>" + "<testcase name='a'/>" * 19 +
                                                "<testcase name='a'><failure/></testcase>" * 3 + "</testsuite>")),
        group={"disable_merged_status": True})
    add("opt-split-twenty", artifacts=junits(("artifacts/junit.xml", "<testsuite>" + "<testcase name='a'/>" * 20 +
                                              "</testsuite>")), group={"disable_merged_status": True})
    add("opt-split-overall", artifacts=junits(("artifacts/junit.xml", "<testsuite><testcase name='job.Overall'>"
                                               "<failure/></testcase></testsuite>")),
        group={"disable_merged_status": True})
    context = {"test_name_config": {"name_format": "%s [%s]", "name_elements": [{"target_config": "Tests name"},
                                                                                 {"target_config": "Context"}]}}
    add("opt-name-context", artifacts=junits(("artifacts/junit_runner_20260101-1200_02.xml", two[1]),
                                             ("artifacts/junit_02.xml", two[1]), ("artifacts/junit.xml", two[1]),
                                             ("artifacts/junitfoo.xml", two[1]), ("artifacts/x/test.xml", two[1])),
        group=context)
    props = ("artifacts/junit_c_1.xml", "<testsuite><testcase name='t'><properties><property name='Context' "
             "value='p1'/><property name='Context' value='p2'/><property name='k' value='v'/></properties>"
             "<properties><property name='m' value='w'/></properties></testcase><testcase name='u'/></testsuite>")
    for parts in (["Tests name", "Context"], ["k", "m", "Thread"], ["Job name", "Tests name"], ["meta", "Timestamp"],
                  ["missing"], []):
        add("opt-name-" + "-".join(p.replace(" ", "_") for p in parts) if parts else "opt-name-none",
            finished=json.dumps({"timestamp": now - 600, "passed": True,
                                 "metadata": {"meta": "from-finished", "k": "not-first", "num": 1}}).encode(),
            artifacts=junits(props), group={"test_name_config": {"name_format": "%s|%s|%s", "name_elements": [
                {"target_config": p} if i % 2 else {"test_property": p} for i, p in enumerate(parts)]}})
    for fmt in ("", "%d", "%[2]s", "%q %x", "%s%%"):
        add("opt-name-format-" + fmt.replace("%", "pct"), artifacts=junits(two),
            group={"test_name_config": {"name_format": fmt, "name_elements": [{"target_config": "Tests name"}]}})
    for status in (0, 4, 9, 12, 13, 15):
        add(f"opt-rule-all-{status}", artifacts=junits(two),
            group={"custom_evaluator_rule_set": {"rules": [{"computed_status": status}]}})
    add("opt-rule-never-then-all", artifacts=junits(two), group={"custom_evaluator_rule_set": {"rules": [
        {"computed_status": 12, "test_result_comparisons": [{"TestResultInfo": None, "comparison": {
            "ComparisonValue": None, "op": 1}}]},
        {"computed_status": 3, "test_result_comparisons": [{"TestResultInfo": None, "comparison": None}]},
        {"test_result_comparisons": []}]}})
    return out


if __name__ == "__main__":  # check them against the oracle (TGORACLE)
    import os
    import sys
    import time

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import harness

    NOW = int(time.time())
    all_cases = cases()
    bad = harness.check(all_cases)
    print(f"{len(all_cases) - len(bad)} of {len(all_cases)} edge cases match TestGrid's own code")
    sys.exit(1 if bad or harness.SLOW else 0)
