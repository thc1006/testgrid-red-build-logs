// Command gocheck prints what Go makes of strings: strconv.ParseFloat, ParseInt,
// time.Parse(time.RFC3339) and resource.ParseQuantity, the rows a tab's base
// options keep (TestGrid's tabulator, see pkg/tabulator/zz_oracle.go),
// fmt.Sprintf of a format with string operands (a test name, as a test group's
// test_name_config renders it), and the builds TestGrid's ListBuilds takes from an
// entry of Prow's index with given metadata, one JSON line per input.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/GoogleCloudPlatform/testgrid/pkg/tabulator"
	"github.com/GoogleCloudPlatform/testgrid/util/gcs"
)

// entries lists the objects it holds, as GCS lists a folder.
type entries []*storage.ObjectAttrs

func (e entries) Objects(context.Context, gcs.Path, string, string) gcs.Iterator {
	return &entryIterator{e}
}

type entryIterator struct{ left entries }

func (it *entryIterator) Next() (*storage.ObjectAttrs, error) {
	if len(it.left) == 0 {
		return nil, iterator.Done
	}
	o := it.left[0]
	it.left = it.left[1:]
	return o, nil
}

// listEntry lists Prow's index of a pull request job, gs://bucket/pr-logs/directory/job/,
// holding one entry, 1.txt, with metadata (key, value, key, value, ...), as TestGrid's
// updater does (util/gcs ListBuilds): ok false if the listing fails, else each build it
// takes and the object it opens for its started.json (Build.Started).
func listEntry(metadata []string) map[string]any {
	md := map[string]string{}
	for i := 0; i+1 < len(metadata); i += 2 {
		md[metadata[i]] = metadata[i+1]
	}
	index, err := gcs.NewPath("gs://bucket/pr-logs/directory/job/")
	if err != nil {
		panic(err)
	}
	listed, err := gcs.ListBuilds(context.Background(), entries{{Name: "pr-logs/directory/job/1.txt", Metadata: md}}, *index, nil)
	if err != nil {
		return map[string]any{"kind": "link", "ok": false, "error": err.Error()}
	}
	builds := []map[string]any{}
	for _, b := range listed {
		started, err := b.Path.ResolveReference(&url.URL{Path: "started.json"})
		object := ""
		if err == nil {
			object = started.Bucket() + "/" + started.Object()
		}
		u := b.Path.URL() // (which client reads it: GCS's for gs, else a local one)
		builds = append(builds, map[string]any{"build": b.Build(), "started": object, "resolved": err == nil,
			"scheme": u.Scheme, "bucket": b.Path.Bucket(), "object": b.Path.Object()})
	}
	return map[string]any{"kind": "link", "ok": true, "builds": builds}
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var c struct {
			Kind, S string
			Names   []string
		}
		if err := json.Unmarshal(in.Bytes(), &c); err != nil {
			panic(err)
		}
		var err error
		switch c.Kind {
		case "float":
			_, err = strconv.ParseFloat(c.S, 64)
		case "int64":
			_, err = strconv.ParseInt(c.S, 10, 64)
		case "int32":
			_, err = strconv.ParseInt(c.S, 10, 32)
		case "time":
			_, err = time.Parse(time.RFC3339, c.S)
		case "quantity":
			_, err = resource.ParseQuantity(strings.TrimSpace(c.S))
		case "sprintf":
			args := make([]any, len(c.Names))
			for i, n := range c.Names {
				args[i] = n
			}
			out.Encode(map[string]any{"kind": c.Kind, "s": c.S, "ok": true, "out": fmt.Sprintf(c.S, args...)})
			continue
		case "link":
			out.Encode(listEntry(c.Names))
			continue
		case "filter":
			kept, err := tabulator.OracleFilter(c.S, c.Names)
			if kept == nil {
				kept = []string{}
			}
			out.Encode(map[string]any{"kind": c.Kind, "s": c.S, "ok": err == nil, "kept": kept})
			continue
		}
		out.Encode(map[string]any{"kind": c.Kind, "s": c.S, "ok": err == nil})
	}
}
