// Command tgoracle judges builds exactly as TestGrid's updater does (see
// pkg/updater/zz_oracle.go), to check a Python port against: one JSON object per
// line in, {"id", "started", "finished", "podinfo" (base64, or null for no such
// object), "artifacts": [{"name", "size", "data"}], "stop", "stop_ns", "group"}:
// the build is too old when it started before time.Unix(stop, stop_ns), and its
// column is made with the test group's options ("group": a TestGroup as protojson,
// else a default one); one per line out.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"

	configpb "github.com/GoogleCloudPlatform/testgrid/pb/config"
	"github.com/GoogleCloudPlatform/testgrid/pkg/updater"
	"github.com/GoogleCloudPlatform/testgrid/util/gcs"
)

type object struct {
	data []byte
	size int64
}

// bucket maps object names (without the bucket) to their content.
type bucket map[string]object

func (b bucket) Open(_ context.Context, p gcs.Path) (io.ReadCloser, *storage.ReaderObjectAttrs, error) {
	o, ok := b[p.Object()]
	if !ok {
		return nil, nil, storage.ErrObjectNotExist
	}
	return io.NopCloser(bytes.NewReader(o.data)), &storage.ReaderObjectAttrs{Size: o.size}, nil
}

type names struct{ left []string }

func (it *names) Next() (*storage.ObjectAttrs, error) {
	if len(it.left) == 0 {
		return nil, iterator.Done
	}
	n := it.left[0]
	it.left = it.left[1:]
	return &storage.ObjectAttrs{Name: n}, nil
}

func (b bucket) Objects(_ context.Context, prefix gcs.Path, _, _ string) gcs.Iterator {
	var all []string
	for n := range b {
		if strings.HasPrefix(n, prefix.Object()) {
			all = append(all, n)
		}
	}
	sort.Strings(all)
	return &names{all}
}

func (b bucket) Upload(context.Context, gcs.Path, []byte, bool, string) (*storage.ObjectAttrs, error) {
	panic("read only")
}

type artifact struct {
	Name string  `json:"name"`
	Size *int64  `json:"size"`
	Data *string `json:"data"`
}

type input struct {
	ID        string     `json:"id"`
	Started   *string    `json:"started"`
	Finished  *string    `json:"finished"`
	PodInfo   *string    `json:"podinfo"`
	Artifacts []artifact `json:"artifacts"`
	Stop      int64           `json:"stop"`
	StopNs    int64           `json:"stop_ns"`
	Group     json.RawMessage `json:"group"`
}

type output struct {
	ID string `json:"id"`
	updater.OracleResult
}

const prefix = "logs/job/1/"

func decode(s *string) []byte {
	b, err := base64.StdEncoding.DecodeString(*s)
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64<<20), 512<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var c input
		if err := json.Unmarshal(in.Bytes(), &c); err != nil {
			panic(err)
		}
		b := bucket{}
		for name, s := range map[string]*string{"started.json": c.Started, "finished.json": c.Finished, "podinfo.json": c.PodInfo} {
			if s != nil {
				d := decode(s)
				b[prefix+name] = object{d, int64(len(d))}
			}
		}
		for _, a := range c.Artifacts {
			var d []byte
			if a.Data != nil {
				d = decode(a.Data)
			}
			size := int64(len(d))
			if a.Size != nil {
				size = *a.Size
			}
			b[prefix+a.Name] = object{d, size}
		}
		path, err := gcs.NewPath("gs://bucket/" + prefix)
		if err != nil {
			panic(err)
		}
		var group *configpb.TestGroup
		if len(c.Group) > 0 && string(c.Group) != "null" {
			group = &configpb.TestGroup{}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(c.Group, group); err != nil {
				panic(err)
			}
		}
		res := updater.OracleColumn(context.Background(), b, gcs.Build{Path: *path}, time.Unix(c.Stop, c.StopNs), group)
		if err := out.Encode(output{c.ID, res}); err != nil {
			panic(err)
		}
	}
	if err := in.Err(); err != nil {
		panic(err)
	}
}
