// Dumps the Go types TestGrid decodes podinfo.json, started.json and finished.json
// into, as seen by encoding/json: kinds, JSON field names, and custom unmarshalers.
package main

import (
	"encoding"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/GoogleCloudPlatform/testgrid/util/gcs"
)

var (
	jsonU = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textU = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	types = map[string]any{}
)

func name(t reflect.Type) string {
	if t.Name() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	return t.String()
}

func walk(t reflect.Type) string {
	n := name(t)
	if _, ok := types[n]; ok {
		return n
	}
	d := map[string]any{"kind": t.Kind().String(), "size": t.Size()}
	types[n] = d
	if t.Implements(jsonU) || reflect.PointerTo(t).Implements(jsonU) {
		d["custom"] = "json"
		return n
	}
	if t.Implements(textU) || reflect.PointerTo(t).Implements(textU) {
		d["custom"] = "text"
		return n
	}
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		d["elem"] = walk(t.Elem())
		if t.Kind() == reflect.Slice {
			// The capacity reflect.Value.Grow(1) gives a full slice of each
			// capacity, as encoding/json grows slices (up to 64 MiB of elements).
			var caps []int
			for c := 0; uintptr(c)*t.Elem().Size() <= 64<<20; {
				v := reflect.New(t).Elem()
				v.Set(reflect.MakeSlice(t, c, c))
				v.Grow(1)
				c = v.Cap()
				caps = append(caps, c)
			}
			d["caps"] = caps
		}
	case reflect.Map:
		d["key"] = walk(t.Key())
		d["elem"] = walk(t.Elem())
	case reflect.Struct:
		var fields []map[string]any
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" {
				continue
			}
			jn, opts, _ := strings.Cut(tag, ",")
			field := map[string]any{"go": f.Name, "json": jn, "opts": opts, "type": walk(f.Type), "exported": f.IsExported(),
				"anonymous": f.Anonymous}
			fields = append(fields, field)
		}
		d["fields"] = fields
	}
	return n
}

func main() {
	roots := map[string]string{
		"podinfo":  walk(reflect.TypeOf(gcs.PodInfo{})),
		"started":  walk(reflect.TypeOf(gcs.Started{})),
		"finished": walk(reflect.TypeOf(gcs.Finished{})),
	}
	folds := map[string]string{}
	for r := rune(0x80); r <= unicode.MaxRune; r++ {
		f := r
		for {
			r2 := unicode.SimpleFold(f)
			if r2 <= f {
				f = r2
				break
			}
			f = r2
		}
		if f < 0x80 {
			folds[string(r)] = string(f)
		}
	}
	custom := []string{}
	for n, d := range types {
		if c, ok := d.(map[string]any)["custom"]; ok {
			custom = append(custom, fmt.Sprintf("%s (%v)", n, c))
		}
	}
	sort.Strings(custom)
	fmt.Fprintln(os.Stderr, len(types), "types; custom unmarshalers:", strings.Join(custom, ", "))
	json.NewEncoder(os.Stdout).Encode(map[string]any{"roots": roots, "types": types, "ascii_folds": folds})
}
