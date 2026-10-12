// Command gocheck prints what Go makes of strings: strconv.ParseFloat, ParseInt,
// time.Parse(time.RFC3339) and resource.ParseQuantity, the rows a tab's base
// options keep (TestGrid's tabulator, see pkg/tabulator/zz_oracle.go), and
// fmt.Sprintf of a format with string operands (a test name, as a test group's
// test_name_config renders it), one JSON line per input.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/GoogleCloudPlatform/testgrid/pkg/tabulator"
)

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
