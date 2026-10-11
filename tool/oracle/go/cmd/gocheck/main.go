// Command gocheck prints what Go makes of strings: strconv.ParseFloat, ParseInt,
// time.Parse(time.RFC3339) and resource.ParseQuantity, one JSON line per input.
package main

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

func main() {
	in := bufio.NewScanner(os.Stdin)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var c struct{ Kind, S string }
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
		}
		out.Encode(map[string]any{"kind": c.Kind, "s": c.S, "ok": err == nil})
	}
}
