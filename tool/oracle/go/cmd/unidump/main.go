// Command unidump prints, as JSON, the tables of Go's unicode package that
// regexp/syntax parses \p{...} classes and case-insensitive patterns with: each
// category's and script's ranges (R16 then R32, with their strides, in the order
// regexp/syntax appends them), their fold tables, the category aliases, Cn, and
// unicode.SimpleFold for every rune it moves. goregexp.py reads them
// (tool/go_unicode.json.gz); the oracle workflow checks that file against this.
package main

import (
	"encoding/json"
	"os"
	"runtime"
	"sort"
	"unicode"
)

type table [][3]uint32

func ranges(t *unicode.RangeTable) table {
	out := table{}
	for _, r := range t.R16 {
		out = append(out, [3]uint32{uint32(r.Lo), uint32(r.Hi), uint32(r.Stride)})
	}
	for _, r := range t.R32 {
		out = append(out, [3]uint32{r.Lo, r.Hi, r.Stride})
	}
	return out
}

func all(m map[string]*unicode.RangeTable) map[string]table {
	out := map[string]table{}
	for name, t := range m {
		out[name] = ranges(t)
	}
	return out
}

func main() {
	var fold [][2]rune
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if f := unicode.SimpleFold(r); f != r {
			fold = append(fold, [2]rune{r, f})
		}
	}
	sort.Slice(fold, func(i, j int) bool { return fold[i][0] < fold[j][0] })
	out := map[string]any{
		"go":               runtime.Version(),
		"unicode":          unicode.Version,
		"categories":       all(unicode.Categories),
		"scripts":          all(unicode.Scripts),
		"fold_category":    all(unicode.FoldCategory),
		"fold_script":      all(unicode.FoldScript),
		"category_aliases": unicode.CategoryAliases,
		"cn":               ranges(unicode.Cn),
		"simple_fold":      fold,
	}
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(out); err != nil {
		panic(err)
	}
}
