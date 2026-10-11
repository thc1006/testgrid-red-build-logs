package tabulator

// For differential testing of a Python port only: the rows a tab's base options
// keep, by TestGrid's own filterGrid.

import (
	statepb "github.com/GoogleCloudPlatform/testgrid/pb/state"
)

// OracleFilter runs filterGrid on rows of these names: the names it keeps, or its error.
func OracleFilter(baseOptions string, names []string) ([]string, error) {
	rows := make([]*statepb.Row, len(names))
	for i, n := range names {
		rows[i] = &statepb.Row{Name: n}
	}
	kept, err := filterGrid(baseOptions, rows)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(kept))
	for _, r := range kept {
		out = append(out, r.Name)
	}
	return out, nil
}
