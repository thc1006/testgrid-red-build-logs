/*
Copyright 2020 The TestGrid Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Changed from readResult in TestGrid's pkg/updater/read.go (2bbb98db9f1a): the
// four reads run one after another and every error is kept. See THIRD_PARTY_NOTICES.md
// at the root of this repository.

package updater

// For differential testing of a Python port only: what this package makes of
// one build, with readResult's four reads run one after another and every
// error kept (readResult returns whichever arrives first), and convertResult
// run with the test group's options (a default group when none is given), as
// production runs it (--enable-ignore-skip off).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	configpb "github.com/GoogleCloudPlatform/testgrid/pb/config"
	"github.com/GoogleCloudPlatform/testgrid/util/gcs"
)

// OracleResult is one build as TestGrid's updater sees it.
type OracleResult struct {
	Errors     []string          `json:"errors"`  // "nostart", "ancient", or the read error
	Results    []string          `json:"results"` // the Overall results its column can get
	Malformed  []string          `json:"malformed"`
	Cells      map[string]string `json:"cells"` // without a read error: row -> status
	Messages   map[string]string `json:"messages"`
	StartedTS  int64             `json:"started_ts"`
	FinishedTS *int64            `json:"finished_ts"`
	Passed     *bool             `json:"passed"`
	Result     string            `json:"result"`
	Now        int64             `json:"now"`
}

// OracleColumn reads one build like readResult and converts it like readColumns.
func OracleColumn(ctx context.Context, client gcs.Downloader, build gcs.Build, stop time.Time, group *configpb.TestGroup) OracleResult {
	result := gcsResult{job: build.Job(), build: build.Build()}
	var errs []string

	if pi, err := build.PodInfo(ctx, client); errors.Is(err, io.EOF) {
		result.malformed = append(result.malformed, "podinfo.json")
	} else if err != nil {
		errs = append(errs, "podinfo: "+err.Error())
	} else if pi != nil {
		result.podInfo = *pi
	}

	s, err := build.Started(ctx, client)
	switch {
	case errors.Is(err, io.EOF):
		result.malformed = append(result.malformed, "started.json")
	case err != nil:
		errs = append(errs, "started: "+err.Error())
	case time.Unix(s.Timestamp, 0).Before(stop):
		if s.Timestamp == 0 {
			errs = append(errs, "nostart")
		} else {
			errs = append(errs, "ancient")
		}
	default:
		result.started = *s
	}

	f, err := build.Finished(ctx, client)
	switch {
	case errors.Is(err, io.EOF):
		result.malformed = append(result.malformed, "finished.json")
	case err != nil:
		errs = append(errs, "finished: "+err.Error())
	default:
		result.finished = *f
	}

	suites, err := readSuites(ctx, client, build)
	if err != nil {
		errs = append(errs, "suites: "+err.Error())
	}
	var problems []string
	for _, s := range suites {
		if s.Err != nil {
			p := strings.TrimPrefix(s.Path, build.Path.String())
			problems = append(problems, fmt.Sprintf("%s: %s", p, s.Err))
		} else {
			result.suites = append(result.suites, s)
		}
	}
	result.malformed = append(result.malformed, problems...)
	sort.Strings(result.malformed)

	out := OracleResult{Errors: errs, Malformed: result.malformed, StartedTS: result.started.Timestamp,
		FinishedTS: result.finished.Timestamp, Passed: result.finished.Passed, Result: result.finished.Result,
		Now: time.Now().Unix()}
	seen := map[string]bool{}
	for _, e := range errs {
		r := "TOOL_FAIL"
		switch e {
		case "nostart":
			r = "RUNNING"
		case "ancient":
			r = "UNKNOWN"
		}
		seen[r] = true
	}
	if len(errs) == 0 {
		if group == nil {
			group = &configpb.TestGroup{}
		}
		group.GcsPrefix = build.Path.Bucket() + "/" + strings.TrimSuffix(build.Path.Object(), "/")
		opts := makeOptions(group)
		opts.ignoreSkip = false
		col := convertResult(logrus.New(), makeNameConfig(group), build.Build(), nil, result, opts)
		out.Cells, out.Messages = map[string]string{}, map[string]string{}
		for name, c := range col.Cells {
			out.Cells[name] = c.Result.String()
			out.Messages[name] = c.Message
		}
		seen[out.Cells[result.job+"."+overallRow]] = true
	}
	for r := range seen {
		out.Results = append(out.Results, r)
	}
	sort.Strings(out.Results)
	if out.Errors == nil {
		out.Errors = []string{}
	}
	if out.Malformed == nil {
		out.Malformed = []string{}
	}
	return out
}
