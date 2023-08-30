//	Copyright 2023 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rsvihladremio/dremio-batch-execute/pkg/conf"
	"github.com/rsvihladremio/dremio-batch-execute/pkg/protocol"
)

func cleanup(t *testing.T) {
	t.Helper()
	srcFile := "testdata/delete.sql"

	progressFile := filepath.Join(t.TempDir(), "progress-delete.txt")
	defer func() {
		if err := os.Remove(progressFile); err != nil {
			log.Printf("WARN: unable to remove progress file `%v` with error: %v", progressFile, err)
		}
	}()
	err := Execute(conf.Args{
		DremioUsername:   "dremio",
		DremioPassword:   "dremio123",
		DremioURL:        "http://localhost:9047",
		HTTPTimeout:      time.Second * 5,
		RequestSleepTime: time.Millisecond * 1,
		RequestThreads:   1,
		SourceQueryFile:  srcFile,
		ProgressFilePath: progressFile,
	})
	if err != nil {
		t.Fatalf("cleanup failure: %v", err)
	}

}

func setup(t *testing.T) {
	t.Helper()
	eng, err := protocol.NewHTTPEngine(conf.ProtocolArgs{
		User:     "dremio",
		Password: "dremio123",
		URL:      "http://localhost:9047",
		Timeout:  time.Second * 5,
		SkipSSL:  true,
	})
	if err != nil {
		t.Fatalf("cleanup failure on new http engine: %v", err)
	}
	if err := eng.MakeSource("a"); err != nil {
		t.Logf("WARN: unable to make source: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	srcFile := "testdata/create.sql"

	progressFile := filepath.Join(t.TempDir(), "progress-create.txt")
	err = Execute(conf.Args{
		DremioUsername:   "dremio",
		DremioPassword:   "dremio123",
		DremioURL:        "http://localhost:9047",
		HTTPTimeout:      time.Second * 5,
		RequestSleepTime: time.Millisecond * 1,
		RequestThreads:   1,
		SourceQueryFile:  srcFile,
		ProgressFilePath: progressFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

}

func TestExecute(t *testing.T) {
	defer cleanup(t)
	setup(t)
	srcFile := "testdata/queries.sql"
	progressFile := filepath.Join(t.TempDir(), "progress-default.txt")
	err := Execute(conf.Args{
		DremioUsername:   "dremio",
		DremioPassword:   "dremio123",
		DremioURL:        "http://localhost:9047",
		HTTPTimeout:      time.Second * 5,
		RequestSleepTime: time.Millisecond * 1,
		RequestThreads:   1,
		SourceQueryFile:  srcFile,
		ProgressFilePath: progressFile,
	})
	if err != nil {
		t.Fatalf("unable to execute process to be complete %v", err)
	}
	progressBuff, err := os.ReadFile(progressFile)
	if err != nil {
		t.Fatal(err)
	}
	srcBuff, err := os.ReadFile(srcFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(srcBuff) != string(progressBuff) {
		t.Errorf("expected progress and original file to match but did not original:\n%q\nsrc\n%q", string(srcBuff), string(progressBuff))
	}
}

func TestExecuteWithThreads(t *testing.T) {
	defer cleanup(t)
	setup(t)

	srcFile := "testdata/threading/queries.sql"
	progressFile := filepath.Join(t.TempDir(), "progress-threads.txt")
	err := Execute(conf.Args{
		DremioUsername:   "dremio",
		DremioPassword:   "dremio123",
		DremioURL:        "http://localhost:9047",
		HTTPTimeout:      time.Second * 5,
		RequestSleepTime: time.Millisecond * 1,
		RequestThreads:   4,
		SourceQueryFile:  srcFile,
		ProgressFilePath: progressFile,
	})
	if err != nil {
		t.Fatalf("unable to execute process to be complete %v", err)
	}
	progressBuff, err := os.ReadFile(progressFile)
	if err != nil {
		t.Fatal(err)
	}
	srcBuff, err := os.ReadFile(srcFile)
	if err != nil {
		t.Fatal(err)
	}
	queriesCompleted := []string{}
	scanner := bufio.NewScanner(strings.NewReader(string(progressBuff)))
	for scanner.Scan() {
		queriesCompleted = append(queriesCompleted, scanner.Text())
	}
	sort.Strings(queriesCompleted)

	queriesAttempted := []string{}

	scanner = bufio.NewScanner(strings.NewReader(string(srcBuff)))
	for scanner.Scan() {
		queriesAttempted = append(queriesAttempted, scanner.Text())
	}
	sort.Strings(queriesAttempted)

	if !reflect.DeepEqual(queriesAttempted, queriesCompleted) {
		t.Errorf("expected progress and original file to match but did not original:\n%#v\nsrc\n%#v", queriesAttempted, queriesCompleted)
	}
}

func TestExecuteWithResume(t *testing.T) {
	defer cleanup(t)
	setup(t)
	srcFile := "testdata/resume/queries.sql"
	progressFile := filepath.Join(t.TempDir(), "progress-resume.txt")
	err := Execute(conf.Args{
		DremioUsername:   "dremio",
		DremioPassword:   "dremio123",
		DremioURL:        "http://localhost:9047",
		HTTPTimeout:      time.Second * 5,
		RequestSleepTime: time.Millisecond * 1,
		RequestThreads:   1,
		SourceQueryFile:  srcFile,
		ProgressFilePath: progressFile,
	})
	if err != nil {
		t.Fatalf("unable to execute process to be complete %v", err)
	}
	progressBuff, err := os.ReadFile(progressFile)
	if err != nil {
		t.Fatal(err)
	}
	srcBuff, err := os.ReadFile(srcFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(srcBuff) != string(progressBuff) {
		t.Errorf("expected progress and original file to match but did not original:\n%q\nsrc\n%q", string(srcBuff), string(progressBuff))
	}
}
