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
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func cleanup(t *testing.T) {
	t.Helper()
	srcFile := "testdata/delete.sql"

	progressFile := filepath.Join(t.TempDir(), "progress-delete.txt")
	err := Execute(Args{
		RestAPIUsername:  "dremio",
		RestAPIPassword:  "dremio123",
		RestAPIURL:       "http://localhost:9047",
		RestHTTPTimeout:  time.Second * 5,
		SleepTime:        time.Millisecond * 1,
		Threads:          1,
		SourceQueryFile:  srcFile,
		ProgressFilePath: progressFile,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func setup(t *testing.T) {
	t.Helper()
	srcFile := "testdata/create.sql"

	progressFile := filepath.Join(t.TempDir(), "progress-create.txt")
	err := Execute(Args{
		RestAPIUsername:  "dremio",
		RestAPIPassword:  "dremio123",
		RestAPIURL:       "http://localhost:9047",
		RestHTTPTimeout:  time.Second * 5,
		SleepTime:        time.Millisecond * 1,
		Threads:          1,
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
	err := Execute(Args{
		RestAPIUsername:  "dremio",
		RestAPIPassword:  "dremio123",
		RestAPIURL:       "http://localhost:9047",
		RestHTTPTimeout:  time.Second * 5,
		SleepTime:        time.Millisecond * 1,
		Threads:          1,
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
	err := Execute(Args{
		RestAPIUsername:  "dremio",
		RestAPIPassword:  "dremio123",
		RestAPIURL:       "http://localhost:9047",
		RestHTTPTimeout:  time.Second * 5,
		SleepTime:        time.Millisecond * 1,
		Threads:          4,
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
	err := Execute(Args{
		RestAPIUsername:  "dremio",
		RestAPIPassword:  "dremio123",
		RestAPIURL:       "http://localhost:9047",
		RestHTTPTimeout:  time.Second * 5,
		SleepTime:        time.Millisecond * 1,
		Threads:          1,
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
