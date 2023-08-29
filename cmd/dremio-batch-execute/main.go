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
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/rsvihladremio/dremio-batch-execute/pkg/conf"
	"github.com/rsvihladremio/dremio-batch-execute/pkg/parser"
	"github.com/rsvihladremio/dremio-batch-execute/pkg/pool"
	"github.com/rsvihladremio/dremio-batch-execute/pkg/process"
	"github.com/rsvihladremio/dremio-batch-execute/pkg/protocol"
)

func main() {
	restAPIURL := flag.String("url", "http://localhost:9047", "Dremio REST api URL")
	restAPIUsername := flag.String("user", "dremio", "User to use for operations")
	restAPIPassword := flag.String("pass", "dremio123", "Password for -user")
	restHTTPTimeout := flag.Duration("request-timeout", time.Minute*1, "request timeout")
	sleepTime := flag.Duration("request-sleep-time", time.Second*1, "duration to wait after query is done to mark it as complete, this can also be used to keep from overwhelming a server")
	threads := flag.Int("threads", 1, "number of threads to execute at once, by default 1 is recommended")
	sourceQueryFile := flag.String("source-file", "queries.sql", "file with a list of queries to execute. Each query must be terminated by a ; or be on only one line. Queries must be unique for resume support to work correctly")
	progressFilePath := flag.String("query-progress-file", "queries-completed.txt", "the file that logs all completed queries, will prevent completed queries in the source file from being retried. Multiple invocations of dremio-batch-execute for the same progress file may result in corruption")
	flag.Parse()

	if err := Execute(Args{
		RestAPIUsername:  *restAPIUsername,
		RestAPIPassword:  *restAPIPassword,
		RestAPIURL:       *restAPIURL,
		RestHTTPTimeout:  *restHTTPTimeout,
		SleepTime:        *sleepTime,
		Threads:          *threads,
		SourceQueryFile:  *sourceQueryFile,
		ProgressFilePath: *progressFilePath,
	}); err != nil {
		log.Fatal(err)
	}
}

type Args struct {
	RestAPIUsername  string
	RestAPIPassword  string
	RestAPIURL       string
	RestHTTPTimeout  time.Duration
	SleepTime        time.Duration
	Threads          int
	SourceQueryFile  string
	ProgressFilePath string
}

func Execute(args Args) error {
	httpArgs := conf.ProtocolArgs{
		User:     args.RestAPIUsername,
		Password: args.RestAPIPassword,
		URL:      args.RestAPIURL,
		SkipSSL:  true,
		Timeout:  args.RestHTTPTimeout,
	}
	eng, err := protocol.NewHTTPEngine(httpArgs)
	if err != nil {
		return fmt.Errorf("unable to configure engine: %v", err)
	}

	queries, err := parser.ReadQueriesWithProgressFileFiltering(args.SourceQueryFile, args.ProgressFilePath)
	if err != nil {
		return fmt.Errorf("parsing error: %v", err)
	}
	queryPool, err := pool.DivideQueries(args.Threads, queries)
	if err != nil {
		return fmt.Errorf("unable to divide queries among threads %v", err)
	}

	if err := process.Execute(eng, args.SleepTime, args.ProgressFilePath, queryPool); err != nil {
		return fmt.Errorf("process failure: %v", err)
	}
	return nil
}
