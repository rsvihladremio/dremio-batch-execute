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
	"github.com/rsvihladremio/dremio-batch-execute/pkg/output"
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
	// commenting batch size until we implement odbc, we can just set a default value for the meantime
	// batchSize := flag.Int("batch-size", 1, "number of sql statements to execute at once")
	batchSize := 1
	sourceQueryFile := flag.String("source-file", "queries.sql", "file with a list of queries to execute. Each query must be terminated by a ; or be on only one line. Queries must be unique for resume support to work correctly")
	progressFilePath := flag.String("query-progress-file", "queries-completed.txt", "the file that logs all completed queries, will prevent completed queries in the source file from being retried. Multiple invocations of dremio-batch-execute for the same progress file may result in corruption")
	flag.Parse()
	args := conf.Args{
		DremioUsername:   *restAPIUsername,
		DremioPassword:   *restAPIPassword,
		DremioURL:        *restAPIURL,
		HTTPTimeout:      *restHTTPTimeout,
		RequestSleepTime: *sleepTime,
		RequestThreads:   *threads,
		SourceQueryFile:  *sourceQueryFile,
		ProgressFilePath: *progressFilePath,
		BatchSize:        batchSize,
	}
	output.LogStartMessage(args)
	if err := Execute(args); err != nil {
		log.Fatal(err)
	}
}

func Execute(args conf.Args) error {
	httpArgs := conf.ProtocolArgs{
		User:     args.DremioUsername,
		Password: args.DremioPassword,
		URL:      args.DremioURL,
		SkipSSL:  true,
		Timeout:  args.HTTPTimeout,
	}
	eng, err := protocol.NewHTTPEngine(httpArgs)
	if err != nil {
		return fmt.Errorf("unable to configure engine: %v", err)
	}

	queries, err := parser.ReadQueriesWithProgressFileFiltering(args)
	if err != nil {
		return fmt.Errorf("parsing error: %v", err)
	}
	queryPool, err := pool.DivideQueries(args.RequestThreads, queries)
	if err != nil {
		return err
	}

	if err := process.Execute(eng, args.RequestSleepTime, args.ProgressFilePath, queryPool); err != nil {
		return fmt.Errorf("process failure: %v", err)
	}
	return nil
}
