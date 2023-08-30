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

package parser

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/rsvihladremio/dremio-batch-execute/pkg/conf"
)

// ReadQueries reads the query file and extracts sql queries from it, each query must end with a ';'
func ReadQueries(sourceQueryFile string) (queries []string, err error) {
	f, err := os.Open(sourceQueryFile)
	if err != nil {
		return []string{}, err
	}
	scanner := bufio.NewScanner(f)

	// adjust the capacity to your need (max characters in line)
	const maxLineLength = 1024 * 1024
	buf := make([]byte, maxLineLength)
	scanner.Buffer(buf, maxLineLength)
	var existingSql strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if _, err := existingSql.WriteString(line); err != nil {
			return []string{}, err
		}
		if strings.HasSuffix(line, ";") {
			//statement now complete
			queries = append(queries, existingSql.String())
			existingSql.Reset()
		} else {
			if _, err := existingSql.WriteString("\n"); err != nil {
				return []string{}, err
			}
		}
	}
	return
}

func GroupStatements(queries []string, batchSize int) ([]string, error) {
	var batched []string
	var currentBatch strings.Builder
	for i, q := range queries {
		if _, err := currentBatch.WriteString(q); err != nil {
			return []string{}, err
		}
		if _, err := currentBatch.WriteString("\n"); err != nil {
			return []string{}, err
		}
		if i%batchSize == 0 {
			batched = append(batched, currentBatch.String())
			currentBatch.Reset()
		}
	}
	if currentBatch.Len() > 0 {
		batched = append(batched, currentBatch.String())
		currentBatch.Reset()
	}
	return batched, nil
}

func ReadQueriesWithProgressFileFiltering(args conf.Args) (queries []string, err error) {
	queriesInSourceFile, err := ReadQueries(args.SourceQueryFile)
	if err != nil {
		return
	}
	var completedQueries []string
	if _, statErr := os.Stat(args.ProgressFilePath); statErr == nil {
		completedQueries, err = ReadQueries(args.ProgressFilePath)
		if err != nil {
			return
		}
	}
	for _, proposedQuery := range queriesInSourceFile {
		skipQuery := false
		for _, completedQuery := range completedQueries {
			// if we find a completed query has already been done then don't add this query
			if proposedQuery == completedQuery {
				skipQuery = true
				break
			}
		}
		if !skipQuery {
			queries = append(queries, proposedQuery)
		}
	}
	if len(queriesInSourceFile) > 0 && len(queries) == 0 {
		err = fmt.Errorf("all queries in file %v have already been completed according to the file %v. If this is undesirable delete the file %v and try again", args.SourceQueryFile, args.ProgressFilePath, args.ProgressFilePath)
	}
	if args.BatchSize > 1 {
		queries, err = GroupStatements(queries, args.BatchSize)
		if err != nil {
			return
		}
	}
	return
}
