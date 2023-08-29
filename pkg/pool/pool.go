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

package pool

import (
	"errors"
	"fmt"
)

func DivideQueries(threads int, queries []string) (queriesByThread [][]string, err error) {
	if threads == 0 {
		return queriesByThread, errors.New("unable to have 0 threads")
	}
	if len(queries) == 0 {
		return queriesByThread, errors.New("unable to have 0 queries")
	}
	if threads > len(queries) {
		return queriesByThread, fmt.Errorf("unable to have more threads (%v) than queries (%v)", threads, len(queries))
	}
	queriesByThread = make([][]string, threads)
	for i := 0; i < threads; i++ {
		queriesByThread[i] = []string{}
	}
	for i := 0; i < len(queries); i++ {
		currentThread := i % threads
		queriesByThread[currentThread] = append(queriesByThread[currentThread], queries[i])
	}
	return
}
