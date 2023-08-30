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

package pool_test

import (
	"testing"

	"github.com/rsvihladremio/dremio-batch-execute/pkg/pool"
)

func TestPoolHasNoRemainders(t *testing.T) {
	threads := 2
	queries := []string{
		"SELECT 1",
		"SELECT 2",
		"SELECT 3",
		"SELECT 4",
	}
	queryPool, err := pool.DivideQueries(threads, queries)
	if err != nil {
		t.Fatalf("unexpected %v", err)
	}
	if len(queryPool) != 2 {
		t.Fatalf("expected 2 but had %v pools", len(queryPool))
	}

	if len(queryPool[0]) != 2 {
		t.Fatalf("expected pool 0 to have 2 but had %v queries", len(queryPool[0]))
	}

	if len(queryPool[1]) != 2 {
		t.Fatalf("expected pool 1 to have 2 but had %v queries", len(queryPool[1]))
	}
}

func TestPoolHasSameThreadsAsQueries(t *testing.T) {
	threads := 2
	queries := []string{
		"SELECT 1",
		"SELECT 2",
	}
	queryPool, err := pool.DivideQueries(threads, queries)
	if err != nil {
		t.Fatalf("unexpected %v", err)
	}
	if len(queryPool) != 2 {
		t.Fatalf("expected 2 but had %v pools", len(queryPool))
	}

	if len(queryPool[0]) != 1 {
		t.Fatalf("expected pool 0 to have 1 but had %v queries", len(queryPool[0]))
	}

	if len(queryPool[1]) != 1 {
		t.Fatalf("expected pool 1 to have 1 but had %v queries", len(queryPool[1]))
	}
}

func TestPoolHasRemainders(t *testing.T) {
	threads := 2
	queries := []string{
		"SELECT 1",
		"SELECT 2",
		"SELECT 3",
		"SELECT 4",
		"SELECT 5",
	}
	queryPool, err := pool.DivideQueries(threads, queries)
	if err != nil {
		t.Fatalf("unexpected %v", err)
	}
	if len(queryPool) != 2 {
		t.Fatalf("expected 2 but had %v pools", len(queryPool))
	}

	if len(queryPool[0]) != 3 {
		t.Fatalf("expected pool 0 to have 3 but had %v queries", len(queryPool[0]))
	}

	if len(queryPool[1]) != 2 {
		t.Fatalf("expected pool 1 to have 2 but had %v queries", len(queryPool[1]))
	}
}
