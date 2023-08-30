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

package progress_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rsvihladremio/dremio-batch-execute/pkg/progress"
)

func TestNoProgressFilePresent(t *testing.T) {
	progressFilePath := filepath.Join(t.TempDir(), "progress.txt")

	if err := progress.MarkQueryComplete(progressFilePath, "INSERT INTO TABLE A.C VALUES (1,2);"); err != nil {
		t.Fatalf("unexpected failure %v", err)
	}
	if written, err := os.ReadFile(progressFilePath); err != nil {
		t.Fatalf("unexpected failure reading file: %v", err)
	} else {
		actual := string(written)
		expected := `INSERT INTO TABLE A.C VALUES (1,2);
`
		if expected != actual {
			t.Errorf("does not match expected\n%q\nactual\n%q", expected, actual)
		}
	}
}

func TestProgressFilePresent(t *testing.T) {
	progressFilePath := filepath.Join(t.TempDir(), "progress.txt")

	if err := os.WriteFile(progressFilePath, []byte("DROP TABLE A.B;\n"), 0600); err != nil {
		t.Fatalf("unable to setup test %v", err)
	}

	if err := progress.MarkQueryComplete(progressFilePath, "INSERT INTO TABLE A.C VALUES (1,2);"); err != nil {
		t.Fatalf("unexpected failure %v", err)
	}
	if written, err := os.ReadFile(progressFilePath); err != nil {
		t.Fatalf("unexpected failure reading file: %v", err)
	} else {
		actual := string(written)
		expected := `DROP TABLE A.B;
INSERT INTO TABLE A.C VALUES (1,2);
`
		if expected != actual {
			t.Errorf("does not match expected\n%q\nactual\n%q", expected, actual)
		}
	}
}
