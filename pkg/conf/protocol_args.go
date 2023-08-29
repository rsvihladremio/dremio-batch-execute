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

package conf

import "time"

// ProtocolArgs provides a way to configure the communication protocol
type ProtocolArgs struct {
	User     string        // User for Dremio to ues to execute the queries in stress.json
	Password string        // Password for Dremio to use to execute the queries in stress.json
	URL      string        // URL either HTTP URL
	SkipSSL  bool          // SkipSSL avoids validating certificates and hostname for HTTPS
	Timeout  time.Duration // Timeout duration for requests (in the case of HTTP will be each request, included checks for query status
}
