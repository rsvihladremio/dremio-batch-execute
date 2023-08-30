# dremio-batch-execute

[![build](https://github.com/rsvihladremio/dremio-batch-execute/actions/workflows/ci.yaml/badge.svg)](https://github.com/rsvihladremio/dremio-batch-execute/actions/workflows/ci.yaml)

Batch run Dremio a list of queries with limits on throughput, concurrency and support for resuming

## How to run

    dremio-batch-execute -url https://myhost:9047 -pass myDremioPass -user myDremioUser -source-file queries.sql


```bash
dremio-batch-execute -h
Usage of dremio-batch-execute:
 -pass string
    	Password for -user (default "dremio123")
  -query-progress-file string
    	the file that logs all completed queries, will prevent completed queries in the source file from being retried. Multiple invocations of dremio-batch-execute for the same progress file may result in corruption (default "queries-completed.txt")
  -request-sleep-time duration
    	duration to wait after query is done to mark it as complete, this can also be used to keep from overwhelming a server (default 1s)
  -request-timeout duration
    	request timeout (default 1m0s)
  -source-file string
    	file with a list of queries to execute. Each query must be terminated by a ; or be on only one line. Queries must be unique for resume support to work correctly (default "queries.sql")
  -threads int
    	number of threads to execute at once, by default 1 is recommended (default 1)
  -url string
    	Dremio REST api URL (default "http://localhost:9047")
  -user string
    	User to use for operations (default "dremio")
```

### SQL file

For this version each query must be terminated with a ; and can span multiple lines. However, multiple statements per line is NOT currently supported

