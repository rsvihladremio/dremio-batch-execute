package output

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/rsvihladremio/dremio-batch-execute/pkg/conf"
)

var Version = "dev"
var Sha256 = "unknown"

type QueryResults struct {
	Completed int
	Total     int
	Failed    int
}

func LogQueriesCompleted(q QueryResults) {
	percentFailed := (float64(q.Failed) / float64(q.Total)) * 100.0
	completedString := fmt.Sprintf("%v/%v", q.Completed+q.Failed, q.Total)
	log.Printf("%*v - failure rate (%04.1f%%)", len(completedString)+2, completedString, percentFailed)
}

func LogStartMessage(args conf.Args) error {
	log.Printf("dbe version: %v-%v", Version, Sha256)
	log.Printf("parameters")
	log.Printf("----------")
	fullSourcePath, err := filepath.Abs(args.SourceQueryFile)
	if err != nil {
		return err
	}
	log.Printf("source file:     %v", fullSourcePath)
	fullProgressPath, err := filepath.Abs(args.ProgressFilePath)
	if err != nil {
		return err
	}
	log.Printf("progress file:   %v", fullProgressPath)
	log.Printf("url:             %v", args.DremioURL)
	log.Printf("user:            %v", args.DremioUsername)
	masked, err := MaskString(args.DremioPassword)
	if err != nil {
		return err
	}
	log.Printf("pass:            %v", masked)
	log.Printf("timeout:         %v", args.HTTPTimeout)
	log.Printf("request sleep:   %v", args.RequestSleepTime)
	log.Printf("batch size:      %v", args.BatchSize)
	log.Printf("request threads: %v", args.RequestThreads)
	return nil
}

func MaskString(s string) (string, error) {
	var builder strings.Builder
	for _, r := range s {
		_, err := builder.WriteRune(r)
		if err != nil {
			return "", err
		}
	}
	return builder.String(), nil
}
