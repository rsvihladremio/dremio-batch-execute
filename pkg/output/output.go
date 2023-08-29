package output

import (
	"fmt"
	"log"
)

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
