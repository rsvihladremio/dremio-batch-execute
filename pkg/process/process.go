package process

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/rsvihladremio/dremio-batch-execute/pkg/output"
	"github.com/rsvihladremio/dremio-batch-execute/pkg/progress"
	"github.com/rsvihladremio/dremio-batch-execute/pkg/protocol"
)

func Execute(eng protocol.Engine, sleepTime time.Duration, progressFilePath string, queryPool [][]string) error {
	progressLock := sync.Mutex{}
	requestErrorLock := sync.Mutex{}
	finishLock := sync.Mutex{}
	wg := sync.WaitGroup{}
	kill := false
	var errorMessages []string
	totalQueries := 0

	for _, q := range queryPool {
		totalQueries += len(q)
	}
	completed := 0
	failed := 0
	finish := false
	go func() {
		for {
			time.Sleep(10 * time.Second)
			finishLock.Lock()
			if finish {
				finishLock.Unlock()
				return
			}
			finishLock.Unlock()
			progressLock.Lock()
			output.LogQueriesCompleted(output.QueryResults{
				Total:     totalQueries,
				Failed:    failed,
				Completed: completed,
			})
			progressLock.Unlock()
		}
	}()
	for _, p := range queryPool {
		wg.Add(1)
		go func(queriesForThread []string) {
			defer wg.Done()
			for threadID, q := range queriesForThread {
				err := eng.Execute(q)
				if err != nil {
					log.Printf("error executing '%v' retrying with error: `%v`", q, err)
					err = eng.Execute(q)
					if err != nil {
						requestErrorLock.Lock()
						log.Printf("error executing '%v' with 1 retry due to error `%v`. Skipping query", q, err)
						errorMessages = append(errorMessages, err.Error())
						failed += 1
						requestErrorLock.Unlock()
						continue
					}
				}
				time.Sleep(sleepTime)
				progressLock.Lock()
				if err := progress.MarkQueryComplete(progressFilePath, q); err != nil {
					kill = true
					errorMessages = append(errorMessages, err.Error())
					log.Printf("unable to mark query progress for query `%v` due to error `%v`, exiting, manually add this query to the progress file: %v and run the batch again", q, err, progressFilePath)
				}
				completed += 1
				if kill {
					log.Printf("emergency stopping thread ID: %v", threadID)
					return
				}
				progressLock.Unlock()
			}
		}(p)
	}
	wg.Wait()
	finishLock.Lock()
	finish = true
	finishLock.Unlock()
	output.LogQueriesCompleted(output.QueryResults{
		Total:     totalQueries,
		Failed:    failed,
		Completed: completed,
	})
	if len(errorMessages) == 0 {
		return nil
	}
	return fmt.Errorf("errors during processing: %v", strings.Join(errorMessages, ", "))
}
