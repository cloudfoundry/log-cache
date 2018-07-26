package store_test

import (
	"fmt"
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("store under high concurrent load", func() {
	timeoutInSeconds := 60

	FIt("", func(done Done) {
		var wg sync.WaitGroup

		sp := newSpyPruner()
		sp.result = 100
		sm := newSpyMetrics()

		loadStore := store.NewStore(10000, 5000, sp, sm)
		start := time.Now()

		// 10 writers per sourceId, 10k envelopes per writer
		for sourceId := 0; sourceId < 10; sourceId++ {
			for writers := 0; writers < 10; writers++ {
				wg.Add(1)
				go func(sourceId string) {
					defer wg.Done()

					for envelopes := 0; envelopes < 10000; envelopes++ {
						e := buildTypedEnvelope(time.Now().UnixNano(), sourceId, &loggregator_v2.Log{})
						loadStore.Put(e, sourceId)
						time.Sleep(10 * time.Microsecond)
					}
				}(fmt.Sprintf("index-%d", sourceId))
			}
		}

		for readers := 0; readers < 10; readers++ {
			go func() {
				for i := 0; i < 100; i++ {
					loadStore.Meta()
					time.Sleep(200 * time.Millisecond)
				}
			}()
		}

		go func() {
			wg.Wait()
			fmt.Printf("Finished writing 1M envelopes in %s\n", time.Since(start))
			close(done)
		}()

		Consistently(func() int64 {
			envelopes := loadStore.Get("index-9", start, time.Now(), nil, 100000, false)
			return int64(len(envelopes))
		}, timeoutInSeconds).Should(BeNumerically("<=", 10000))
	}, float64(timeoutInSeconds))
})
