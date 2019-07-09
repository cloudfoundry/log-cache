package store_test

import (

	"code.cloudfoundry.org/go-loggregator/metrics/testhelpers"
	"fmt"

	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("store under high concurrent load", func() {
	timeoutInSeconds := 300

	It("", func(done Done) {
		var wg sync.WaitGroup

		sp := newSpyPruner()
		sp.numberToPrune = 128
		sm := testhelpers.NewMetricsRegistry()

		loadStore := store.NewStore(2500, sp, sm)
		start := time.Now()
		var envelopesWritten uint64

		// 10 writers per sourceId, 10k envelopes per writer
		for sourceId := 0; sourceId < 10; sourceId++ {
			for writers := 0; writers < 10; writers++ {
				wg.Add(1)
				go func(sourceId string) {
					defer wg.Done()

					for envelopes := 0; envelopes < 500; envelopes++ {
						e := buildTypedEnvelope(time.Now().UnixNano(), sourceId, &loggregator_v2.Log{})
						loadStore.Put(e, sourceId)
						atomic.AddUint64(&envelopesWritten, 1)
						time.Sleep(50 * time.Microsecond)
					}
				}(fmt.Sprintf("index-%d", sourceId))
			}
		}

		for readers := 0; readers < 10; readers++ {
			go func() {
				for i := 0; i < 100; i++ {
					loadStore.Meta()
					time.Sleep(10 * time.Millisecond)
				}
			}()
		}

		go func() {
			wg.Wait()
			// fmt.Printf("Finished writing %d envelopes in %s\n", atomic.LoadUint64(&envelopesWritten), time.Since(start))
			close(done)
		}()

		Consistently(func() int64 {
			envelopes := loadStore.Get("index-9", start, time.Now(), nil, nil, 100000, false)
			return int64(len(envelopes))
		}, timeoutInSeconds).Should(BeNumerically("<=", 2500))

	}, float64(timeoutInSeconds))
})
