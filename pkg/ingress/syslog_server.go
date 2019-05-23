package ingress

import (
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"code.cloudfoundry.org/rfc5424"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache"
)

var readDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name: "syslog_read_duration",
})

var convertDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name: "syslog_convert_duration",
})

var putDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name: "syslog_put_duration",
})

type SysLog struct {
	cache        *cache.LogCache
	incIngress   func(delta uint64)
	setConnCount func(value float64)
	connCount    *int64
}

func NewSyslogServer(cache *cache.LogCache, m *metrics.Metrics) *SysLog {
	m.Registry.MustRegister(readDuration, putDuration, convertDuration)
	incIngress := m.NewCounter("syslog_ingress")
	setConnCount := m.NewGauge("syslog_connections", "connections")
	connCount := int64(0)

	return &SysLog{
		cache:        cache,
		incIngress:   incIngress,
		setConnCount: setConnCount,
		connCount:    &connCount,
	}
}

func (r *SysLog) Start(port string) {
	addr := fmt.Sprintf(":%s", port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	log.Printf("syslog server listening on: %s", lis.Addr())

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatalf("failed to accept: %s", err)
		}

		count := atomic.AddInt64(r.connCount, 1)
		r.setConnCount(float64(count))
		r.handleConn(conn)
	}
}

func (r *SysLog) handleConn(conn net.Conn) {
	done := make(chan struct{})
	envs := make(chan *loggregator_v2.Envelope, 1024)

	go r.readToChannel(conn, envs, done)
	go r.writeFromChannel(envs, done)
}

func (r *SysLog) readToChannel(conn net.Conn, envs chan *loggregator_v2.Envelope, done chan struct{}) {
	defer func() {
		close(done)
		conn.Close()
		count := atomic.AddInt64(r.connCount, -1)
		r.setConnCount(float64(count))
	}()
	conn.SetReadDeadline(time.Now().Add(time.Minute))

	for {
		var msg rfc5424.Message
		var err error
		duration(readDuration, func() error {
			_, err = msg.ReadFrom(conn)
			return err
		})

		if err != nil {
			if err == io.EOF {
				log.Println("Got EOF")
				return
			}
			log.Printf("ReadFrom err: %s", err)
			return
		}

		if len(envs) > 1000 {
			log.Printf("channel is filling up %d", len(envs))
		}

		envs <- convertSyslogToEnvelope(msg)
		r.incIngress(1)
	}
}

func convertSyslogToEnvelope(msg rfc5424.Message) *loggregator_v2.Envelope {
	sourceType, instanceId := getSourceTypeInstIdFromPID(msg.ProcessID)
	return &loggregator_v2.Envelope{
		SourceId:   "syslog_" + msg.AppName,
		Timestamp:  msg.Timestamp.UnixNano(),
		InstanceId: instanceId,
		Message: &loggregator_v2.Envelope_Log{
			Log: &loggregator_v2.Log{
				Payload: []byte(msg.Message),
				Type:    getTypeFromPriority(int(msg.Priority)),
			},
		},
		Tags: map[string]string{
			"source_type": sourceType,
		},
	}
}

func (r *SysLog) writeFromChannel(envs chan *loggregator_v2.Envelope, done chan struct{}) {
	defer log.Println("exiting flushing routine")
	envelopes := map[string][]*loggregator_v2.Envelope{}
	ticker := time.Tick(500 * time.Millisecond)

	for {
		select {
		case <-ticker:
			r.put(envelopes)
			envelopes = map[string][]*loggregator_v2.Envelope{}
		case e := <-envs:
			envsForSource, ok := envelopes[e.SourceId]
			if !ok {
				envsForSource = []*loggregator_v2.Envelope{}
			}

			envelopes[e.SourceId] = append(envsForSource, e)
			if len(envelopes[e.SourceId]) > 1000 {
				r.put(envelopes)
				envelopes = map[string][]*loggregator_v2.Envelope{}
			}
		case <-done:
			close(envs)
			for e := range envs {
				envsForSource, ok := envelopes[e.SourceId]
				if !ok {
					envsForSource = []*loggregator_v2.Envelope{}
				}
				envelopes[e.SourceId] = append(envsForSource, e)
			}
			r.put(envelopes)
			return
		}
	}
}

func (r *SysLog) put(envsMapThing map[string][]*loggregator_v2.Envelope) {
	if len(envsMapThing) == 0 {
		return
	}

	count := 0
	multiDuration(putDuration, func() int {
		wg := &sync.WaitGroup{}
		for sourceID, envelopesForOneSourceID := range envsMapThing {
			envCount := len(envelopesForOneSourceID)
			if envCount == 0 {
				continue
			}

			count += envCount

			wg.Add(1)
			go func(sourceID string, envelopes []*loggregator_v2.Envelope) {
				defer wg.Done()
				r.cache.Put(envelopes, sourceID)
			}(sourceID, envelopesForOneSourceID)
		}
		wg.Wait()
		return count
	})
}

func getTypeFromPriority(priority int) loggregator_v2.Log_Type {
	if priority == 11 {
		return loggregator_v2.Log_ERR
	}

	return loggregator_v2.Log_OUT
}

func getSourceTypeInstIdFromPID(pid string) (sourceType, instanceId string) {
	pid = strings.Trim(pid, "[]")

	pidToks := strings.Split(pid, "/")
	sourceType = pidToks[0]

	if len(pidToks) > 1 {
		instanceId = pidToks[len(pidToks)-1]
	} else {
		instanceId = "UNKNOWN"
	}

	return
}

func duration(h prometheus.Histogram, f func() error) {
	start := time.Now()
	err := f()
	end := time.Now()

	if err == nil {
		h.Observe(end.Sub(start).Seconds())
	}
}

func multiDuration(h prometheus.Histogram, f func() int) {
	start := time.Now()
	count := f()
	end := time.Now()

	duration := end.Sub(start).Seconds()
	for i := 0; i < count; i++ {
		h.Observe(duration / float64(count))
	}
}
