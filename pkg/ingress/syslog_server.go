package ingress

import (
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"code.cloudfoundry.org/rfc5424"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache"
)

type SysLog struct {
	cache        *cache.LogCache
	incIngress   func(delta uint64)
	setConnCount func(value float64)
	connCount    *int64
}

func NewSyslogServer(cache *cache.LogCache, m *metrics.Metrics) *SysLog {
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
		go r.handleConn(conn)
	}
}

func (r *SysLog) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		count := atomic.AddInt64(r.connCount, -1)
		r.setConnCount(float64(count))
	}()
	conn.SetReadDeadline(time.Now().Add(time.Minute))

	for {
		var msg rfc5424.Message
		_, err := msg.ReadFrom(conn)
		if err != nil {
			if err == io.EOF {
				log.Println("Got EOF")
				return
			}
			log.Printf("ReadFrom err: %s", err)
			return
		}
		r.incIngress(1)

		log.Printf("msg: %+v\n", msg)
		sourceType, instanceId := getSourceTypeInstIdFromPID(msg.ProcessID)

		env := &loggregator_v2.Envelope{
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

		r.cache.Put(env, "syslog_"+msg.AppName)
	}
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
