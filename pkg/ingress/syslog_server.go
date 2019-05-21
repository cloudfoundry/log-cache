package ingress

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache"
	"github.com/influxdata/go-syslog"
	"github.com/influxdata/go-syslog/rfc5424"
)

type rfc5424Parser interface {
	Parse(input []byte) (syslog.Message, error)
}

type SysLog struct {
	cache     *cache.LogCache
	source_id string
	parser    rfc5424Parser
}

func NewSyslogServer(cache *cache.LogCache, source_id string) *SysLog {
	return &SysLog{
		cache:     cache,
		source_id: source_id,
		parser:    rfc5424.NewParser(),
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

		r.handleConn(conn)
	}
}

func (r *SysLog) handleConn(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(time.Minute))

	buffer := make([]byte, 1024)
	var msgBuff []byte
	for {
		readcount, err := conn.Read(buffer)
		if err == io.EOF {
			break
		}

		msgBuff = append(msgBuff, buffer[:readcount]...)
	}

	msg, err := r.parser.Parse(msgBuff)
	if err != nil {
		log.Printf("Failed to parse rfc5424 msg: %s", msg)
	}

	sourceType, instanceId := getSourceTypeInstIdFromPID(*msg.ProcID())

	env := &loggregator_v2.Envelope{
		Timestamp:  msg.Timestamp().Unix(),
		InstanceId: instanceId,
		Message: &loggregator_v2.Envelope_Log{
			Log: &loggregator_v2.Log{
				Payload: []byte(*msg.Message()),
				Type:    getTypeFromPriority(*msg.Priority()),
			},
		},
		Tags: map[string]string{
			"source_type": sourceType,
		},
	}

	r.cache.Put(env, r.source_id)
}

func getTypeFromPriority(priority uint8) loggregator_v2.Log_Type {
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
		instanceId = pidToks[1]
	}

	return
}
