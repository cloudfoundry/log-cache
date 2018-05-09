package promql

import (
	"context"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

type EnvelopesDataReader struct {
	m map[string][]*loggregator_v2.Envelope
}

func NewEnvelopesDataReader(es []*loggregator_v2.Envelope) *EnvelopesDataReader {
	m := make(map[string][]*loggregator_v2.Envelope)
	for _, e := range es {
		m[e.GetSourceId()] = append(m[e.GetSourceId()], e)
	}

	return &EnvelopesDataReader{
		m: m,
	}
}

func (r *EnvelopesDataReader) Read(
	ctx context.Context,
	in *logcache_v1.ReadRequest,
) (*logcache_v1.ReadResponse, error) {

	return &logcache_v1.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: r.m[in.GetSourceId()],
		},
	}, nil
}
