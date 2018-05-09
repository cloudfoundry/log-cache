package promql

import (
	"context"
	"time"

	logcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

type WalkingDataReader struct {
	r logcache.Reader
}

func NewWalkingDataReader(reader logcache.Reader) *WalkingDataReader {
	return &WalkingDataReader{
		r: reader,
	}
}

func (r *WalkingDataReader) Read(
	ctx context.Context,
	in *logcache_v1.ReadRequest,
) (*logcache_v1.ReadResponse, error) {

	var result []*loggregator_v2.Envelope

	logcache.Walk(ctx, in.GetSourceId(), func(es []*loggregator_v2.Envelope) bool {
		result = append(result, es...)
		return true
	}, r.r,
		logcache.WithWalkStartTime(time.Unix(0, in.GetStartTime())),
		logcache.WithWalkEndTime(time.Unix(0, in.GetEndTime())),
		logcache.WithWalkLimit(int(in.GetLimit())),
		logcache.WithWalkEnvelopeTypes(in.GetEnvelopeTypes()...),
		logcache.WithWalkBackoff(logcache.NewRetryBackoffOnErr(time.Second, 5)),
	)

	return &logcache_v1.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: result,
		},
	}, nil
}
