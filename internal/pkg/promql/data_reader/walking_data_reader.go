package data_reader

import (
	"context"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/pkg/client"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
)

type WalkingDataReader struct {
	r client.Reader
}

func NewWalkingDataReader(reader client.Reader) *WalkingDataReader {
	return &WalkingDataReader{
		r: reader,
	}
}

func (r *WalkingDataReader) Read(
	ctx context.Context,
	in *logcache_v1.ReadRequest,
) (*logcache_v1.ReadResponse, error) {

	var result []*loggregator_v2.Envelope

	client.Walk(ctx, in.GetSourceId(), func(es []*loggregator_v2.Envelope) bool {
		result = append(result, es...)
		return true
	}, r.r,
		client.WithWalkStartTime(time.Unix(0, in.GetStartTime())),
		client.WithWalkEndTime(time.Unix(0, in.GetEndTime())),
		client.WithWalkLimit(int(in.GetLimit())),
		client.WithWalkEnvelopeTypes(in.GetEnvelopeTypes()...),
		client.WithWalkBackoff(client.NewRetryBackoffOnErr(time.Second, 5)),
	)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &logcache_v1.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: result,
		},
	}, nil
}
