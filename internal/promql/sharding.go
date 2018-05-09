package promql

import (
	"context"
	"encoding/json"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Sharding struct {
	s logcache_v1.ShardGroupReaderServer
	p Parser
	q Querier
}

type Parser interface {
	Parse(query string) ([]string, error)
}

type Querier interface {
	InstantQuery(
		ctx context.Context,
		query string,
		envelopes []*loggregator_v2.Envelope,
	) (*logcache_v1.PromQL_QueryResult, error)
}

type QuerierFunc func(
	ctx context.Context,
	query string,
	envelopes []*loggregator_v2.Envelope,
) (*logcache_v1.PromQL_QueryResult, error)

func (f QuerierFunc) InstantQuery(
	ctx context.Context,
	query string,
	envelopes []*loggregator_v2.Envelope,
) (*logcache_v1.PromQL_QueryResult, error) {
	return f(ctx, query, envelopes)
}

func NewSharding(
	p Parser,
	q Querier,
	s logcache_v1.ShardGroupReaderServer,
) *Sharding {
	return &Sharding{
		s: s,
		q: q,
		p: p,
	}
}

func (s *Sharding) SetShardPromQL(
	ctx context.Context,
	r *logcache_v1.PromQL_SetShardRequest,
) (*logcache_v1.PromQL_SetShardResponse, error) {

	sIDs, err := s.p.Parse(r.GetQuery().GetQuery())
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	argData, err := json.Marshal(struct {
		Query string `json:"query"`
		Arg   string `json:"arg"`
	}{
		Query: r.GetQuery().GetQuery(),
		Arg:   r.GetQuery().GetArg(),
	})
	if err != nil {
		return nil, err
	}

	_, err = s.s.SetShardGroup(ctx, &logcache_v1.SetShardGroupRequest{
		Name: r.GetName(),
		SubGroup: &logcache_v1.GroupedSourceIds{
			SourceIds: sIDs,
			Arg:       string(argData),
		},
	})

	if err != nil {
		return nil, err
	}

	return &logcache_v1.PromQL_SetShardResponse{}, nil
}

func (s *Sharding) ReadPromQL(
	ctx context.Context,
	r *logcache_v1.PromQL_ShardReadRequest,
) (*logcache_v1.PromQL_ShardReadResponse, error) {

	now := time.Now()
	resp, err := s.s.Read(ctx, &logcache_v1.ShardGroupReadRequest{
		Name:        r.GetName(),
		RequesterId: r.GetRequesterId(),
		EnvelopeTypes: []logcache_v1.EnvelopeType{
			logcache_v1.EnvelopeType_COUNTER,
			logcache_v1.EnvelopeType_GAUGE,
			logcache_v1.EnvelopeType_TIMER,
		},
		StartTime: now.Add(-time.Hour).UnixNano(),
		EndTime:   now.UnixNano(),
	})
	if err != nil {
		return nil, err
	}

	m := make(map[string]*logcache_v1.PromQL_QueryResult)
	for _, arg := range resp.GetArgs() {

		a := struct {
			Query string `json:"query"`
			Arg   string `json:"arg"`
		}{}
		if err := json.Unmarshal([]byte(arg), &a); err != nil {
			return nil, err
		}

		result, err := s.q.InstantQuery(ctx, a.Query, resp.GetEnvelopes().GetBatch())
		if err != nil {
			return nil, err
		}

		m[a.Arg] = result
	}

	return &logcache_v1.PromQL_ShardReadResponse{
		Results: m,
	}, nil
}

func (s *Sharding) ShardPromQL(
	ctx context.Context,
	r *logcache_v1.PromQL_ShardRequest,
) (*logcache_v1.PromQL_ShardResponse, error) {

	resp, err := s.s.ShardGroup(ctx, &logcache_v1.ShardGroupRequest{
		Name: r.GetName(),
	})
	if err != nil {
		return nil, err
	}

	var queries []string
	var args []string
	for _, arg := range resp.GetArgs() {
		a := struct {
			Query string `json:"query"`
			Arg   string `json:"arg"`
		}{}
		if err := json.Unmarshal([]byte(arg), &a); err != nil {
			return nil, err
		}

		queries = append(queries, a.Query)
		args = append(args, a.Arg)
	}

	return &logcache_v1.PromQL_ShardResponse{
		RequesterIds: resp.GetRequesterIds(),
		Queries:      queries,
		Args:         args,
	}, nil
}
