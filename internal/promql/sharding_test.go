package promql_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/promql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ logcache_v1.PromQLShardReaderServer = &promql.Sharding{}

var _ = Describe("Sharding", func() {
	var (
		s          *promql.Sharding
		spyReader  *spyShardGroupReaderServer
		spyParser  *spyParser
		spyQuerier *spyQuerier
	)

	BeforeEach(func() {
		spyReader = newSpyShardGroupReaderServer()
		spyParser = newSpyParser()
		spyQuerier = newSpyQuerier()
		s = promql.NewSharding(spyParser, spyQuerier, spyReader)
	})

	It("sets the group with the given source IDs and query for an arg", func() {
		spyParser.sourceIDs = []string{"a", "b"}

		query := `metric{source_id="a"}+metric{source_id="b"}`
		arg := "some-arg"
		_, err := s.SetShardPromQL(context.Background(), &logcache_v1.PromQL_SetShardRequest{
			Name: "some-name",
			Query: &logcache_v1.PromQL_SetShardQuery{
				Query: query,
				Arg:   arg,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyParser.query).To(Equal(query))

		Expect(spyReader.setRequests).To(ConsistOf(&logcache_v1.SetShardGroupRequest{
			Name: "some-name",
			SubGroup: &logcache_v1.GroupedSourceIds{
				SourceIds: []string{"a", "b"},
				Arg:       fmt.Sprintf(`{"query":%q,"arg":%q}`, query, arg),
			},
		}))
	})

	It("returns an error if the query fails to parse", func() {
		spyParser.err = errors.New("some-error")

		_, err := s.SetShardPromQL(context.Background(), &logcache_v1.PromQL_SetShardRequest{
			Name: "some-name",
			Query: &logcache_v1.PromQL_SetShardQuery{
				Query: `invalid`,
			},
		})
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("returns an error if setting the group fails", func() {
		spyReader.setErrs = []error{errors.New("some-error")}

		_, err := s.SetShardPromQL(context.Background(), &logcache_v1.PromQL_SetShardRequest{
			Name: "some-name",
			Query: &logcache_v1.PromQL_SetShardQuery{
				Query: `invalid`,
			},
		})
		Expect(err).To(HaveOccurred())
	})

	It("Read uses the returned arg and data to execute the query", func() {
		spyReader.readErrs = []error{nil}
		spyReader.readResponses = []*logcache_v1.ShardGroupReadResponse{
			{
				Args: []string{
					`{"query":"query-1","arg":"arg-1"}`,
					`{"query":"query-2","arg":"arg-2"}`,
				},
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{Timestamp: 1},
						{Timestamp: 2},
					},
				},
			},
		}

		spyQuerier.results["query-1"] = &logcache_v1.PromQL_QueryResult{
			Result: &logcache_v1.PromQL_QueryResult_Scalar{
				Scalar: &logcache_v1.PromQL_Scalar{
					Value: 99,
				},
			},
		}

		spyQuerier.results["query-2"] = &logcache_v1.PromQL_QueryResult{
			Result: &logcache_v1.PromQL_QueryResult_Scalar{
				Scalar: &logcache_v1.PromQL_Scalar{
					Value: 101,
				},
			},
		}

		result, err := s.ReadPromQL(context.Background(), &logcache_v1.PromQL_ShardReadRequest{
			Name:        "some-name",
			RequesterId: 99,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(result.GetResults()["arg-1"].GetScalar().GetValue()).To(Equal(99.0))
		Expect(result.GetResults()["arg-2"].GetScalar().GetValue()).To(Equal(101.0))

		Expect(spyReader.readRequests).To(HaveLen(1))
		Expect(spyReader.readRequests[0].EndTime - spyReader.readRequests[0].StartTime).To(Equal(int64(time.Hour)))
		Expect(time.Unix(0, spyReader.readRequests[0].EndTime)).To(BeTemporally("~", time.Now()))

		Expect(spyReader.readRequests).To(ConsistOf(&logcache_v1.ShardGroupReadRequest{
			Name:        "some-name",
			RequesterId: 99,
			StartTime:   spyReader.readRequests[0].StartTime,
			EndTime:     spyReader.readRequests[0].EndTime,
			EnvelopeTypes: []logcache_v1.EnvelopeType{
				logcache_v1.EnvelopeType_COUNTER,
				logcache_v1.EnvelopeType_GAUGE,
				logcache_v1.EnvelopeType_TIMER,
			},
		}))

		Expect(spyQuerier.queries).To(ConsistOf(
			"query-1",
			"query-2",
		))

		Expect(spyQuerier.envelopes).To(ContainElement([]*loggregator_v2.Envelope{
			{Timestamp: 1},
			{Timestamp: 2},
		}))
	})

	It("returns an error for Read when the underlying Read returns an error", func() {
		spyReader.readErrs = []error{errors.New("some-error")}
		spyReader.readResponses = []*logcache_v1.ShardGroupReadResponse{nil}

		_, err := s.ReadPromQL(context.Background(), &logcache_v1.PromQL_ShardReadRequest{
			Name:        "some-name",
			RequesterId: 99,
		})
		Expect(err).To(MatchError("some-error"))
	})

	It("returns an error for Read when the executing the query fails", func() {
		spyReader.readErrs = []error{nil}
		spyReader.readResponses = []*logcache_v1.ShardGroupReadResponse{
			{
				Args: []string{
					`{"query":"query-1","arg":"arg-1"}`,
					`{"query":"query-2","arg":"arg-2"}`,
				},
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{Timestamp: 1},
						{Timestamp: 2},
					},
				},
			},
		}

		_, err := s.ReadPromQL(context.Background(), &logcache_v1.PromQL_ShardReadRequest{
			Name:        "some-name",
			RequesterId: 99,
		})
		Expect(err).To(MatchError("unknown query"))
	})

	It("uses the ShardGroup endpoint to return data for ShardPromQL", func() {
		spyReader.shardErrs = []error{nil}
		spyReader.shardResponses = []*logcache_v1.ShardGroupResponse{
			{
				RequesterIds: []uint64{99, 101},
				Args: []string{
					`{"query":"some-query-1","arg":"some-arg-1"}`,
					`{"query":"some-query-2","arg":"some-arg-2"}`,
				},
			},
		}

		resp, err := s.ShardPromQL(context.Background(), &logcache_v1.PromQL_ShardRequest{
			Name: "some-name",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.GetRequesterIds()).To(ConsistOf(uint64(99), uint64(101)))
		Expect(resp.GetQueries()).To(ConsistOf("some-query-1", "some-query-2"))
		Expect(resp.GetArgs()).To(ConsistOf("some-arg-1", "some-arg-2"))

		Expect(spyReader.shardRequests).To(ConsistOf(&logcache_v1.ShardGroupRequest{
			Name: "some-name",
		}))
	})

	It("returns an error if ShardGroup returns an error", func() {
		spyReader.shardErrs = []error{errors.New("some-error")}
		spyReader.shardResponses = []*logcache_v1.ShardGroupResponse{nil}

		_, err := s.ShardPromQL(context.Background(), &logcache_v1.PromQL_ShardRequest{
			Name: "some-name",
		})
		Expect(err).To(MatchError("some-error"))
	})

	It("returns an error if ShardGroup returns a malformed arg", func() {
		spyReader.shardErrs = []error{nil}
		spyReader.shardResponses = []*logcache_v1.ShardGroupResponse{
			{
				RequesterIds: []uint64{99, 101},
				Args: []string{
					"invalid",
				},
			},
		}

		_, err := s.ShardPromQL(context.Background(), &logcache_v1.PromQL_ShardRequest{
			Name: "some-name",
		})
		Expect(err).To(HaveOccurred())
	})
})

type spyShardGroupReaderServer struct {
	setRequests []*logcache_v1.SetShardGroupRequest
	setErrs     []error

	readRequests  []*logcache_v1.ShardGroupReadRequest
	readResponses []*logcache_v1.ShardGroupReadResponse
	readErrs      []error

	shardRequests  []*logcache_v1.ShardGroupRequest
	shardResponses []*logcache_v1.ShardGroupResponse
	shardErrs      []error
}

func newSpyShardGroupReaderServer() *spyShardGroupReaderServer {
	return &spyShardGroupReaderServer{}
}

func (s *spyShardGroupReaderServer) SetShardGroup(ctx context.Context, r *logcache_v1.SetShardGroupRequest) (*logcache_v1.SetShardGroupResponse, error) {
	s.setRequests = append(s.setRequests, r)
	if len(s.setErrs) == 0 {
		return &logcache_v1.SetShardGroupResponse{}, nil
	}

	err := s.setErrs[0]
	s.setErrs = s.setErrs[1:]

	return &logcache_v1.SetShardGroupResponse{}, err
}

func (s *spyShardGroupReaderServer) Read(ctx context.Context, r *logcache_v1.ShardGroupReadRequest) (*logcache_v1.ShardGroupReadResponse, error) {
	s.readRequests = append(s.readRequests, r)
	if len(s.readErrs) != len(s.readResponses) {
		panic("out of sync")
	}

	if len(s.readErrs) == 0 {
		return &logcache_v1.ShardGroupReadResponse{}, nil
	}

	err := s.readErrs[0]
	s.readErrs = s.readErrs[1:]
	resp := s.readResponses[0]
	s.readResponses = s.readResponses[1:]

	return resp, err
}

func (s *spyShardGroupReaderServer) ShardGroup(ctx context.Context, r *logcache_v1.ShardGroupRequest) (*logcache_v1.ShardGroupResponse, error) {
	s.shardRequests = append(s.shardRequests, r)
	if len(s.shardErrs) != len(s.shardResponses) {
		panic("out of sync")
	}

	if len(s.shardErrs) == 0 {
		return &logcache_v1.ShardGroupResponse{}, nil
	}

	err := s.shardErrs[0]
	s.shardErrs = s.shardErrs[1:]
	resp := s.shardResponses[0]
	s.shardResponses = s.shardResponses[1:]

	return resp, err
}

type spyParser struct {
	query     string
	sourceIDs []string
	err       error
}

func newSpyParser() *spyParser {
	return &spyParser{}
}

func (s *spyParser) Parse(query string) ([]string, error) {
	s.query = query
	return s.sourceIDs, s.err
}

type spyQuerier struct {
	queries   []string
	envelopes [][]*loggregator_v2.Envelope

	results map[string]*logcache_v1.PromQL_QueryResult
}

func newSpyQuerier() *spyQuerier {
	return &spyQuerier{
		results: make(map[string]*logcache_v1.PromQL_QueryResult),
	}
}

func (s *spyQuerier) InstantQuery(ctx context.Context, query string, e []*loggregator_v2.Envelope) (*logcache_v1.PromQL_QueryResult, error) {
	s.queries = append(s.queries, query)
	s.envelopes = append(s.envelopes, e)

	result, ok := s.results[query]
	if !ok {
		return nil, errors.New("unknown query")
	}

	return result, nil
}
