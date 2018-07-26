package logcache_test

import (
	"crypto/tls"
	"net"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ShardGroupReader", func() {
	var (
		g  *logcache.ShardGroupReader
		c  rpc.ShardGroupReaderClient
		oc rpc.OrchestrationClient

		spy         *spyShardGroupReader
		spyLogCache *spyLogCache
	)

	BeforeEach(func() {
		tlsConfig, err := newTLSConfig(
			Cert("log-cache-ca.crt"),
			Cert("log-cache.crt"),
			Cert("log-cache.key"),
			"log-cache",
		)
		Expect(err).ToNot(HaveOccurred())

		spy = newSpyGroupReader(tlsConfig)
		spyAddr := spy.start()

		spyLogCache = newSpyLogCache(tlsConfig)
		logCacheAddr := spyLogCache.start()

		g = logcache.NewGroupReader(
			logCacheAddr,
			[]string{"127.0.0.1:0", spyAddr},
			0,
			logcache.WithGroupReaderServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithGroupReaderDialOpts(
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
		)
		g.Start()

		c, oc = newShardGroupReaderClient(g.Addr(), tlsConfig)

		_, err = oc.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 0,
				End:   9223372036854775807,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = oc.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				g.Addr(): {
					Ranges: []*rpc.Range{
						{
							Start: 0,
							End:   9223372036854775807,
						},
					},
				},
				spyAddr: {
					Ranges: []*rpc.Range{
						{
							Start: 9223372036854775808,
							End:   18446744073709551615,
						},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("reads data from a group of sourceIDs", func() {
		spyLogCache.readEnvelopes["source-0"] = func() []*loggregator_v2.Envelope {
			return []*loggregator_v2.Envelope{
				{Timestamp: 98},
				{Timestamp: 99},
				{Timestamp: 101},
			}
		}

		spyLogCache.readEnvelopes["source-1"] = func() []*loggregator_v2.Envelope {
			return []*loggregator_v2.Envelope{
				{Timestamp: 100},
				{Timestamp: 102},
				{Timestamp: 103},
			}
		}

		_, err := c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-a",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"source-0"},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-a",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"source-1"},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() []int64 {
			resp, err := c.Read(context.Background(), &rpc.ShardGroupReadRequest{
				Name: "some-name-a",

				// [99,103)
				StartTime: 99,
				EndTime:   103,
			})
			Expect(err).ToNot(HaveOccurred())

			var result []int64
			for _, e := range resp.Envelopes.Batch {
				result = append(result, e.GetTimestamp())
			}

			return result
		}).Should(Equal([]int64{99, 100, 101, 102}))
	})

	XIt("shards data from a group of sourceIDs", func() {
		i := int64(99)
		j := int64(100)

		spyLogCache.readEnvelopes["source-0"] = func() []*loggregator_v2.Envelope {
			i += 2
			return []*loggregator_v2.Envelope{
				{Timestamp: i},
			}
		}

		spyLogCache.readEnvelopes["source-1"] = func() []*loggregator_v2.Envelope {
			j += 2
			return []*loggregator_v2.Envelope{
				{Timestamp: j},
			}
		}

		_, err := c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-a",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"source-0"},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-a",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"source-1"},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		var wg sync.WaitGroup
		defer wg.Wait()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			Eventually(func() []int64 {
				resp, err := c.Read(context.Background(), &rpc.ShardGroupReadRequest{
					Name:        "some-name-a",
					RequesterId: 1,
				})
				Expect(err).ToNot(HaveOccurred())

				var result []int64
				for _, e := range resp.Envelopes.Batch {
					result = append(result, e.GetTimestamp()%2)
				}

				return result
			}, 3).Should(Or(
				// Should only be odd or only be even
				And(ContainElement(int64(0)), Not(ContainElement(int64(1)))),
				And(Not(ContainElement(int64(0))), ContainElement(int64(1))),
			))
		}()

		Eventually(func() []int64 {
			resp, err := c.Read(context.Background(), &rpc.ShardGroupReadRequest{
				Name:        "some-name-a",
				RequesterId: 0,
			})
			Expect(err).ToNot(HaveOccurred())

			var result []int64
			for _, e := range resp.Envelopes.Batch {
				result = append(result, e.GetTimestamp()%2)
			}

			return result
		}, 3).Should(Or(
			// Should only be odd or only be even
			And(ContainElement(int64(0)), Not(ContainElement(int64(1)))),
			And(Not(ContainElement(int64(0))), ContainElement(int64(1))),
		))
	})

	It("keeps track of groups", func() {
		_, err := c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-a",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"some-id"},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-b",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"some-other-id"},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-a",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"some-other-id"},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := c.ShardGroup(context.Background(), &rpc.ShardGroupRequest{
			Name: "some-name-a",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.SubGroups).To(ConsistOf(
			&rpc.GroupedSourceIds{SourceIds: []string{"some-id"}},
			&rpc.GroupedSourceIds{SourceIds: []string{"some-other-id"}},
		))

		resp, err = c.ShardGroup(context.Background(), &rpc.ShardGroupRequest{
			Name: "some-name-b",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.SubGroups).To(ConsistOf(
			&rpc.GroupedSourceIds{SourceIds: []string{"some-other-id"}},
		))
	})

	It("routes requests to the correct node", func() {
		// some-name-a hashes to 4464231820929349922 (node 0)
		_, err := c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-a",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"some-id"},
			},
		})

		Expect(err).ToNot(HaveOccurred())
		Consistently(spy.AddRequests).Should(BeEmpty())

		// some-name-c hashes to 14515125134919833977 (node 1)
		_, err = c.SetShardGroup(context.Background(), &rpc.SetShardGroupRequest{
			Name: "some-name-c",
			SubGroup: &rpc.GroupedSourceIds{
				SourceIds: []string{"some-id"},
			},
		})

		Expect(err).ToNot(HaveOccurred())
		Eventually(spy.AddRequests).Should(HaveLen(1))
		Expect(spy.AddRequests()[0].Name).To(Equal("some-name-c"))
		Expect(spy.AddRequests()[0].GetSubGroup().GetSourceIds()).To(ConsistOf("some-id"))

		resp, err := c.Read(context.Background(), &rpc.ShardGroupReadRequest{
			Name: "some-name-c",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Envelopes.Batch).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 1},
			&loggregator_v2.Envelope{Timestamp: 2},
		))
	})

	It("lists ranges", func() {
		resp, err := oc.ListRanges(context.Background(), &rpc.ListRangesRequest{})

		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Ranges).To(ConsistOf([]*rpc.Range{
			{
				Start: 0,
				End:   9223372036854775807,
			},
		}))
	})
})

func newShardGroupReaderClient(addr string, tlsConfig *tls.Config) (rpc.ShardGroupReaderClient, rpc.OrchestrationClient) {
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	)
	if err != nil {
		panic(err)
	}

	return rpc.NewShardGroupReaderClient(conn), rpc.NewOrchestrationClient(conn)
}

type spyShardGroupReader struct {
	mu        sync.Mutex
	addReqs   []*rpc.SetShardGroupRequest
	readReqs  []*rpc.ShardGroupReadRequest
	tlsConfig *tls.Config
}

func newSpyGroupReader(tlsConfig *tls.Config) *spyShardGroupReader {
	return &spyShardGroupReader{tlsConfig: tlsConfig}
}

func (s *spyShardGroupReader) start() string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	go func() {
		srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(s.tlsConfig)))

		rpc.RegisterShardGroupReaderServer(srv, s)
		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}()

	return lis.Addr().String()
}

func (s *spyShardGroupReader) SetShardGroup(c context.Context, r *rpc.SetShardGroupRequest) (*rpc.SetShardGroupResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addReqs = append(s.addReqs, r)
	return &rpc.SetShardGroupResponse{}, nil
}

func (s *spyShardGroupReader) AddRequests() []*rpc.SetShardGroupRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.SetShardGroupRequest, len(s.addReqs))
	copy(r, s.addReqs)
	return r
}

func (s *spyShardGroupReader) Read(c context.Context, r *rpc.ShardGroupReadRequest) (*rpc.ShardGroupReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readReqs = append(s.readReqs, r)

	return &rpc.ShardGroupReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: []*loggregator_v2.Envelope{
				{Timestamp: 1},
				{Timestamp: 2},
			},
		},
	}, nil
}

func (s *spyShardGroupReader) getReadRequests() []*rpc.ShardGroupReadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.ShardGroupReadRequest, len(s.readReqs))
	copy(r, s.readReqs)

	return r
}

func (s *spyShardGroupReader) ShardGroup(context.Context, *rpc.ShardGroupRequest) (*rpc.ShardGroupResponse, error) {
	panic("not implemented")
}
