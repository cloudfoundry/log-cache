package replication_test

import (
	"errors"
	"io/ioutil"

	"code.cloudfoundry.org/log-cache/internal/store/replication"

	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ raft.SnapshotStore = &replication.OnDemandSnapshotStore{}

var _ = Describe("OnDemandSnapshotStore", func() {
	var (
		s   *replication.OnDemandSnapshotStore
		cfg raft.Configuration
	)

	BeforeEach(func() {
		s = replication.NewOnDemandSnapshotStore()
		cfg = raft.Configuration{
			Servers: []raft.Server{{ID: "some-id"}},
		}
	})

	It("saves meta from the create", func() {
		list, _ := s.List()
		Expect(list).To(BeEmpty())

		_, _, err := s.Open("on-demand")
		Expect(err).To(HaveOccurred())

		s.Create(1, 4, 3, cfg, 2, stubTransport{})

		list, _ = s.List()
		Expect(list).To(ConsistOf(
			&raft.SnapshotMeta{
				Version:            1,
				ID:                 "on-demand",
				Index:              4,
				Term:               3,
				Configuration:      cfg,
				ConfigurationIndex: 2,
			},
		))
	})

	It("Open will read all the data from the sink", func() {
		sink, err := s.Create(1, 4, 3, cfg, 2, stubTransport{})
		Expect(err).ToNot(HaveOccurred())

		n, err := sink.Write([]byte("hello world"))
		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(11))
		Expect(sink.Close()).To(Succeed())

		m, r, err := s.Open("on-demand")
		Expect(err).ToNot(HaveOccurred())
		Expect(m.ID).To(Equal("on-demand"))

		data, err := ioutil.ReadAll(r)
		Expect(err).ToNot(HaveOccurred())
		Expect(data).To(Equal([]byte("hello world")))
	})

	It("throws data away if the Read isn't keeping up", func() {
		sink, err := s.Create(1, 4, 3, cfg, 2, stubTransport{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() int {
			n, _ := sink.Write([]byte("hello"))
			return n
		}, 1, "1ns").Should(Equal(0))
	})

	It("returns an error if version is not 1", func() {
		_, err := s.Create(2, 0, 0, cfg, 0, stubTransport{})

		Expect(err).To(MatchError(errors.New("raft snapshot version must be 1")))
	})

	It("returns an error from Open for non 'on-demand' id", func() {
		s.Create(1, 0, 0, cfg, 0, stubTransport{})
		_, _, err := s.Open("invalid")
		Expect(err).To(HaveOccurred())
	})

	It("survives the race detector", func() {
		go func() {
			for i := 0; i < 10; i++ {
				s.Create(1, 4, 3, cfg, 2, stubTransport{})
			}
		}()

		go func() {
			for i := 0; i < 10; i++ {
				s.List()
			}
		}()

		for i := 0; i < 10; i++ {
			s.Open("on-demand")
		}
	})
})

type stubTransport struct {
	raft.Transport
}
