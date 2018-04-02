package replication_test

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"code.cloudfoundry.org/log-cache/internal/store/replication"
	replication_v1 "code.cloudfoundry.org/log-cache/internal/store/replication/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("GrpcTransport", func() {
	var (
		a    *replication.GRPCTransport
		addr string
	)

	BeforeEach(func() {
		a = replication.NewGRPCTransport(
			"localhost:9999",
			log.New(GinkgoWriter, "", 0),
			grpc.WithInsecure(),
		)
		addr = startServer(a)
	})

	It("returns the expected address", func() {
		_, port, err := net.SplitHostPort(a.StreamLayerFor("some-id").Addr().String())
		Expect(err).ToNot(HaveOccurred())
		Expect(port).To(Equal("9999"))
	})

	It("Accept return a Conn the stream", func() {
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			// Server side
			conn, err := a.StreamLayerFor("raft-1").Accept()
			Expect(err).ToNot(HaveOccurred())

			buffer := make([]byte, 1024)
			n, err := conn.Read(buffer)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(buffer[:n])).To(Equal("hello"))

			_, err = conn.Write([]byte("goodbye"))
			Expect(err).ToNot(HaveOccurred())
		}()

		// Client side
		conn, err := a.StreamLayerFor("raft-1").Dial(raft.ServerAddress(addr), time.Second)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		_, err = conn.Write([]byte("hello"))
		Expect(err).ToNot(HaveOccurred())

		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		Expect(err).ToNot(HaveOccurred())
		Expect(string(buffer[:n])).To(Equal("goodbye"))
	})

	It("multiplexes based on raft-ID", func() {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		Expect(err).ToNot(HaveOccurred())

		var wg sync.WaitGroup
		wg.Add(2)
		defer wg.Wait()
		go func() {
			defer wg.Done()
			// Server side
			defer GinkgoRecover()
			conn, err := a.StreamLayerFor("raft-1").Accept()
			Expect(err).ToNot(HaveOccurred())

			_, err = conn.Write([]byte("hello-1"))
			Expect(err).ToNot(HaveOccurred())
		}()

		go func() {
			defer wg.Done()
			// Server side
			defer GinkgoRecover()
			conn, err := a.StreamLayerFor("raft-2").Accept()
			Expect(err).ToNot(HaveOccurred())

			_, err = conn.Write([]byte("hello-2"))
			Expect(err).ToNot(HaveOccurred())
		}()

		client1 := replication_v1.NewTransportClient(conn)
		ctx := metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs("raftid", "raft-1"),
		)
		stream1, err := client1.Talk(ctx)
		Expect(err).ToNot(HaveOccurred())

		client2 := replication_v1.NewTransportClient(conn)
		ctx = metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs("raftid", "raft-2"),
		)
		stream2, err := client2.Talk(ctx)
		Expect(err).ToNot(HaveOccurred())

		// raft 1
		msg, err := stream1.Recv()
		Expect(err).ToNot(HaveOccurred())
		Expect(msg.Payload).To(Equal([]byte("hello-1")))

		// raft 2
		msg, err = stream2.Recv()
		Expect(err).ToNot(HaveOccurred())
		Expect(msg.Payload).To(Equal([]byte("hello-2")))
	})

	It("returns an error after closing the conn", func() {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		Expect(err).ToNot(HaveOccurred())

		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		go func() {
			defer wg.Done()
			// Server side
			defer GinkgoRecover()
			conn, err := a.StreamLayerFor("raft-1").Accept()
			Expect(err).ToNot(HaveOccurred())

			conn.Close()

			Eventually(func() error {
				_, err := conn.Write([]byte("goodbye"))
				return err
			}).Should(HaveOccurred())
		}()

		// Client side
		client := replication_v1.NewTransportClient(conn)
		ctx := metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs("raftid", "raft-1"),
		)
		_, err = client.Talk(ctx)
		Expect(err).ToNot(HaveOccurred())
	})
})

func startServer(a *replication.GRPCTransport) string {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()

	replication_v1.RegisterTransportServer(s, a)

	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()

	return lis.Addr().String()
}
