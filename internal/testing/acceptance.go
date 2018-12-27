package testing

import (
	"net"

	. "github.com/onsi/gomega"
)

func GetFreePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		Expect(err).ToNot(HaveOccurred())
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		Expect(err).ToNot(HaveOccurred())
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
