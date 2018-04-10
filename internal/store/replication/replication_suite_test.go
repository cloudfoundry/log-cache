package replication_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestReplication(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replication Suite")
}
