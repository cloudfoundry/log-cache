package promql_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestPromql(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Promql Suite")
}
