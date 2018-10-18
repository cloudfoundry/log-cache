package expvar_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestExpvar(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Expvar Suite")
}
