package cache_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLogCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LogCache Suite")
}
