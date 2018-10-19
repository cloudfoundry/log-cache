package end2end_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestEnd2end(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "End2end Suite")
}
