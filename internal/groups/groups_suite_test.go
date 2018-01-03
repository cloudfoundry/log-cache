package groups_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGroups(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Groups Suite")
}
