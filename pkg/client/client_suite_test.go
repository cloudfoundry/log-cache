package client_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

// TODO - the tests here are not actually ginkgo; this is just to get
// scripts/test to work temporarily, and not fail on an unknown ginkgo flag
// [#160701220]

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client Suite")
}
