package testing

import (
	"fmt"
	"strconv"
	"time"

	. "github.com/onsi/gomega"
)

func FormatTimeWithDecimalMillis(t time.Time) string {
	return fmt.Sprintf("%.3f", float64(t.UnixNano())/1e9)
}

func ParseTimeWithDecimalMillis(t string) time.Time {
	decimalTime, err := strconv.ParseFloat(t, 64)
	Expect(err).ToNot(HaveOccurred())
	return time.Unix(0, int64(decimalTime*1e9))
}
