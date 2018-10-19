package promql

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/prometheus/common/model"
)

func ParseStep(param string) (time.Duration, error) {
	if step, err := strconv.ParseFloat(param, 64); err == nil {
		stepInNanoSeconds := step * float64(time.Second)
		if stepInNanoSeconds > float64(math.MaxInt64) || stepInNanoSeconds < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid step. It overflows int64", param)
		}
		return time.Duration(stepInNanoSeconds), nil
	}
	if step, err := ParseDuration(param); err == nil {
		return step, nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid step", param)
}

func ParseDuration(param string) (time.Duration, error) {
	duration, err := model.ParseDuration(param)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q to a valid duration", param)
	}

	return time.Duration(duration), nil
}

func ParseTime(param string) (time.Time, error) {
	if decimalTime, err := strconv.ParseFloat(param, 64); err == nil {
		return time.Unix(0, int64(decimalTime*1e9)), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, param); err == nil {
		return t, nil
	}
	return time.Unix(0, 0), fmt.Errorf("cannot parse %q to a valid Unix or RFC3339 timestamp", param)
}
