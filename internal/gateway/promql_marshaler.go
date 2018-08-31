package gateway

import (
	"encoding/json"
	"io"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

type PromqlMarshaler struct {
	fallback runtime.Marshaler
}

func NewPromqlMarshaler(fallback runtime.Marshaler) *PromqlMarshaler {
	return &PromqlMarshaler{
		fallback: fallback,
	}
}

func (m *PromqlMarshaler) Marshal(v interface{}) ([]byte, error) {
	switch q := v.(type) {
	case *logcache_v1.PromQL_InstantQueryResult:
		return json.Marshal(m.convertInstantQueryResult(q))
	case *logcache_v1.PromQL_RangeQueryResult:
		return json.Marshal(m.convertRangeQueryResult(q))
	default:
		return m.fallback.Marshal(v)
	}
}

type queryResult struct {
	Status string     `json:"status"`
	Data   resultData `json:"data"`
}

type resultData struct {
	ResultType string        `json:"resultType"`
	Result     []interface{} `json:"result,omitempty"`
}

type sample struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
}

type series struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

func (m *PromqlMarshaler) convertInstantQueryResult(v *logcache_v1.PromQL_InstantQueryResult) *queryResult {
	var data resultData
	switch v.GetResult().(type) {
	case *logcache_v1.PromQL_InstantQueryResult_Scalar:
		data = convertScalarResultData(v.GetScalar())
	case *logcache_v1.PromQL_InstantQueryResult_Vector:
		data = convertVectorResultData(v.GetVector())
	case *logcache_v1.PromQL_InstantQueryResult_Matrix:
		data = convertMatrixResultData(v.GetMatrix())
	}

	return &queryResult{
		Status: "success",
		Data:   data,
	}
}

func (m *PromqlMarshaler) convertRangeQueryResult(v *logcache_v1.PromQL_RangeQueryResult) *queryResult {
	var data resultData
	switch v.GetResult().(type) {
	case *logcache_v1.PromQL_RangeQueryResult_Matrix:
		data = convertMatrixResultData(v.GetMatrix())
	}

	return &queryResult{
		Status: "success",
		Data:   data,
	}
}

func convertScalarResultData(v *logcache_v1.PromQL_Scalar) resultData {
	return resultData{
		ResultType: "scalar",
		Result:     []interface{}{v.GetTime(), v.GetValue()},
	}
}

func convertVectorResultData(v *logcache_v1.PromQL_Vector) resultData {
	var samples []interface{}

	for _, s := range v.GetSamples() {
		p := s.GetPoint()
		samples = append(samples, sample{
			Metric: s.GetMetric(),
			Value:  []interface{}{p.GetTime(), p.GetValue()},
		})
	}

	return resultData{
		ResultType: "vector",
		Result:     samples,
	}
}

func convertMatrixResultData(v *logcache_v1.PromQL_Matrix) resultData {
	var result []interface{}

	for _, s := range v.GetSeries() {
		var values [][]interface{}
		for _, p := range s.GetPoints() {
			values = append(values, []interface{}{p.GetTime(), p.GetValue()})
		}
		result = append(result, series{
			Metric: s.GetMetric(),
			Values: values,
		})
	}

	return resultData{
		ResultType: "matrix",
		Result:     result,
	}
}

func (m *PromqlMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	fallbackEncoder := m.fallback.NewEncoder(w)
	jsonEncoder := json.NewEncoder(w)

	return runtime.EncoderFunc(func(v interface{}) error {
		switch q := v.(type) {
		case *logcache_v1.PromQL_InstantQueryResult:
			return jsonEncoder.Encode(m.convertInstantQueryResult(q))
		case *logcache_v1.PromQL_RangeQueryResult:
			return jsonEncoder.Encode(m.convertRangeQueryResult(q))
		default:
			return fallbackEncoder.Encode(v)
		}
	})
}

// The special marshaling for PromQL results is currently only implemented
// for encoding.
func (m *PromqlMarshaler) Unmarshal(data []byte, v interface{}) error {
	return m.fallback.Unmarshal(data, v)
}

func (m *PromqlMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return m.fallback.NewDecoder(r)
}

func (m *PromqlMarshaler) ContentType() string {
	return `application/json`
}
