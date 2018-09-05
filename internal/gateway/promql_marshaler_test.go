package gateway_test

import (
	"bytes"
	"errors"
	"io"
	"strings"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache/internal/gateway"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PromqlMarshaler", func() {
	Context("Marshal()", func() {
		It("marshals a scalar instant query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			result, err := marshaler.Marshal(&logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Scalar{
					Scalar: &logcache_v1.PromQL_Scalar{
						Time:  1,
						Value: 2.5,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(MatchJSON(`{
				"status": "success",
				"data": {
					"resultType": "scalar",
					"result": [1, "2.5"]
				}
			}`))
		})

		It("marshals a vector instant query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			result, err := marshaler.Marshal(&logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Vector{
					Vector: &logcache_v1.PromQL_Vector{
						Samples: []*logcache_v1.PromQL_Sample{
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name":   "tag-value",
								},
								Point: &logcache_v1.PromQL_Point{
									Time:  1,
									Value: 2.5,
								},
							},
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name2":  "tag-value2",
								},
								Point: &logcache_v1.PromQL_Point{
									Time:  2,
									Value: 3.5,
								},
							},
						},
					},
				},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(MatchJSON(`{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": [
						{
							"metric": {
								"deployment": "cf",
								"tag-name":   "tag-value"
							},
							"value": [ 1, "2.5" ]
						},
						{
							"metric": {
								"deployment": "cf",
								"tag-name2":   "tag-value2"
							},
							"value": [ 2, "3.5" ]
						}
					]
				}
			}`))
		})

		It("marshals a matrix instant query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			result, err := marshaler.Marshal(&logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Matrix{
					Matrix: &logcache_v1.PromQL_Matrix{
						Series: []*logcache_v1.PromQL_Series{
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name":   "tag-value",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 2.5,
									},
									{
										Time:  2,
										Value: 3.5,
									},
								},
							},
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name2":  "tag-value2",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 4.5,
									},
									{
										Time:  2,
										Value: 6.5,
									},
								},
							},
						},
					},
				},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(MatchJSON(`{
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"deployment": "cf",
								"tag-name":   "tag-value"
							},
							"values": [
								[ 1, "2.5" ],
								[ 2, "3.5" ]
							]
						},
						{
							"metric": {
								"deployment": "cf",
								"tag-name2":   "tag-value2"
							},
							"values": [
								[ 1, "4.5" ],
								[ 2, "6.5" ]
							]
						}
					]
				}
			}`))
		})

		It("marshals a matrix range query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			result, err := marshaler.Marshal(&logcache_v1.PromQL_RangeQueryResult{
				Result: &logcache_v1.PromQL_RangeQueryResult_Matrix{
					Matrix: &logcache_v1.PromQL_Matrix{
						Series: []*logcache_v1.PromQL_Series{
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name":   "tag-value",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 2.5,
									},
									{
										Time:  2,
										Value: 3.5,
									},
								},
							},
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name2":  "tag-value2",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 4.5,
									},
									{
										Time:  2,
										Value: 6.5,
									},
								},
							},
						},
					},
				},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(MatchJSON(`{
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"deployment": "cf",
								"tag-name":   "tag-value"
							},
							"values": [
								[ 1, "2.5" ],
								[ 2, "3.5" ]
							]
						},
						{
							"metric": {
								"deployment": "cf",
								"tag-name2":   "tag-value2"
							},
							"values": [
								[ 1, "4.5" ],
								[ 2, "6.5" ]
							]
						}
					]
				}
			}`))
		})

		It("falls back to the fallback marshaler for non-PromQL replies", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			result, err := marshaler.Marshal(nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal([]byte("mock marshaled result")))
		})

		It("passes through errors from the fallback marshaler", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{
				marshalError: errors.New("expected error"),
			})

			_, err := marshaler.Marshal(nil)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("NewEncoder()", func() {
		It("can encode to a writer", func() {
			encoded := bytes.NewBuffer(nil)
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})
			encoder := marshaler.NewEncoder(encoded)

			err := encoder.Encode(&logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Scalar{
					Scalar: &logcache_v1.PromQL_Scalar{
						Time:  1,
						Value: 2.5,
					},
				},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(encoded.String()).To(MatchJSON(`{
				"status": "success",
				"data": {
					"resultType": "scalar",
					"result": [1, "2.5"]
				}
			}`))
		})

		It("falls back to the fallback marshaler for non-PromQL replies", func() {
			encoded := bytes.NewBuffer(nil)
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})
			encoder := marshaler.NewEncoder(encoded)

			err := encoder.Encode(nil)

			Expect(err).ToNot(HaveOccurred())

			Expect(err).ToNot(HaveOccurred())
			Expect(encoded.String()).To(Equal("mock encoded result"))
		})

		It("passes through errors from the fallback marshaler", func() {
			encoded := bytes.NewBuffer(nil)
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{
				encodeError: errors.New("expected error"),
			})
			encoder := marshaler.NewEncoder(encoded)

			err := encoder.Encode(nil)

			Expect(err).To(HaveOccurred())
		})
	})

	Context("Unmarshal()", func() {
		It("unmarshals a scalar instant query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			var result logcache_v1.PromQL_InstantQueryResult
			err := marshaler.Unmarshal([]byte(`{
				"status": "success",
				"data": {
					"resultType": "scalar",
					"result": [1, "2.5"]
				}
			}`), &result)
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Scalar{
					Scalar: &logcache_v1.PromQL_Scalar{
						Time:  1.0,
						Value: 2.5,
					},
				},
			}))
		})

		It("marshals a vector instant query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			var result logcache_v1.PromQL_InstantQueryResult
			err := marshaler.Unmarshal([]byte(`{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": [
						{
							"metric": {
								"deployment": "cf",
								"tag-name":   "tag-value"
							},
							"value": [ 1, "2.5" ]
						},
						{
							"metric": {
								"deployment": "cf",
								"tag-name2":   "tag-value2"
							},
							"value": [ 2, "3.5" ]
						}
					]
				}
			}`), &result)
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Vector{
					Vector: &logcache_v1.PromQL_Vector{
						Samples: []*logcache_v1.PromQL_Sample{
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name":   "tag-value",
								},
								Point: &logcache_v1.PromQL_Point{
									Time:  1,
									Value: 2.5,
								},
							},
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name2":  "tag-value2",
								},
								Point: &logcache_v1.PromQL_Point{
									Time:  2,
									Value: 3.5,
								},
							},
						},
					},
				},
			}))
		})

		It("marshals a matrix instant query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			var result logcache_v1.PromQL_InstantQueryResult
			err := marshaler.Unmarshal([]byte(`{
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"deployment": "cf",
								"tag-name":   "tag-value"
							},
							"values": [
								[ 1, "2.5" ],
								[ 2, "3.5" ]
							]
						},
						{
							"metric": {
								"deployment": "cf",
								"tag-name2":   "tag-value2"
							},
							"values": [
								[ 1, "4.5" ],
								[ 2, "6.5" ]
							]
						}
					]
				}
			}`), &result)
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Matrix{
					Matrix: &logcache_v1.PromQL_Matrix{
						Series: []*logcache_v1.PromQL_Series{
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name":   "tag-value",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 2.5,
									},
									{
										Time:  2,
										Value: 3.5,
									},
								},
							},
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name2":  "tag-value2",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 4.5,
									},
									{
										Time:  2,
										Value: 6.5,
									},
								},
							},
						},
					},
				},
			}))
		})

		It("marshals a matrix range query result", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			var result logcache_v1.PromQL_RangeQueryResult
			err := marshaler.Unmarshal([]byte(`{
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"deployment": "cf",
								"tag-name":   "tag-value"
							},
							"values": [
								[ 1, "2.5" ],
								[ 2, "3.5" ]
							]
						},
						{
							"metric": {
								"deployment": "cf",
								"tag-name2":   "tag-value2"
							},
							"values": [
								[ 1, "4.5" ],
								[ 2, "6.5" ]
							]
						}
					]
				}
			}`), &result)
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(logcache_v1.PromQL_RangeQueryResult{
				Result: &logcache_v1.PromQL_RangeQueryResult_Matrix{
					Matrix: &logcache_v1.PromQL_Matrix{
						Series: []*logcache_v1.PromQL_Series{
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name":   "tag-value",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 2.5,
									},
									{
										Time:  2,
										Value: 3.5,
									},
								},
							},
							{
								Metric: map[string]string{
									"deployment": "cf",
									"tag-name2":  "tag-value2",
								},
								Points: []*logcache_v1.PromQL_Point{
									{
										Time:  1,
										Value: 4.5,
									},
									{
										Time:  2,
										Value: 6.5,
									},
								},
							},
						},
					},
				},
			}))
		})

		It("falls back to the fallback marshaler", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			var result string
			err := marshaler.Unmarshal(nil, &result)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal("mock unmarshaled result"))
		})

		It("passes through errors from the fallback marshaler", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{
				unmarshalError: errors.New("expected error"),
			})

			var result string
			err := marshaler.Unmarshal(nil, &result)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("NewDecoder()", func() {
		It("can decodes from a reader", func() {
			marshaled := strings.NewReader(`{
				"status": "success",
				"data": {
					"resultType": "scalar",
					"result": [1, "2.5"]
				}
			}`)
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			var result logcache_v1.PromQL_InstantQueryResult
			err := marshaler.NewDecoder(marshaled).Decode(&result)

			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(logcache_v1.PromQL_InstantQueryResult{
				Result: &logcache_v1.PromQL_InstantQueryResult_Scalar{
					Scalar: &logcache_v1.PromQL_Scalar{
						Time:  1,
						Value: 2.5,
					},
				},
			}))
		})

		It("falls back to the fallback marshaler", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})
			decoder := marshaler.NewDecoder(bytes.NewBuffer(nil))

			var result string
			err := decoder.Decode(&result)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal("mock decoded result"))
		})

		It("passes through errors from the fallback marshaler", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{
				decodeError: errors.New("expected error"),
			})
			decoder := marshaler.NewDecoder(bytes.NewBuffer(nil))

			var result string
			err := decoder.Decode(&result)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("ContentType()", func() {
		It("returns application/json", func() {
			marshaler := gateway.NewPromqlMarshaler(&mockMarshaler{})

			Expect(marshaler.ContentType()).To(Equal("application/json"))
		})
	})
})

type mockMarshaler struct {
	marshalError   error
	unmarshalError error
	encodeError    error
	decodeError    error
}

func (m *mockMarshaler) Marshal(v interface{}) ([]byte, error) {
	return []byte("mock marshaled result"), m.marshalError
}

func (m *mockMarshaler) Unmarshal(data []byte, v interface{}) error {
	*(v.(*string)) = "mock unmarshaled result"

	return m.unmarshalError
}

func (m *mockMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return runtime.EncoderFunc(func(interface{}) error {
		w.Write([]byte("mock encoded result"))

		return m.encodeError
	})
}

func (m *mockMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return runtime.DecoderFunc(func(v interface{}) error {
		*(v.(*string)) = "mock decoded result"

		return m.decodeError
	})
}

func (m *mockMarshaler) ContentType() string {
	panic("not implemented")
}
