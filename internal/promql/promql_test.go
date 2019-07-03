package promql_test

import (
	"code.cloudfoundry.org/go-loggregator/metrics/testhelpers"
	"context"
	"errors"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/promql"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"

	"code.cloudfoundry.org/log-cache/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PromQL", func() {
	var (
		spyMetrics    *testhelpers.SpyMetricsRegistry
		spyDataReader *spyDataReader
		q             *promql.PromQL
	)

	BeforeEach(func() {
		spyDataReader = newSpyDataReader()
		spyMetrics = testhelpers.NewMetricsRegistry()

		q = promql.New(
			spyDataReader,
			spyMetrics,
			log.New(ioutil.Discard, "", 0),
			5*time.Second,
		)
	})

	Describe("SanitizeMetricName", func() {
		It("converts all valid separators to underscores", func() {
			metrics := []string{
				"9vitals.vm.cpu.count1",
				"__vitals.vm..cpu.count2",
				"&vitals.vm.cpu./count3",
				"vitals.vm.cpu.count99",
				"vitals vm/cpu#count100",
				"vitals:vm+cpu-count101",
				"1",
				"_",
				"a",
				"&",
				"&+&+&+9",
			}

			converted := []string{
				"_vitals_vm_cpu_count1",
				"__vitals_vm__cpu_count2",
				"_vitals_vm_cpu__count3",
				"vitals_vm_cpu_count99",
				"vitals_vm_cpu_count100",
				"vitals_vm_cpu_count101",
				"_",
				"_",
				"a",
				"_",
				"______9",
			}

			for n, metric := range metrics {
				Expect(promql.SanitizeMetricName(metric)).To(Equal(converted[n]))
			}
		})
	})

	Context("ExtractSourceIds", func() {
		It("returns the given source IDs", func() {
			sIDs, err := promql.ExtractSourceIds(`metric{source_id="a"}+metric{source_id="b"}`)
			Expect(err).ToNot(HaveOccurred())
			Expect(sIDs).To(ConsistOf("a", "b"))
		})

		It("returns the given source IDs in deeply nested queries", func() {
			sIDs, err := promql.ExtractSourceIds(`avg_over_time(gauge_example{source_id="a"}[10m])`)
			Expect(err).ToNot(HaveOccurred())
			Expect(sIDs).To(ConsistOf("a"))
		})

		It("expands requests filtered for multiple source IDs", func() {
			sIDs, err := promql.ExtractSourceIds(`metric{source_id=~"a|b"}`)
			Expect(err).ToNot(HaveOccurred())
			Expect(sIDs).To(ConsistOf("a", "b"))
		})

		It("returns an error for an invalid query", func() {
			_, err := promql.ExtractSourceIds(`invalid.query`)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("ReplaceSourceIdSets", func() {
		It("returns queries unmodified when given no expansions", func() {
			query := `metric{source_id="a"} + avg_over_time(gauge_example{source_id="b"}[10m])`
			modifiedQuery, err := promql.ReplaceSourceIdSets(query, map[string][]string{})

			Expect(err).ToNot(HaveOccurred())
			Expect(modifiedQuery).To(Equal(query))
		})

		It("returns queries unmodified when no expansions match", func() {
			query := `metric{source_id="a"} + avg_over_time(gauge_example{source_id="b"}[10m])`
			modifiedQuery, err := promql.ReplaceSourceIdSets(query, map[string][]string{
				"expanded": {"expansion-1", "expansion-2"},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(modifiedQuery).To(Equal(query))
		})

		It("returns appropriate queries when the original sourceId is elided", func() {
			query := `metric{source_id="a"} + avg_over_time(gauge_example{source_id="expanded"}[10m])`
			modifiedQuery, err := promql.ReplaceSourceIdSets(query, map[string][]string{
				"expanded": {"expansion-1", "expansion-2"},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(modifiedQuery).To(
				Equal(`metric{source_id="a"} + avg_over_time(gauge_example{source_id=~"expansion-1|expansion-2"}[10m])`),
			)
		})

		It("returns queries using the equality matcher when replacing a single sourceId", func() {
			query := `metric{source_id="to-be-replaced"}`
			modifiedQuery, err := promql.ReplaceSourceIdSets(query, map[string][]string{
				"to-be-replaced": {"replacement"},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(modifiedQuery).To(
				Equal(`metric{source_id="replacement"}`),
			)
		})

		It("returns queries using the equality matcher when replacing multiple sourceIds with a single sourceId", func() {
			query := `metric{source_id=~"to-be-replaced-1|to-be-replaced-2"}`
			modifiedQuery, err := promql.ReplaceSourceIdSets(query, map[string][]string{
				"to-be-replaced-1": {"replacement"},
				"to-be-replaced-2": {},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(modifiedQuery).To(
				Equal(`metric{source_id="replacement"}`),
			)
		})

		It("returns an error for an invalid query", func() {
			_, err := promql.ReplaceSourceIdSets(`invalid.query`, map[string][]string{})
			Expect(err).To(HaveOccurred())
		})
	})

	It("a query against data with invalid envelope types returns only the valid metrics", func() {
		now := time.Now().Add(-time.Minute)
		spyDataReader.readResults = [][]*loggregator_v2.Envelope{
			{
				{
					SourceId:  "some-id",
					Timestamp: now.UnixNano(),
					Message: &loggregator_v2.Envelope_Counter{
						Counter: &loggregator_v2.Counter{Name: "metric", Total: 100},
					},
					Tags: map[string]string{
						"tag": "a",
					},
				},
				{
					SourceId:  "some-id",
					Timestamp: now.UnixNano() + 1,
					Message: &loggregator_v2.Envelope_Event{
						Event: &loggregator_v2.Event{Title: "some-title", Body: "some-body"},
					},
					Tags: map[string]string{
						"tag": "b",
					},
				},
				{
					SourceId:  "some-id",
					Timestamp: now.UnixNano() + 2,
					Message: &loggregator_v2.Envelope_Log{
						Log: &loggregator_v2.Log{Payload: []byte("some-payload")},
					},
					Tags: map[string]string{
						"tag": "c",
					},
				},
				{
					SourceId:  "some-id",
					Timestamp: now.UnixNano() + 3,
					Message: &loggregator_v2.Envelope_Gauge{
						Gauge: &loggregator_v2.Gauge{
							Metrics: map[string]*loggregator_v2.GaugeValue{
								"metric": {Unit: "some-unit", Value: 1},
							},
						},
					},
					Tags: map[string]string{
						"tag": "d",
					},
				},
				{
					SourceId:  "some-id",
					Timestamp: now.UnixNano() + 4,
					Message: &loggregator_v2.Envelope_Timer{
						Timer: &loggregator_v2.Timer{Name: "metric", Start: 10, Stop: 20},
					},
					Tags: map[string]string{
						"tag": "e",
					},
				},
			},
		}

		spyDataReader.readErrs = []error{nil}

		r, err := q.InstantQuery(
			context.Background(),
			&logcache_v1.PromQL_InstantQueryRequest{
				Query: `metric{source_id="some-id"}`,
			},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.GetVector().GetSamples()).To(HaveLen(3))

		Eventually(spyDataReader.ReadSourceIDs).Should(
			ConsistOf("some-id"),
		)

		Expect(spyDataReader.ReadEnvelopeTypes()).To(HaveLen(1))
		Expect(spyDataReader.ReadEnvelopeTypes()[0]).To(Equal(
			[]logcache_v1.EnvelopeType{
				logcache_v1.EnvelopeType_GAUGE,
				logcache_v1.EnvelopeType_COUNTER,
				logcache_v1.EnvelopeType_TIMER,
			},
		),
		)
	})

	Context("when metric names contain unsupported characters", func() {
		It("converts counter metric names to proper promql format", func() {
			now := time.Now()
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Counter{
							Counter: &loggregator_v2.Counter{
								Name:  "some-metric$count",
								Total: 104,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
				{
					{
						SourceId:  "some-id-2",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Counter{
							Counter: &loggregator_v2.Counter{
								Name:  "some|metric#count",
								Total: 100,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
			}

			for range spyDataReader.readResults {
				spyDataReader.readErrs = append(spyDataReader.readErrs, nil)
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{
					Query: `some_metric_count{source_id="some-id-1"} + ignoring (source_id) some_metric_count{source_id="some-id-2"}`,
				},
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(r.GetVector().GetSamples()).To(HaveLen(1))
			Expect(r.GetVector().GetSamples()[0].Point.Value).To(Equal(204.0))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1", "some-id-2"),
			)
		})

		It("converts timer metric names to proper promql format", func() {
			hourAgo := time.Now().Add(-time.Hour)
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: hourAgo.UnixNano(),
						Message: &loggregator_v2.Envelope_Timer{
							Timer: &loggregator_v2.Timer{
								Name:  "some-metric$time",
								Start: 199,
								Stop:  201,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
				{
					{
						SourceId:  "some-id-2",
						Timestamp: hourAgo.UnixNano(),
						Message: &loggregator_v2.Envelope_Timer{
							Timer: &loggregator_v2.Timer{
								Name:  "some|metric#time",
								Start: 299,
								Stop:  302,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
			}

			for range spyDataReader.readResults {
				spyDataReader.readErrs = append(spyDataReader.readErrs, nil)
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{
					Query: `some_metric_time{source_id="some-id-1"} + ignoring(source_id) some_metric_time{source_id="some-id-2"}`,
					Time:  testing.FormatTimeWithDecimalMillis(hourAgo),
				},
			)
			Expect(err).NotTo(HaveOccurred())

			Expect(r.GetVector().GetSamples()).To(HaveLen(1))
			Expect(r.GetVector().GetSamples()[0].Point.Value).To(Equal(5.0))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1", "some-id-2"),
			)
		})

		It("converts gauge metric names to proper promql format", func() {
			now := time.Now()
			// hourAgo := time.Now().Add(-time.Hour)
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"some-metric$value": {Unit: "thing", Value: 99},
								},
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
				{
					{
						SourceId:  "some-id-2",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"some|metric#value": {Unit: "thing", Value: 199},
								},
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
				{
					{
						SourceId:  "some-id-3",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"some.metric+value": {Unit: "thing", Value: 299},
								},
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
			}

			for range spyDataReader.readResults {
				spyDataReader.readErrs = append(spyDataReader.readErrs, nil)
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{
					Query: `some_metric_value{source_id="some-id-1"} + ignoring (source_id) some_metric_value{source_id="some-id-2"} + ignoring (source_id) some_metric_value{source_id="some-id-3"}`,
					Time:  testing.FormatTimeWithDecimalMillis(now),
				},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetVector().GetSamples()).To(HaveLen(1))
			Expect(r.GetVector().GetSamples()[0].Point.Value).To(Equal(597.0))
		})
	})

	Context("When using an InstantQuery", func() {
		It("returns a scalar", func() {
			r, err := q.InstantQuery(context.Background(), &logcache_v1.PromQL_InstantQueryRequest{Query: `7*9`})
			Expect(err).ToNot(HaveOccurred())

			Expect(testing.ParseTimeWithDecimalMillis(r.GetScalar().GetTime())).To(
				BeTemporally("~", time.Now(), time.Second),
			)

			Expect(r.GetScalar().GetValue()).To(Equal(63.0))
		})

		It("returns a vector", func() {
			hourAgo := time.Now().Add(-time.Hour)
			spyDataReader.readErrs = []error{nil, nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{{
					SourceId:  "some-id-1",
					Timestamp: hourAgo.UnixNano(),
					Message: &loggregator_v2.Envelope_Counter{
						Counter: &loggregator_v2.Counter{
							Name:  "metric",
							Total: 99,
						},
					},
					Tags: map[string]string{
						"a": "tag-a",
						"b": "tag-b",
					},
				}},
				{{
					SourceId:  "some-id-2",
					Timestamp: hourAgo.UnixNano(),
					Message: &loggregator_v2.Envelope_Counter{
						Counter: &loggregator_v2.Counter{
							Name:  "metric",
							Total: 101,
						},
					},
					Tags: map[string]string{
						"a": "tag-a",
						"b": "tag-b",
					},
				}},
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{
					Time:  testing.FormatTimeWithDecimalMillis(hourAgo),
					Query: `metric{source_id="some-id-1"} + ignoring (source_id) metric{source_id="some-id-2"}`,
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetVector().GetSamples()).To(HaveLen(1))

			actualTime := r.GetVector().GetSamples()[0].Point.Time
			Expect(testing.ParseTimeWithDecimalMillis(actualTime)).To(BeTemporally("~", hourAgo, time.Second))

			Expect(r.GetVector().GetSamples()).To(Equal([]*logcache_v1.PromQL_Sample{
				{
					Metric: map[string]string{
						"a": "tag-a",
						"b": "tag-b",
					},
					Point: &logcache_v1.PromQL_Point{
						Time:  actualTime,
						Value: 200,
					},
				},
			}))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1", "some-id-2"),
			)

			Expect(testing.ParseTimeWithDecimalMillis(actualTime)).To(BeTemporally("~", spyDataReader.readEnds[0]))
			Expect(
				spyDataReader.readEnds[0].Sub(spyDataReader.readStarts[0]),
			).To(Equal(time.Minute*5 + time.Second))
		})

		It("returns a matrix", func() {
			now := time.Now()
			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{{
					SourceId:   "some-id-1",
					InstanceId: "0",
					Timestamp:  now.UnixNano(),
					Message: &loggregator_v2.Envelope_Counter{
						Counter: &loggregator_v2.Counter{
							Name:  "metric",
							Total: 99,
						},
					},
					Tags: map[string]string{
						"a": "tag-a",
						"b": "tag-b",
					},
				}},
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"}[5m]`},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
				{
					Metric: map[string]string{
						"a":           "tag-a",
						"b":           "tag-b",
						"source_id":   "some-id-1",
						"instance_id": "0",
					},
					Points: []*logcache_v1.PromQL_Point{{
						Time:  testing.FormatTimeWithDecimalMillis(now.Truncate(time.Second)),
						Value: 99,
					}},
				},
			}))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1"),
			)
		})

		It("filters for correct counter metric name and label", func() {
			now := time.Now()
			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Counter{
							Counter: &loggregator_v2.Counter{
								Name:  "metric",
								Total: 99,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Counter{
							Counter: &loggregator_v2.Counter{
								Name:  "wrongname",
								Total: 101,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Counter{
							Counter: &loggregator_v2.Counter{
								Name:  "metric",
								Total: 103,
							},
						},
						Tags: map[string]string{
							"a": "wrong-tag",
							"b": "tag-b",
						},
					},
				},
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1",a="tag-a"}`},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetVector().GetSamples()).To(HaveLen(1))
			Expect(r.GetVector().GetSamples()[0].Point.Value).To(Equal(99.0))
		})

		It("filters for correct timer metric name", func() {
			now := time.Now()
			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Timer{
							Timer: &loggregator_v2.Timer{
								Name:  "metric",
								Start: 99,
								Stop:  101,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Timer{
							Timer: &loggregator_v2.Timer{
								Name:  "wrongname",
								Start: 99,
								Stop:  101,
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"}`},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetVector().GetSamples()).To(HaveLen(1))
			Expect(r.GetVector().GetSamples()[0].Point.Value).To(Equal(2.0))
		})

		It("filters for correct gauge metric name", func() {
			now := time.Now()
			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric":      {Unit: "thing", Value: 99},
									"othermetric": {Unit: "thing", Value: 103},
								},
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"wrongname": {Unit: "thing", Value: 101},
								},
							},
						},
						Tags: map[string]string{
							"a": "tag-a",
							"b": "tag-b",
						},
					},
				},
			}

			r, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"}`},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetVector().GetSamples()).To(HaveLen(1))
			Expect(r.GetVector().GetSamples()[0].Point.Value).To(Equal(99.0))
		})

		It("captures the query time as a metric", func() {
			_, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"}`},
			)

			Expect(err).ToNot(HaveOccurred())

			Eventually(func() float64 {
				return spyMetrics.GetMetricValue("log_cache_promql_instant_query_time", nil)
			}).ShouldNot(BeZero())
		})

		It("expands requests filtered for multiple source IDs", func() {
			now := time.Now()
			spyDataReader.readErrs = []error{nil, nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 99},
								},
							},
						},
					},
				},
				{
					{
						SourceId:  "some-id-2",
						Timestamp: now.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 101},
								},
							},
						},
					},
				},
			}

			result, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id=~"some-id-1|some-id-2"}`},
			)
			Expect(err).ToNot(HaveOccurred())

			Eventually(spyDataReader.ReadSourceIDs).Should(ConsistOf("some-id-1", "some-id-2"))

			Expect(func() []map[string]string {
				var metrics []map[string]string

				for _, sample := range result.GetVector().GetSamples() {
					metrics = append(metrics, sample.GetMetric())
				}

				return metrics
			}()).To(ConsistOf(
				HaveKeyWithValue("source_id", "some-id-1"),
				HaveKeyWithValue("source_id", "some-id-2"),
			))
		})

		It("returns an error for an invalid query", func() {
			_, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `invalid.query`},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error for an invalid time", func() {
			_, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"}`, Time: "409l"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error if a metric does not have a source ID", func() {
			_, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"} + metric`},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error if the data reader fails", func() {
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{nil}
			spyDataReader.readErrs = []error{errors.New("some-error")}

			_, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"}[5m]`},
			)
			Expect(err).To(HaveOccurred())

			Eventually(func() float64 {
				return spyMetrics.GetMetricValue("log_cache_promql_timeout", nil)
			}).Should(Equal(1.0))
		})

		It("returns an error for a cancelled context", func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := q.InstantQuery(
				ctx,
				&logcache_v1.PromQL_InstantQueryRequest{Query: `metric{source_id="some-id-1"}[5m]`},
			)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When using a RangeQuery", func() {
		It("returns a matrix of aggregated values", func() {
			lastHour := time.Now().Truncate(time.Hour).Add(-time.Hour)

			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(-time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 97},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric":      {Unit: "thing", Value: 99},
									"othermetric": {Unit: "thing", Value: 103},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(30 * time.Second).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric":    {Unit: "thing", Value: 101},
									"wrongname": {Unit: "thing", Value: 201},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(45 * time.Second).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 103},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(1 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 105},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(2 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 107},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(3 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 109},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(4 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 111},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(5 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 113},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
				},
			}

			r, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{
					Query: `avg_over_time(metric{source_id="some-id-1"}[1m])`,
					Start: testing.FormatTimeWithDecimalMillis(lastHour),
					End:   testing.FormatTimeWithDecimalMillis(lastHour.Add(5 * time.Minute)),
					Step:  "1m",
				},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
				{
					Metric: map[string]string{
						"a":         "tag-a",
						"b":         "tag-b",
						"source_id": "some-id-1",
					},
					Points: []*logcache_v1.PromQL_Point{
						{Time: testing.FormatTimeWithDecimalMillis(lastHour), Value: 98},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(time.Minute)), Value: 102},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(2 * time.Minute)), Value: 106},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(3 * time.Minute)), Value: 108},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(4 * time.Minute)), Value: 110},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(5 * time.Minute)), Value: 112},
					},
				},
			}))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1"),
			)
		})

		It("returns a matrix of unaggregated values, choosing the latest value in the time window", func() {
			lastHour := time.Now().Truncate(time.Hour).Add(-time.Hour)

			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(-time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 97},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(30 * time.Second).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric":    {Unit: "thing", Value: 101},
									"wrongname": {Unit: "thing", Value: 201},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(45 * time.Second).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 103},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(3 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 105},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(5 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 111},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(6 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 113},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
				},
			}

			r, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{
					Query: `metric{source_id="some-id-1"}`,
					Start: testing.FormatTimeWithDecimalMillis(lastHour),
					End:   testing.FormatTimeWithDecimalMillis(lastHour.Add(5 * time.Minute)),
					Step:  "1m",
				},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
				{
					Metric: map[string]string{
						"a":         "tag-a",
						"b":         "tag-b",
						"source_id": "some-id-1",
					},
					Points: []*logcache_v1.PromQL_Point{
						{Time: testing.FormatTimeWithDecimalMillis(lastHour), Value: 97},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(time.Minute)), Value: 103},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(2 * time.Minute)), Value: 103},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(3 * time.Minute)), Value: 105},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(4 * time.Minute)), Value: 105},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(5 * time.Minute)), Value: 111},
					},
				},
			}))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1"),
			)
		})

		It("returns a matrix of unaggregated values, ordered by tag", func() {
			lastHour := time.Now().Truncate(time.Hour).Add(-time.Hour)

			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 97},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(1 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 101},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "c": "tag-c"},
					},
					{
						SourceId:  "some-id-1",
						Timestamp: lastHour.Add(2 * time.Minute).UnixNano(),
						Message: &loggregator_v2.Envelope_Gauge{
							Gauge: &loggregator_v2.Gauge{
								Metrics: map[string]*loggregator_v2.GaugeValue{
									"metric": {Unit: "thing", Value: 113},
								},
							},
						},
						Tags: map[string]string{"a": "tag-a", "b": "tag-b"},
					},
				},
			}

			r, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{
					Query: `metric{source_id="some-id-1"}`,
					Start: testing.FormatTimeWithDecimalMillis(lastHour),
					End:   testing.FormatTimeWithDecimalMillis(lastHour.Add(5 * time.Minute)),
					Step:  "1m",
				},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
				{
					Metric: map[string]string{
						"a":         "tag-a",
						"b":         "tag-b",
						"source_id": "some-id-1",
					},
					Points: []*logcache_v1.PromQL_Point{
						{Time: testing.FormatTimeWithDecimalMillis(lastHour), Value: 97},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(time.Minute)), Value: 97},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(2 * time.Minute)), Value: 113},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(3 * time.Minute)), Value: 113},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(4 * time.Minute)), Value: 113},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(5 * time.Minute)), Value: 113},
					},
				},
				{
					Metric: map[string]string{
						"a":         "tag-a",
						"c":         "tag-c",
						"source_id": "some-id-1",
					},
					Points: []*logcache_v1.PromQL_Point{
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(time.Minute)), Value: 101},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(2 * time.Minute)), Value: 101},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(3 * time.Minute)), Value: 101},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(4 * time.Minute)), Value: 101},
						{Time: testing.FormatTimeWithDecimalMillis(lastHour.Add(5 * time.Minute)), Value: 101},
					},
				},
			}))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1"),
			)
		})

		It("accepts RFC3339 start and end times", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{
					Query: `metric{source_id="some-id-1"}`,
					Start: "2099-01-01T01:23:45.678Z",
					End:   "2099-01-01T01:24:45.678Z",
					Step:  "1m",
				},
			)

			Expect(err).ToNot(HaveOccurred())
		})

		It("captures the query time as a metric", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}`, Start: "1", End: "1", Step: "1m"},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(func() float64 {
				return spyMetrics.GetMetricValue("log_cache_promql_range_query_time", nil)
			}).ShouldNot(BeZero())
		})

		It("returns an error for an invalid query", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `invalid.query`, Start: "1", End: "2", Step: "1m"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error for an invalid start time", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}`, Start: "potato", End: "2", Step: "1m"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error for an invalid end time", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}`, Start: "1", End: "lemons", Step: "1m"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error if a metric does not have a source ID", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"} + metric`, Start: "1", End: "2", Step: "1m"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error if the data reader fails", func() {
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{nil}
			spyDataReader.readErrs = []error{errors.New("some-error")}

			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}`, Start: "1", End: "2", Step: "1m"},
			)
			Expect(err).To(HaveOccurred())

			Eventually(func() float64 {
				return spyMetrics.GetMetricValue("log_cache_promql_timeout", nil)
			}).Should(Equal(1.0))
		})

		It("returns an error for a cancelled context", func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := q.RangeQuery(
				ctx,
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}[5m]`, Start: "1", End: "2", Step: "1m"},
			)
			Expect(err).To(HaveOccurred())
		})
	})

})

type spyDataReader struct {
	mu            sync.Mutex
	readSourceIDs []string
	readStarts    []time.Time
	readEnds      []time.Time
	readTypes     [][]logcache_v1.EnvelopeType

	readResults [][]*loggregator_v2.Envelope
	readErrs    []error
}

func newSpyDataReader() *spyDataReader {
	return &spyDataReader{}
}

func (s *spyDataReader) Read(
	ctx context.Context,
	req *logcache_v1.ReadRequest,
) (*logcache_v1.ReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Give ourselves some time to capture runtime metrics
	time.Sleep(time.Millisecond)

	s.readSourceIDs = append(s.readSourceIDs, req.SourceId)
	s.readStarts = append(s.readStarts, time.Unix(0, req.StartTime))
	s.readEnds = append(s.readEnds, time.Unix(0, req.EndTime))
	s.readTypes = append(s.readTypes, req.EnvelopeTypes)

	if len(s.readResults) != len(s.readErrs) {
		panic("readResults and readErrs are out of sync")
	}

	if len(s.readResults) == 0 {
		return nil, nil
	}

	r := s.readResults[0]
	err := s.readErrs[0]

	s.readResults = s.readResults[1:]
	s.readErrs = s.readErrs[1:]

	return &logcache_v1.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: r,
		},
	}, err
}

func (s *spyDataReader) ReadSourceIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]string, len(s.readSourceIDs))
	copy(result, s.readSourceIDs)

	return result
}

func (s *spyDataReader) ReadEnvelopeTypes() [][]logcache_v1.EnvelopeType {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([][]logcache_v1.EnvelopeType, len(s.readTypes))
	copy(result, s.readTypes)

	return result
}

func (s *spyDataReader) setRead(es [][]*loggregator_v2.Envelope, errs []error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readResults = es
	s.readErrs = errs
}
