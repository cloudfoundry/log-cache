package promql_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/promql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PromQL", func() {
	var (
		spyMetrics    *spyMetrics
		spyDataReader *spyDataReader
		q             *promql.PromQL
	)

	BeforeEach(func() {
		spyDataReader = newSpyDataReader()
		spyMetrics = newSpyMetrics()

		q = promql.New(
			spyDataReader,
			spyMetrics,
			log.New(ioutil.Discard, "", 0),
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

	It("returns the given source_ids", func() {
		sIDs, err := q.Parse(`metric{source_id="a"}+metric{source_id="b"}`)
		Expect(err).ToNot(HaveOccurred())
		Expect(sIDs).To(ConsistOf("a", "b"))
	})

	It("returns an error for an invalid query", func() {
		_, err := q.Parse(`invalid.query`)
		Expect(err).To(HaveOccurred())
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
					Query: `some_metric_count{source_id="some-id-1"} + some_metric_count{source_id="some-id-2"}`,
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
					Query: `some_metric_time{source_id="some-id-1"} + some_metric_time{source_id="some-id-2"}`,
					Time:  hourAgo.UnixNano(),
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
					Query: `some_metric_value{source_id="some-id-1"} + some_metric_value{source_id="some-id-2"} + some_metric_value{source_id="some-id-3"}`,
					Time:  now.UnixNano(),
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

			Expect(time.Unix(0, r.GetScalar().GetTime())).To(
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
					Time:  hourAgo.UnixNano(),
					Query: `metric{source_id="some-id-1"} + metric{source_id="some-id-2"}`,
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetVector().GetSamples()).To(HaveLen(1))

			actualTime := r.GetVector().GetSamples()[0].Point.Time
			Expect(time.Unix(0, actualTime)).To(BeTemporally("~", hourAgo, time.Second))

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

			Expect(time.Unix(0, actualTime)).To(BeTemporally("~", spyDataReader.readEnds[0]))
			Expect(
				spyDataReader.readEnds[0].Sub(spyDataReader.readStarts[0]),
			).To(Equal(time.Minute*5 + time.Second))
		})

		It("returns a matrix", func() {
			now := time.Now()
			spyDataReader.readErrs = []error{nil}
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{
				{{
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
						"a": "tag-a",
						"b": "tag-b",
					},
					Points: []*logcache_v1.PromQL_Point{{
						Time:  now.Truncate(time.Second).UnixNano(),
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

			Expect(spyMetrics.gauges).To(HaveKey("PromqlInstantQueryTime"))
			Expect(spyMetrics.gauges["PromqlInstantQueryTime"]).To(HaveLen(1))
			Expect(spyMetrics.gauges["PromqlInstantQueryTime"][0]).ToNot(BeZero())
		})

		It("returns an error for an invalid query", func() {
			_, err := q.InstantQuery(
				context.Background(),
				&logcache_v1.PromQL_InstantQueryRequest{Query: `invalid.query`},
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

			Expect(spyMetrics.names).To(ContainElement("PromQLTimeout"))
			Expect(spyMetrics.deltas).To(ContainElement(uint64(1)))
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
					Start: lastHour.UnixNano(),
					End:   lastHour.Add(5 * time.Minute).UnixNano(),
					Step:  "1m",
				},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
				{
					Metric: map[string]string{
						"a": "tag-a",
						"b": "tag-b",
					},
					Points: []*logcache_v1.PromQL_Point{
						{Time: lastHour.UnixNano(), Value: 98},
						{Time: lastHour.Add(time.Minute).UnixNano(), Value: 102},
						{Time: lastHour.Add(2 * time.Minute).UnixNano(), Value: 106},
						{Time: lastHour.Add(3 * time.Minute).UnixNano(), Value: 108},
						{Time: lastHour.Add(4 * time.Minute).UnixNano(), Value: 110},
						{Time: lastHour.Add(5 * time.Minute).UnixNano(), Value: 112},
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
					Start: lastHour.UnixNano(),
					End:   lastHour.Add(5 * time.Minute).UnixNano(),
					Step:  "1m",
				},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
				{
					Metric: map[string]string{
						"a": "tag-a",
						"b": "tag-b",
					},
					Points: []*logcache_v1.PromQL_Point{
						{Time: lastHour.UnixNano(), Value: 97},
						{Time: lastHour.Add(time.Minute).UnixNano(), Value: 103},
						{Time: lastHour.Add(2 * time.Minute).UnixNano(), Value: 103},
						{Time: lastHour.Add(3 * time.Minute).UnixNano(), Value: 105},
						{Time: lastHour.Add(4 * time.Minute).UnixNano(), Value: 105},
						{Time: lastHour.Add(5 * time.Minute).UnixNano(), Value: 111},
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
					Start: lastHour.UnixNano(),
					End:   lastHour.Add(5 * time.Minute).UnixNano(),
					Step:  "1m",
				},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
				{
					Metric: map[string]string{"a": "tag-a", "b": "tag-b"},
					Points: []*logcache_v1.PromQL_Point{
						{Time: lastHour.UnixNano(), Value: 97},
						{Time: lastHour.Add(time.Minute).UnixNano(), Value: 97},
						{Time: lastHour.Add(2 * time.Minute).UnixNano(), Value: 113},
						{Time: lastHour.Add(3 * time.Minute).UnixNano(), Value: 113},
						{Time: lastHour.Add(4 * time.Minute).UnixNano(), Value: 113},
						{Time: lastHour.Add(5 * time.Minute).UnixNano(), Value: 113},
					},
				},
				{
					Metric: map[string]string{"a": "tag-a", "c": "tag-c"},
					Points: []*logcache_v1.PromQL_Point{
						{Time: lastHour.Add(time.Minute).UnixNano(), Value: 101},
						{Time: lastHour.Add(2 * time.Minute).UnixNano(), Value: 101},
						{Time: lastHour.Add(3 * time.Minute).UnixNano(), Value: 101},
						{Time: lastHour.Add(4 * time.Minute).UnixNano(), Value: 101},
						{Time: lastHour.Add(5 * time.Minute).UnixNano(), Value: 101},
					},
				},
			}))

			Eventually(spyDataReader.ReadSourceIDs).Should(
				ConsistOf("some-id-1"),
			)
		})

		It("captures the query time as a metric", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}`, Start: 1, End: 1, Step: "1m"},
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(spyMetrics.gauges).To(HaveKey("PromqlRangeQueryTime"))
			Expect(spyMetrics.gauges["PromqlRangeQueryTime"]).To(HaveLen(1))
			Expect(spyMetrics.gauges["PromqlRangeQueryTime"][0]).ToNot(BeZero())
		})

		It("returns an error for an invalid query", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `invalid.query`, Start: 1, End: 2, Step: "1m"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error if a metric does not have a source ID", func() {
			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"} + metric`, Start: 1, End: 2, Step: "1m"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("returns an error if the data reader fails", func() {
			spyDataReader.readResults = [][]*loggregator_v2.Envelope{nil}
			spyDataReader.readErrs = []error{errors.New("some-error")}

			_, err := q.RangeQuery(
				context.Background(),
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}`, Start: 1, End: 2, Step: "1m"},
			)
			Expect(err).To(HaveOccurred())

			Expect(spyMetrics.names).To(ContainElement("PromQLTimeout"))
			Expect(spyMetrics.deltas).To(ContainElement(uint64(1)))
		})

		It("returns an error for a cancelled context", func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := q.RangeQuery(
				ctx,
				&logcache_v1.PromQL_RangeQueryRequest{Query: `metric{source_id="some-id-1"}[5m]`, Start: 1, End: 2, Step: "1m"},
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

	s.readSourceIDs = append(s.readSourceIDs, req.SourceId)
	s.readStarts = append(s.readStarts, time.Unix(0, req.StartTime))
	s.readEnds = append(s.readEnds, time.Unix(0, req.EndTime))

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

func (s *spyDataReader) setRead(es [][]*loggregator_v2.Envelope, errs []error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readResults = es
	s.readErrs = errs
}

type spyMetrics struct {
	names  []string
	deltas []uint64
	gauges map[string][]float64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		gauges: make(map[string][]float64),
	}
}

func (s *spyMetrics) NewCounter(name string) func(delta uint64) {
	s.names = append(s.names, name)
	return func(delta uint64) {
		s.deltas = append(s.deltas, delta)
	}
}

func (s *spyMetrics) NewGauge(name string) func(value float64) {
	return func(value float64) {
		s.gauges[name] = append(s.gauges[name], value)
	}
}
