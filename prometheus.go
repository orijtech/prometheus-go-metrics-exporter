// Copyright 2019, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"go.opencensus.io/trace"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Exporter struct {
	options   Options
	handler   http.Handler
	collector *collector
	gatherer  prometheus.Gatherer
}

type Options struct {
	Namespace   string
	OnError     func(err error)
	ConstLabels prometheus.Labels // ConstLabels will be set as labels on all views.
	Registry    *prometheus.Registry
}

func New(o Options) (*Exporter, error) {
	if o.Registry == nil {
		o.Registry = prometheus.NewRegistry()
	}
	collector := newCollector(o, o.Registry)
	exp := &Exporter{
		options:   o,
		gatherer:  o.Registry,
		collector: collector,
		handler:   promhttp.HandlerFor(o.Registry, promhttp.HandlerOpts{}),
	}
	return exp, nil
}

type collector struct {
	mu                  sync.Mutex
	opts                Options
	registry            *prometheus.Registry
	registerOnce        sync.Once
	registeredMetricsMu sync.Mutex
	registeredMetrics   map[string]*prometheus.Desc
	metricsData         map[string]*metricspb.Metric
}

func newCollector(opts Options, registry *prometheus.Registry) *collector {
	return &collector{
		registry:          registry,
		opts:              opts,
		registeredMetrics: make(map[string]*prometheus.Desc),
	}
}

var _ http.Handler = (*Exporter)(nil)

func (c *collector) lookupPrometheusDesc(namespace string, metric *metricspb.Metric) (*prometheus.Desc, string, bool) {
	signature := metricSignature(namespace, metric)
	c.registeredMetricsMu.Lock()
	desc, ok := c.registeredMetrics[signature]
	c.registeredMetricsMu.Unlock()

	return desc, signature, ok
}

func (c *collector) registerMetrics(metrics ...*metricspb.Metric) {
	count := 0
	for _, metric := range metrics {
		_, signature, ok := c.lookupPrometheusDesc(c.opts.Namespace, metric)

		if !ok {
			desc := prometheus.NewDesc(
				metricName(c.opts.Namespace, metric),
				metric.GetMetricDescriptor().GetDescription(),
				protoLabelKeysToLabels(metric.GetMetricDescriptor().GetLabelKeys()),
				c.opts.ConstLabels,
			)
			c.registeredMetricsMu.Lock()
			c.registeredMetrics[signature] = desc
			c.registeredMetricsMu.Unlock()
			count++
		}
	}

	if count == 0 {
		return
	}

	c.ensureRegisteredOnce()
}

func metricName(namespace string, metric *metricspb.Metric) string {
	var name string
	if namespace != "" {
		name = namespace + "_"
	}
	mName := metric.GetMetricDescriptor().GetName()
	if mName == "" {
		mName = metric.GetName()
	}
	return name + sanitize(mName)
}

func metricSignature(namespace string, metric *metricspb.Metric) string {
	var buf bytes.Buffer
	buf.WriteString(metricName(namespace, metric))
	labelKeys := metric.GetMetricDescriptor().GetLabelKeys()
	for _, labelKey := range labelKeys {
		buf.WriteString("-" + labelKey.Key)
	}
	return buf.String()
}

func (c *collector) ensureRegisteredOnce() {
	c.registerOnce.Do(func() {
		if err := c.registry.Register(c); err != nil {
			c.opts.OnError(fmt.Errorf("ensureRegisteredOnce: cannot register the collector: %v", err))
		}
	})
}

func (exp *Exporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	exp.handler.ServeHTTP(w, r)
}

func (o *Options) onError(err error) {
	if o.OnError != nil {
		o.OnError(err)
	} else {
		log.Printf("Failed to export to Prometheus: %v", err)
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	c.registeredMetricsMu.Lock()
	registered := make(map[string]*prometheus.Desc)
	for key, desc := range c.registeredMetrics {
		registered[key] = desc
	}
	c.registeredMetricsMu.Unlock()

	for _, desc := range registered {
		ch <- desc
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	// We need a copy of all the metric data up until this point in time.
	metricsData := c.cloneMetricsData()

	ctx, span := trace.StartSpan(context.Background(), "prometheus.Metrics.Collect", trace.WithSampler(trace.NeverSample()))
	defer span.End()

	for _, metric := range metricsData {
		for _, timeseries := range metric.Timeseries {
			pmetrics, err := c.protoTimeSeriesToPrometheusMetrics(ctx, metric, timeseries)
			if err == nil {
				for _, pmetric := range pmetrics {
					ch <- pmetric
				}
			} else {
				c.opts.onError(err)
			}
		}
	}
}

var errNilTimeSeries = errors.New("expecting a non-nil TimeSeries")

func (c *collector) protoTimeSeriesToPrometheusMetrics(ctx context.Context, metric *metricspb.Metric, ts *metricspb.TimeSeries) ([]prometheus.Metric, error) {
	if ts == nil {
		return nil, errNilTimeSeries
	}
	labelValues := protoLabelValuesToLabelValues(ts.LabelValues)
	derivedPrometheusValueType := prometheusValueType(metric)
	desc, _, _ := c.lookupPrometheusDesc(c.opts.Namespace, metric)

	pmetrics := make([]prometheus.Metric, 0, len(ts.Points))
	for _, point := range ts.Points {
		pmet, err := protoMetricToPrometheusMetric(ctx, point, desc, derivedPrometheusValueType, labelValues)
		if err == nil {
			pmetrics = append(pmetrics, pmet)
		} else {
			// TODO: (@odeke-em) record these errors
		}
	}
	return pmetrics, nil
}

func protoLabelValuesToLabelValues(protoLabelValues []*metricspb.LabelValue) []string {
	if len(protoLabelValues) == 0 {
		return nil
	}

	plainLabelValues := make([]string, len(protoLabelValues))
	for i, protoLabelValue := range protoLabelValues {
		if protoLabelValue.HasValue {
			plainLabelValues[i] = protoLabelValue.Value
		}
	}
	return plainLabelValues
}

func protoLabelKeysToLabels(protoLabelKeys []*metricspb.LabelKey) []string {
	labelKeys := make([]string, 0, len(protoLabelKeys))
	for _, protoLabelKey := range protoLabelKeys {
		labelKeys = append(labelKeys, protoLabelKey.GetKey())
	}
	return labelKeys
}

func protoMetricToPrometheusMetric(ctx context.Context, point *metricspb.Point, desc *prometheus.Desc, derivedPrometheusType prometheus.ValueType, labelValues []string) (prometheus.Metric, error) {
	switch value := point.Value.(type) {
	case *metricspb.Point_DistributionValue:
		dValue := value.DistributionValue

		// Histograms are cumulative in Prometheus.
		indicesMap := make(map[float64]int)
		dBuckets := dValue.BucketOptions.GetExplicit().GetBounds()
		buckets := make([]float64, 0, len(dValue.Buckets))
		for index, bucket := range dBuckets {
			if _, added := indicesMap[bucket]; !added {
				indicesMap[bucket] = index
				buckets = append(buckets, bucket)
			}
		}
		sort.Float64s(buckets)

		// 2. Now that the buckets are sorted by magnitude, we can create
		// cumulative indices and map them back by reverse index.
		cumCount := uint64(0)

		points := make(map[float64]uint64)
		for _, bucket := range buckets {
			index := indicesMap[bucket]
			var countPerBucket uint64
			if len(dValue.Buckets) > 0 && index < len(dValue.Buckets) {
				countPerBucket = uint64(dValue.Buckets[index].GetCount())
			}
			cumCount += countPerBucket
			points[bucket] = cumCount
		}
		return prometheus.NewConstHistogram(desc, uint64(dValue.Count), dValue.Sum, points, labelValues...)

	case *metricspb.Point_Int64Value:
		// Derive the Prometheus
		return prometheus.NewConstMetric(desc, prometheus.CounterValue, float64(value.Int64Value), labelValues...)

	case *metricspb.Point_DoubleValue:
		return prometheus.NewConstMetric(desc, prometheus.CounterValue, value.DoubleValue, labelValues...)

	default:
		return nil, fmt.Errorf("Unhandled type: %T", point.Value)
	}
}

func prometheusValueType(metric *metricspb.Metric) prometheus.ValueType {
	switch metric.GetMetricDescriptor().GetType() {
	case metricspb.MetricDescriptor_GAUGE_DOUBLE, metricspb.MetricDescriptor_GAUGE_INT64, metricspb.MetricDescriptor_GAUGE_DISTRIBUTION:
		return prometheus.GaugeValue

	case metricspb.MetricDescriptor_CUMULATIVE_DOUBLE, metricspb.MetricDescriptor_CUMULATIVE_INT64, metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION:
		return prometheus.CounterValue

	default:
		return prometheus.UntypedValue
	}
}

func (c *collector) cloneMetricsData() map[string]*metricspb.Metric {
	c.mu.Lock()
	defer c.mu.Unlock()

	metricsDataCopy := make(map[string]*metricspb.Metric)
	for signature, metric := range c.metricsData {
		metricsDataCopy[signature] = metric
	}
	return metricsDataCopy
}
