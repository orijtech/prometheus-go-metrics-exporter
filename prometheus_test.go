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
	"context"
	"sync"
	"testing"
	"time"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	startTimestamp = &timestamp.Timestamp{
		Seconds: 1543160298,
		Nanos:   100000090,
	}
	endTimestamp = &timestamp.Timestamp{
		Seconds: 1543160298,
		Nanos:   100000997,
	}
)

func TestOnlyCumulativeWindowSupported(t *testing.T) {

	tests := []struct {
		metric    *metricspb.Metric
		wantCount int
	}{
		{
			metric: &metricspb.Metric{}, wantCount: 0,
		},
		{
			metric: &metricspb.Metric{
				Descriptor_: &metricspb.Metric_MetricDescriptor{
					MetricDescriptor: &metricspb.MetricDescriptor{
						Name:        "with_metric_descriptor",
						Description: "This is a test",
						Unit:        "By",
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						StartTimestamp: startTimestamp,
						Points: []*metricspb.Point{
							{
								Timestamp: endTimestamp,
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Count:                 1,
										Sum:                   11.9,
										SumOfSquaredDeviation: 0,
										Buckets: []*metricspb.DistributionValue_Bucket{
											{}, {Count: 1}, {}, {}, {},
										},
										BucketOptions: &metricspb.DistributionValue_BucketOptions{
											Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{0, 10, 20, 30, 40},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantCount: 1,
		},
		{
			metric: &metricspb.Metric{
				Descriptor_: &metricspb.Metric_MetricDescriptor{
					MetricDescriptor: &metricspb.MetricDescriptor{
						Name:        "counter",
						Description: "This is a counter",
						Unit:        "1",
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						StartTimestamp: startTimestamp,
						Points: []*metricspb.Point{
							{
								Timestamp: endTimestamp,
								Value:     &metricspb.Point_Int64Value{Int64Value: 197},
							},
						},
					},
				},
			},
			wantCount: 1,
		},
	}

	for i, tt := range tests {
		reg := prometheus.NewRegistry()
		collector := newCollector(Options{}, reg)
		collector.addMetric(tt.metric)
		mm, err := reg.Gather()
		if err != nil {
			t.Errorf("#%d: Gather error: %v", i, err)
		}
		reg.Unregister(collector)
		if got, want := len(mm), tt.wantCount; got != want {
			t.Errorf("#%d: Got %d Want %d", i, got, want)
		}
	}
}

func TestCollectNonRacy(t *testing.T) {
	exp, err := New(Options{})
	if err != nil {
		t.Fatalf("NewExporter failed: %v", err)
	}
	collector := exp.collector

	// Synchronization to ensure that every goroutine terminates before we exit.
	var waiter sync.WaitGroup
	waiter.Add(3)
	defer waiter.Wait()

	doneCh := make(chan bool)

	// 1. Simulate metrics write route with a period of 700ns.
	go func() {
		defer waiter.Done()
		tick := time.NewTicker(700 * time.Nanosecond)

		defer func() {
			tick.Stop()
			close(doneCh)
		}()

		for i := 0; i < 1e3; i++ {
			metrics := []*metricspb.Metric{
				{
					Descriptor_: &metricspb.Metric_MetricDescriptor{
						MetricDescriptor: &metricspb.MetricDescriptor{
							Name:        "with_metric_descriptor",
							Description: "This is a test",
							Unit:        "By",
						},
					},
					Timeseries: []*metricspb.TimeSeries{
						{
							StartTimestamp: startTimestamp,
							Points: []*metricspb.Point{
								{
									Timestamp: endTimestamp,
									Value: &metricspb.Point_DistributionValue{
										DistributionValue: &metricspb.DistributionValue{
											Count:                 int64(i + 10),
											Sum:                   11.9 + float64(i),
											SumOfSquaredDeviation: 0,
											Buckets: []*metricspb.DistributionValue_Bucket{
												{}, {Count: 1}, {}, {}, {},
											},
											BucketOptions: &metricspb.DistributionValue_BucketOptions{
												Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
													Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
														Bounds: []float64{0, 10, 20, 30, 40},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Descriptor_: &metricspb.Metric_MetricDescriptor{
						MetricDescriptor: &metricspb.MetricDescriptor{
							Name:        "counter",
							Description: "This is a counter",
							Unit:        "1",
						},
					},
					Timeseries: []*metricspb.TimeSeries{
						{
							StartTimestamp: startTimestamp,
							Points: []*metricspb.Point{
								{
									Timestamp: endTimestamp,
									Value:     &metricspb.Point_Int64Value{Int64Value: int64(i)},
								},
							},
						},
					},
				},
			}

			for _, metric := range metrics {
				if err := exp.ExportMetric(context.Background(), nil, nil, metric); err != nil {
					t.Errorf("Iteration #%d:: unexpected ExportMetric error: %v", i, err)
				}
				<-tick.C
			}
		}
	}()

	inMetricsChan := make(chan prometheus.Metric, 1000)
	// 2. Simulate the Prometheus metrics consumption routine running at 900ns.
	go func() {
		defer waiter.Done()
		tick := time.NewTicker(900 * time.Nanosecond)
		defer tick.Stop()

		for {
			select {
			case <-doneCh:
				return

			case <-inMetricsChan:
			case <-tick.C:
			}
		}
	}()

	// 3. Collect/Read routine running at 800ns.
	go func() {
		defer waiter.Done()
		tick := time.NewTicker(800 * time.Nanosecond)
		defer tick.Stop()

		for {
			select {
			case <-doneCh:
				return

			case <-tick.C:
				// Perform some collection here.
				collector.Collect(inMetricsChan)
			}
		}
	}()
}
