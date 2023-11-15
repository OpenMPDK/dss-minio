/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/minio/minio/cmd/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "minio_http_requests_duration_seconds",
			Help:    "Time taken by requests served by current Minio server instance",
			Buckets: []float64{.001, .003, .005, .1, .5, 1},
		},
		[]string{"request_type"},
	)
)

// Commented out this collector registration since Prometheus is running collect twice
// Couldn't see any difference in functionality
func init() {
	prometheus.MustRegister(httpRequestsDuration)
}

// newMinioCollector describes the collector
// and returns reference of minioCollector
// It creates the Prometheus Description which is used
// to define metric and  help string
func newMinioCollector() *minioCollector {
	return &minioCollector{
		desc: prometheus.NewDesc("minio_stats", "Statistics exposed by Minio server", nil, nil),
	}
}

// minioCollector is the Custom Collector
type minioCollector struct {
	desc *prometheus.Desc
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioCollector) Collect(ch chan<- prometheus.Metric) {

	globalMetricsChan<- 1
	// Waiting to make sure metrics thread finishes before
	<-globalMetricsChan
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "metrics", "iops"),
			"Current IOPS of this Minio instance",
			nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&globalCurrIOCount)),
	)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "metrics", "bw"),
			"Current BW of this Minio instance",
			nil, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&globalCurrBW)),
	)
	// Reset counters once reported
	atomic.StoreUint64(&globalCurrIOCount, 0)
	atomic.StoreUint64(&globalCurrBW, 0)
	
	// Always expose network stats

	// Network Sent/Received Bytes
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "network", "sent_bytes_total"),
			"Total number of bytes sent by current Minio server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(globalConnStats.getTotalOutputBytes()),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "network", "received_bytes_total"),
			"Total number of bytes received by current Minio server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(globalConnStats.getTotalInputBytes()),
	)

	// Expose cache stats only if available
	cacheObjLayer := newCacheObjectsFn()
	if cacheObjLayer != nil {
		cs := cacheObjLayer.StorageInfo(context.Background())
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("minio", "disk", "cache_storage_bytes"),
				"Total cache capacity on current Minio server instance",
				nil, nil),
			prometheus.GaugeValue,
			float64(cs.Total),
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("minio", "disk", "cache_storage_free_bytes"),
				"Total cache available on current Minio server instance",
				nil, nil),
			prometheus.GaugeValue,
			float64(cs.Free),
		)
	}

	// Expose disk stats only if applicable

	// Fetch disk space info
	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		return
	}

	s := objLayer.StorageInfo(context.Background())

	// Gateways don't provide disk info
	if s.Backend.Type == Unknown {
		return
	}

	var totalDisks, offlineDisks int
	// Setting totalDisks to 1 and offlineDisks to 0 in FS mode
	if s.Backend.Type == BackendFS {
		totalDisks = 1
		offlineDisks = 0
	} else {
		offlineDisks = s.Backend.OfflineDisks
		totalDisks = s.Backend.OfflineDisks + s.Backend.OnlineDisks
	}

	// Total disk usage by current Minio server instance
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "disk", "storage_used_bytes"),
			"Total disk storage used by current Minio server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(s.Used),
	)

	// Total disk capacity by this Minio instance
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "disk", "storage_total_capacity_bytes"),
			"Total disk storage capacity by current Minio server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(s.Total),
	)

	// Minio Total Disk/Offline Disk
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "total", "disks"),
			"Total number of disks for current Minio server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(totalDisks),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "offline", "disks"),
			"Total number of offline disks for current Minio server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(offlineDisks),
	)
}

func metricsHandler() http.Handler {
	registry := prometheus.NewRegistry()

	err := registry.Register(httpRequestsDuration)
	logger.LogIf(context.Background(), err)

	err = registry.Register(newMinioCollector())
	logger.LogIf(context.Background(), err)

	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		registry,
	}
	// Delegate http serving to Prometheus client library, which will call collector.Collect.
	return promhttp.InstrumentMetricHandler(
		registry,
		promhttp.HandlerFor(gatherers,
			promhttp.HandlerOpts{
				ErrorHandling: promhttp.ContinueOnError,
			}),
	)
}
