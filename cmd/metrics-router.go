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
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

const (
	prometheusMetricsPath = "/prometheus/metrics"
	kvMetricsPath         = "/kvstats"
	clusterIDPath         = "/cluster_id"
)

// registerMetricsRouter - add handler functions for metrics.
func registerMetricsRouter(router *mux.Router) {
	// metrics router
	metricsRouter := router.NewRoute().PathPrefix(minioReservedBucketPath).Subrouter()
	metricsRouter.Handle(prometheusMetricsPath, metricsHandler())
	metricsRouter.Methods(http.MethodGet).Path(kvMetricsPath).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := make(chan []byte)
		globalDumpKVStatsCh <- c
		b := <-c
		w.Write(b)
	})
	metricsRouter.Methods(http.MethodGet).Path(clusterIDPath).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := struct {
			UUID string
		}{
			UUID: clusterID,
		}
		b, err := json.Marshal(c)
		if err != nil {
			w.Write([]byte("Error occured when encoding clusterID"))
		} else {
			w.Write(b)
		}
	})
}
