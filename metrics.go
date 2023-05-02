package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metricsClient struct {
	srv               *http.Server
	changesReplicated *prometheus.CounterVec
}

func newMetricsClient(listenPort int) *metricsClient {
	handler := http.NewServeMux()
	handler.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", listenPort),
		Handler: handler,
	}

	go srv.ListenAndServe()

	return &metricsClient{
		srv: srv,
		changesReplicated: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mssql2pg",
				Name:      "changes_replicated_total",
				Help:      "Total number of table changes read from source DB and written to destination DB",
			},
			[]string{"table", "op"},
		),
	}
}

func (c *metricsClient) shutdown() {
	c.srv.Shutdown(context.Background())
	prometheus.DefaultRegisterer.Unregister(c.changesReplicated)
}
