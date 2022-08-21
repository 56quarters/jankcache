package client

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var hits = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "whatever",
	Help: "HITS!",
})

var misses = promauto.With(nil).NewCounter(prometheus.CounterOpts{
	Name: "whatever2",
	Help: "MISSES!",
})
