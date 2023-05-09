package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/grafana/dskit/services"

	"github.com/56quarters/jankcache/server/cache"
	"github.com/56quarters/jankcache/server/proto"
)

// RuntimeSnapshot is a snapshot of the state maintained by RuntimeContext
type RuntimeSnapshot struct {
	Uptime    uint64
	Time      int64
	Pid       int
	UserCPU   float64
	SystemCPU float64
}

// RuntimeContext periodically updates runtime specific information used for building Stats.
type RuntimeContext struct {
	services.Service

	startup   time.Time
	now       time.Time
	pid       int
	userCPU   float64
	systemCPU float64
	mtx       sync.RWMutex
}

func NewRuntimeContext() *RuntimeContext {
	r := &RuntimeContext{
		startup: time.Now(),
		now:     time.Now(),
		pid:     os.Getpid(),
		mtx:     sync.RWMutex{},
	}

	r.Service = services.NewBasicService(nil, r.loop, nil)
	return r
}

func (r *RuntimeContext) loop(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			userCPU, systemCPU, err := getUserSystemCPU()
			if err != nil {
				return err
			}

			r.mtx.Lock()
			r.now = t
			r.userCPU = userCPU
			r.systemCPU = systemCPU
			r.mtx.Unlock()
		}
	}
}

func (r *RuntimeContext) Read() RuntimeSnapshot {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	return RuntimeSnapshot{
		Uptime:    uint64(r.now.Sub(r.startup).Seconds()),
		Time:      r.now.Unix(),
		Pid:       r.pid,
		UserCPU:   r.userCPU,
		SystemCPU: r.systemCPU,
	}
}

func getUserSystemCPU() (float64, float64, error) {
	payload := syscall.Rusage{}
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &payload); err != nil {
		return 0, 0, err
	}

	return timevalToFloat(payload.Utime), timevalToFloat(payload.Stime), nil
}

func timevalToFloat(v syscall.Timeval) float64 {
	return float64(v.Sec) + float64(v.Usec)/1_000_000.0
}

// Metrics is a bundle of server-centric metrics updated by TCPServer and Handler.
type Metrics struct {
	CurrentConnections  atomic.Int64
	MaxConnections      atomic.Uint64
	TotalConnections    atomic.Uint64
	RejectedConnections atomic.Uint64
	BytesWritten        atomic.Uint64
	BytesRead           atomic.Uint64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

// NewStats creates a new Stats object for use as a response to a Memcached `stats` command.
func NewStats(c *cache.Cache, m *Metrics, r RuntimeSnapshot) Stats {
	cacheMetrics := c.Metrics()

	return Stats{
		Pid:        r.Pid,
		Uptime:     r.Uptime,
		ServerTime: r.Time,
		Version:    version,

		UserCPU:   r.UserCPU,
		SystemCPU: r.SystemCPU,

		MaxConnections:      m.MaxConnections.Load(),
		CurrentConnections:  uint64(m.CurrentConnections.Load()),
		TotalConnections:    m.TotalConnections.Load(),
		RejectedConnections: m.RejectedConnections.Load(),

		Gets:    cacheMetrics.GetsKept(),
		Sets:    cacheMetrics.KeysAdded() + cacheMetrics.KeysUpdated(),
		Flushes: 0,
		Touches: 0,
		Meta:    0,

		GetHits:    cacheMetrics.Hits(),
		GetMisses:  cacheMetrics.Misses(),
		GetExpired: 0,
		GetFlushed: 0,

		StoreTooLarge: 0,
		StoreNoMemory: 0,

		DeleteHits:   0,
		DeleteMisses: 0,

		IncrHits:   0,
		IncrMisses: 0,

		DecrHits:   0,
		DecrMisses: 0,

		TouchHits:   0,
		TouchMisses: 0,

		BytesRead:    m.BytesRead.Load(),
		BytesWritten: m.BytesWritten.Load(),
		Bytes:        cacheMetrics.CostAdded() - cacheMetrics.CostEvicted(),
		MaxBytes:     c.MaxBytes(),

		CurrentItems: cacheMetrics.KeysAdded() - cacheMetrics.KeysEvicted(),
		TotalItems:   cacheMetrics.KeysAdded(),
		Evictions:    cacheMetrics.KeysEvicted(),
	}
}

// Stats is the collection of statistics emitted as part of a Memcached `stats` command.
type Stats struct {
	Pid        int
	Uptime     uint64
	ServerTime int64
	Version    string

	UserCPU   float64
	SystemCPU float64

	MaxConnections      uint64
	CurrentConnections  uint64
	TotalConnections    uint64
	RejectedConnections uint64

	Gets    uint64
	Sets    uint64
	Flushes uint64
	Touches uint64
	Meta    uint64

	GetHits    uint64
	GetMisses  uint64
	GetExpired uint64
	GetFlushed uint64

	StoreTooLarge uint64
	StoreNoMemory uint64

	DeleteHits   uint64
	DeleteMisses uint64

	IncrHits   uint64
	IncrMisses uint64

	DecrHits   uint64
	DecrMisses uint64

	TouchHits   uint64
	TouchMisses uint64

	BytesRead    uint64
	BytesWritten uint64
	Bytes        uint64
	MaxBytes     uint64

	CurrentItems uint64
	TotalItems   uint64
	Evictions    uint64
}

func (s *Stats) MarshallMemcached(o *proto.Encoder) {
	o.Line(fmt.Sprintf("STAT %s %d", "pid", s.Pid))
	o.Line(fmt.Sprintf("STAT %s %d", "uptime", s.Uptime))
	o.Line(fmt.Sprintf("STAT %s %d", "time", s.ServerTime))
	o.Line(fmt.Sprintf("STAT %s %s", "version", s.Version))

	o.Line(fmt.Sprintf("STAT %s %f", "rusage_user", s.UserCPU))
	o.Line(fmt.Sprintf("STAT %s %f", "rusage_system", s.SystemCPU))

	o.Line(fmt.Sprintf("STAT %s %d", "max_connections", s.MaxConnections))
	o.Line(fmt.Sprintf("STAT %s %d", "curr_connections", s.CurrentConnections))
	o.Line(fmt.Sprintf("STAT %s %d", "total_connections", s.TotalConnections))
	o.Line(fmt.Sprintf("STAT %s %d", "rejected_connections", s.RejectedConnections))

	o.Line(fmt.Sprintf("STAT %s %d", "cmd_get", s.Gets))
	o.Line(fmt.Sprintf("STAT %s %d", "cmd_set", s.Sets))
	o.Line(fmt.Sprintf("STAT %s %d", "cmd_flush", s.Flushes))
	o.Line(fmt.Sprintf("STAT %s %d", "cmd_touch", s.Touches))
	o.Line(fmt.Sprintf("STAT %s %d", "cmd_meta", s.Meta))

	o.Line(fmt.Sprintf("STAT %s %d", "get_hits", s.GetHits))
	o.Line(fmt.Sprintf("STAT %s %d", "get_misses", s.GetMisses))
	o.Line(fmt.Sprintf("STAT %s %d", "get_expired", s.GetExpired))
	o.Line(fmt.Sprintf("STAT %s %d", "get_flushed", s.GetFlushed))

	o.Line(fmt.Sprintf("STAT %s %d", "store_too_large", s.StoreTooLarge))
	o.Line(fmt.Sprintf("STAT %s %d", "store_no_memory", s.StoreNoMemory))

	o.Line(fmt.Sprintf("STAT %s %d", "delete_hits", s.DeleteHits))
	o.Line(fmt.Sprintf("STAT %s %d", "delete_misses", s.DeleteMisses))

	o.Line(fmt.Sprintf("STAT %s %d", "incr_hits", s.IncrHits))
	o.Line(fmt.Sprintf("STAT %s %d", "incr_misses", s.IncrMisses))

	o.Line(fmt.Sprintf("STAT %s %d", "decr_hits", s.DecrHits))
	o.Line(fmt.Sprintf("STAT %s %d", "decr_misses", s.DecrMisses))

	o.Line(fmt.Sprintf("STAT %s %d", "touch_hits", s.TouchHits))
	o.Line(fmt.Sprintf("STAT %s %d", "touch_misses", s.TouchMisses))

	o.Line(fmt.Sprintf("STAT %s %d", "bytes_read", s.BytesRead))
	o.Line(fmt.Sprintf("STAT %s %d", "bytes_written", s.BytesWritten))
	o.Line(fmt.Sprintf("STAT %s %d", "bytes", s.Bytes))
	o.Line(fmt.Sprintf("STAT %s %d", "limit_maxbytes", s.MaxBytes))

	o.Line(fmt.Sprintf("STAT %s %d", "curr_items", s.CurrentItems))
	o.Line(fmt.Sprintf("STAT %s %d", "total_items", s.TotalItems))
	o.Line(fmt.Sprintf("STAT %s %d", "evictions", s.Evictions))

	o.End()
}
