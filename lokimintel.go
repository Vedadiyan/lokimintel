package lokimintel

import (
	"context"
	"sync"

	"github.com/vedadiyan/lokiclient"
	"github.com/vedadiyan/mintel"
	"github.com/vedadiyan/mintel/util/template"
)

type (
	Kind       string
	LokiWriter struct {
		data   map[string]any
		binder template.Binder
		tel    *LokiClient
		mut    sync.RWMutex
		kind   Kind
	}

	LokiClient struct {
		client   *lokiclient.Client
		metadata mintel.Metadata
		logger   *LokiWriter
		tracer   *LokiWriter
		meter    *LokiWriter
		pool     *sync.Pool
	}
)

const (
	LOG    Kind = "LOG"
	TRACE  Kind = "TRACE"
	METRIC Kind = "METRICS"
)

func New(c *lokiclient.Client, l, t, m template.Binder) mintel.CreateFunc {
	var pool *sync.Pool
	pool = &sync.Pool{
		New: func() any {
			lc := new(LokiClient)
			lc.client = c
			lc.logger = &LokiWriter{
				binder: l,
				tel:    lc,
				data:   make(map[string]any),
			}

			lc.tracer = &LokiWriter{
				binder: l,
				tel:    lc,
				data:   make(map[string]any),
			}

			lc.meter = &LokiWriter{
				binder: l,
				tel:    lc,
				data:   make(map[string]any),
			}

			lc.pool = pool
			return lc
		},
	}

	return func(metadata mintel.Metadata) mintel.Telemetry {
		tel := pool.Get().(*LokiClient)
		tel.metadata = metadata
		return tel
	}
}

func (c *LokiClient) Logger() mintel.Writer {
	return c.logger
}

func (c *LokiClient) Tracer() mintel.Writer {
	return c.tracer
}

func (c *LokiClient) Meter() mintel.Writer {
	return c.meter
}

func (c *LokiClient) Send(k Kind, b template.Binder, v any) {
	value := template.Bind(b, v)
	stream := lokiclient.CopyStream(lokiclient.Stream(c.metadata))
	stream["kind"] = string(k)
	c.client.Log(context.Background(), stream, lokiclient.Value(value))
}

func (c *LokiClient) Close() {
	defer c.pool.Put(c)
	defer c.logger.Clear()
	defer c.tracer.Clear()
	defer c.meter.Clear()
	c.Logger().Flush()
	c.Tracer().Flush()
	c.Meter().Flush()
	c.metadata = nil
}

func (c *LokiWriter) Add(kvs ...*mintel.KeyValue) mintel.Writer {
	c.mut.Lock()
	defer c.mut.Unlock()
	for _, kv := range kvs {
		c.data[kv.Key] = kv.Value
	}
	return c
}

func (c *LokiWriter) Flush() {
	defer c.Clear()
	c.mut.RLock()
	defer c.mut.RUnlock()
	if len(c.data) == 0 {
		return
	}
	c.tel.Send(c.kind, c.binder, c.data)
}

func (c *LokiWriter) Clear() {
	c.mut.Lock()
	defer c.mut.Unlock()
	clear(c.data)
}
