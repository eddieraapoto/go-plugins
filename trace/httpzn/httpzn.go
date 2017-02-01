package httpzn

import (
	"strconv"
	"time"

	"math/rand"

	"github.com/micro/go-os/trace"
	zp "github.com/micro/go-plugins/trace/zipkin"
)

type zipkin struct {
	opts  trace.Options
	spans chan *trace.Span
	exit  chan bool
}

func newHttpzn(opts ...trace.Option) trace.Trace {
	var opt trace.Options
	for _, o := range opts {
		o(&opt)
	}

	if opt.BatchSize == 0 {
		opt.BatchSize = trace.DefaultBatchSize
	}

	if opt.BatchInterval == time.Duration(0) {
		opt.BatchInterval = trace.DefaultBatchInterval
	}

	if len(opt.Topic) == 0 {
		opt.Topic = zp.TraceTopic
	}

	z := &zipkin{
		exit:  make(chan bool),
		opts:  opt,
		spans: make(chan *trace.Span, 100),
	}

	go z.run()
	return z

}

func (z *zipkin) Collect(s *trace.Span) error {
	z.spans <- s
	return nil
}

func (z *zipkin) Close() error {
	select {
	case <-z.exit:
		return nil
	default:
		close(z.exit)
	}
	return nil
}

func (z *zipkin) NewSpan(s *trace.Span) *trace.Span {
	if s == nil {
		return &trace.Span{
			Id:        strconv.FormatInt(random(), 10),
			TraceId:   strconv.FormatInt(random(), 10),
			ParentId:  "0",
			Timestamp: time.Now(),
			Source:    z.opts.Service,
		}
	}

	if _, err := strconv.ParseInt(s.TraceId, 16, 64); err != nil {
		s.TraceId = strconv.FormatInt(random(), 10)
	}
	if _, err := strconv.ParseInt(s.ParentId, 16, 64); err != nil {
		s.ParentId = "0"
	}
	if _, err := strconv.ParseInt(s.Id, 16, 64); err != nil {
		s.Id = strconv.FormatInt(random(), 10)
	}

	if s.Timestamp.IsZero() {
		s.Timestamp = time.Now()
	}

	return &trace.Span{
		Id:        s.Id,
		TraceId:   s.TraceId,
		ParentId:  s.ParentId,
		Timestamp: s.Timestamp,
	}
}

func (z *zipkin) String() string {
	return "httpzn"
}

func (z *zipkin) run() {
}

func random() int64 {
	return rand.Int63() & 0x001fffffffffffff
}
