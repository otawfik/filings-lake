// Package metrics is a small registry of counters and gauges written in the Prometheus text format.
// It covers what the watcher exposes and nothing more, which keeps the module free of dependencies.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

type family struct {
	name, help, kind string
	labels           []string
	values           map[string]float64 // key: label values joined by \xff
	fn               func() float64     // set for a gauge read at scrape time
}

// Registry is safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
}

func New() *Registry { return &Registry{families: map[string]*family{}} }

// Counter is a value that only goes up, split by its label values.
type Counter struct {
	r *Registry
	f *family
}

// Gauge is a value that is set, split by its label values.
type Gauge struct {
	r *Registry
	f *family
}

func (r *Registry) register(name, help, kind string, labels []string) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.families[name]; dup {
		panic("metrics: " + name + " registered twice")
	}
	f := &family{name: name, help: help, kind: kind, labels: labels, values: map[string]float64{}}
	if len(labels) == 0 {
		f.values[""] = 0 // an unlabelled metric is reported from the start, at zero
	}
	r.families[name] = f
	return f
}

func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	return &Counter{r, r.register(name, help, "counter", labels)}
}

func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	return &Gauge{r, r.register(name, help, "gauge", labels)}
}

// GaugeFunc registers a gauge whose value is read when the metrics are written.
func (r *Registry) GaugeFunc(name, help string, fn func() float64) {
	r.register(name, help, "gauge", nil).fn = fn
}

func (f *family) key(values []string) string {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s takes %d label values, got %d", f.name, len(f.labels), len(values)))
	}
	return strings.Join(values, "\xff")
}

// Add adds v (which must not be negative) to the counter for these label values.
func (c *Counter) Add(v float64, labelValues ...string) {
	if v < 0 {
		panic("metrics: counter " + c.f.name + " cannot go down")
	}
	k := c.f.key(labelValues)
	c.r.mu.Lock()
	c.f.values[k] += v
	c.r.mu.Unlock()
}

func (c *Counter) Inc(labelValues ...string) { c.Add(1, labelValues...) }

func (g *Gauge) Set(v float64, labelValues ...string) {
	k := g.f.key(labelValues)
	g.r.mu.Lock()
	g.f.values[k] = v
	g.r.mu.Unlock()
}

// Value reads a counter or gauge back, for tests and the health endpoint.
func (r *Registry) Value(name string, labelValues ...string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.families[name]
	if !ok {
		return 0
	}
	return f.values[strings.Join(labelValues, "\xff")]
}

var escaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// Write writes every metric, sorted by name and then by label values, so the output is stable.
func (r *Registry) Write(w io.Writer) error {
	type block struct {
		text string         // the HELP and TYPE lines, and the samples of an ordinary metric
		name string         // for a gauge function: its sample is added once the lock is released,
		fn   func() float64 // because the function may take locks of its own
	}
	r.mu.Lock()
	names := make([]string, 0, len(r.families))
	for n := range r.families {
		names = append(names, n)
	}
	sort.Strings(names)
	blocks := make([]block, 0, len(names))
	for _, n := range names {
		f := r.families[n]
		var b strings.Builder
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", f.name, strings.ReplaceAll(f.help, "\n", " "), f.name, f.kind)
		keys := make([]string, 0, len(f.values))
		if f.fn == nil {
			for k := range f.values {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString(f.name)
			if len(f.labels) > 0 {
				b.WriteByte('{')
				for i, v := range strings.Split(k, "\xff") {
					if i > 0 {
						b.WriteByte(',')
					}
					fmt.Fprintf(&b, `%s="%s"`, f.labels[i], escaper.Replace(v))
				}
				b.WriteByte('}')
			}
			fmt.Fprintf(&b, " %g\n", f.values[k])
		}
		blocks = append(blocks, block{b.String(), f.name, f.fn})
	}
	r.mu.Unlock()

	var out strings.Builder
	for _, bl := range blocks {
		out.WriteString(bl.text)
		if bl.fn != nil {
			fmt.Fprintf(&out, "%s %g\n", bl.name, bl.fn())
		}
	}
	_, err := io.WriteString(w, out.String())
	return err
}
