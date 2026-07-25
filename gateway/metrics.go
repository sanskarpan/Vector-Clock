package gateway

import (
	"fmt"
	"io"
	"sort"
	"strings"

	dto "github.com/prometheus/client_model/go"
)

// renderPrometheus writes the gathered metric families in Prometheus text
// exposition format. Implemented inline to avoid the dependency surface of
// promhttp.Handler() and to keep handler ordering observable in tests.
func renderPrometheus(w io.Writer, families []*dto.MetricFamily) {
	for _, f := range families {
		writeFamily(w, f)
	}
}

func writeFamily(w io.Writer, f *dto.MetricFamily) {
	var name string
	switch f.GetType() {
	case dto.MetricType_COUNTER:
		name = "# TYPE " + f.GetName() + " counter\n"
	case dto.MetricType_GAUGE:
		name = "# TYPE " + f.GetName() + " gauge\n"
	case dto.MetricType_HISTOGRAM:
		name = "# TYPE " + f.GetName() + " histogram\n"
	case dto.MetricType_SUMMARY:
		name = "# TYPE " + f.GetName() + " summary\n"
	case dto.MetricType_UNTYPED:
		name = "# TYPE " + f.GetName() + " untyped\n"
	default:
		return
	}
	if _, err := io.WriteString(w, name); err != nil {
		return
	}
	// Sort metrics within a family for deterministic output (helpful in tests).
	metrics := f.GetMetric()
	sort.SliceStable(metrics, func(i, j int) bool {
		return labelsKey(metrics[i].GetLabel()) < labelsKey(metrics[j].GetLabel())
	})
	for _, m := range metrics {
		writeMetric(w, f.GetName(), f.GetType(), m)
	}
}

func labelsKey(labels []*dto.LabelPair) string {
	parts := make([]string, 0, len(labels))
	for _, lp := range labels {
		parts = append(parts, lp.GetName()+"="+lp.GetValue())
	}
	return strings.Join(parts, ",")
}

func writeMetric(w io.Writer, name string, t dto.MetricType, m *dto.Metric) {
	labels := renderLabels(m.GetLabel())
	switch t {
	case dto.MetricType_COUNTER:
		fmt.Fprintf(w, "%s%s %g\n", name, labels, m.GetCounter().GetValue())
	case dto.MetricType_GAUGE:
		fmt.Fprintf(w, "%s%s %g\n", name, labels, m.GetGauge().GetValue())
	case dto.MetricType_UNTYPED:
		fmt.Fprintf(w, "%s%s %g\n", name, labels, m.GetUntyped().GetValue())
	case dto.MetricType_HISTOGRAM:
		h := m.GetHistogram()
		fmt.Fprintf(w, "%s_bucket%s %d\n", name, appendLabel(labels, "le", "+Inf"), h.GetSampleCount())
		// Bucket boundaries in ascending order.
		buckets := h.GetBucket()
		sort.SliceStable(buckets, func(i, j int) bool {
			return buckets[i].GetUpperBound() < buckets[j].GetUpperBound()
		})
		for _, b := range buckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", name,
				appendLabel(labels, "le", formatFloat(b.GetUpperBound())),
				b.GetCumulativeCount())
		}
		fmt.Fprintf(w, "%s_sum%s %g\n", name, labels, h.GetSampleSum())
		fmt.Fprintf(w, "%s_count%s %d\n", name, labels, h.GetSampleCount())
	case dto.MetricType_SUMMARY:
		s := m.GetSummary()
		for _, q := range s.GetQuantile() {
			fmt.Fprintf(w, "%s%s %g\n", name,
				appendLabel(labels, "quantile", formatFloat(q.GetQuantile())),
				q.GetValue())
		}
		fmt.Fprintf(w, "%s_sum%s %g\n", name, labels, s.GetSampleSum())
		fmt.Fprintf(w, "%s_count%s %d\n", name, labels, s.GetSampleCount())
	}
}

func renderLabels(labels []*dto.LabelPair) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for _, lp := range labels {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, lp.GetName(), escapeLabelValue(lp.GetValue())))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// appendLabel appends one extra label pair to an existing label string.
// Optimised for the common case of no other labels.
func appendLabel(existing, name, value string) string {
	if existing == "" {
		return fmt.Sprintf(`{%s="%s"}`, name, escapeLabelValue(value))
	}
	return existing[:len(existing)-1] + fmt.Sprintf(`,%s="%s"}`, name, escapeLabelValue(value))
}

func escapeLabelValue(s string) string {
	return strings.ReplaceAll(s, "\\", "\\\\")
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%g", f)
}
