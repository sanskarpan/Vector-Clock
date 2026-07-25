package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/telemetry"
)

func main() {
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	os.Stderr = w

	shutdown, err := telemetry.Init(context.Background(), telemetry.Config{
		Exporter:    "stdout",
		ServiceName: "test-svc",
		SampleRatio: 1.0,
	})
	if err != nil {
		fmt.Fprintln(os.Stdout, "INIT ERROR:", err)
		os.Stderr = origStderr
		return
	}

	tr := telemetry.Tracer()
	_, span := tr.Start(context.Background(), "hello")
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown(ctx)
	w.Close()
	os.Stderr = origStderr
	data, _ := io.ReadAll(r)
	fmt.Fprintln(os.Stdout, "captured:", string(data))
}
