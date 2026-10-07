package config

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// SignalContext is cancelled on SIGINT or SIGTERM. Kubernetes sends SIGTERM when it stops a
// pod, then waits terminationGracePeriodSeconds before SIGKILL; everything shuts down inside
// that window.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// Serve runs an HTTP server until ctx is cancelled, then shuts it down gracefully.
func Serve(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", addr)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Fatal logs and exits; used for startup errors.
func Fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "error", err.Error())
	os.Exit(1)
}
