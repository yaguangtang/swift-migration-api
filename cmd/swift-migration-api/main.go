package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"swift-migration-api/internal/admin"
	"swift-migration-api/internal/auth"
	"swift-migration-api/internal/backend"
	"swift-migration-api/internal/config"
	"swift-migration-api/internal/logging"
	"swift-migration-api/internal/migration"
	"swift-migration-api/internal/proxy"
	"swift-migration-api/internal/store"
)

func main() {
	os.Exit(run())
}

func run() int {
	logger := logging.New()

	configPath := flag.String("config", "", "Path to YAML or JSON config file")
	flag.Parse()

	if *configPath == "" {
		logger.Error("missing -config")
		return 1
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		return 1
	}

	cephClient, err := backend.New("ceph", cfg.Backends.Ceph)
	if err != nil {
		logger.Error("failed to initialize ceph backend", "error", err)
		return 1
	}
	swiftClient, err := backend.New("swift", cfg.Backends.Swift)
	if err != nil {
		logger.Error("failed to initialize swift backend", "error", err)
		return 1
	}

	jobStore, err := store.NewSQLiteStore(cfg.Migration.QueueStore)
	if err != nil {
		logger.Error("failed to initialize job store", "error", err)
		return 1
	}
	defer jobStore.Close()

	var tokenSource auth.TokenSource
	if cfg.Auth.WorkerKeystone.AuthURL != "" {
		tokenSource = auth.NewKeystoneTokenSource(&http.Client{Timeout: 30 * time.Second}, cfg.Auth.WorkerKeystone)
	} else {
		logger.Warn("worker keystone credentials not configured; background copy operations may fail")
	}

	migrationService := migration.NewService(cephClient, swiftClient, jobStore, tokenSource, cfg.Migration, logger.With("component", "migration"))
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	migrationService.Start(rootCtx)

	headerForwarder := auth.NewHeaderForwarder(cfg.Auth.ForwardHeaders)
	adminHandler := admin.NewHandler(migrationService, cfg.Admin.AuthToken)
	proxyHandler := proxy.NewHandler(
		cephClient,
		swiftClient,
		migrationService,
		headerForwarder,
		cfg.Migration.CopyOnHeadMiss,
		cfg.Migration.ScanOnContainerList,
		logger.With("component", "proxy"),
	)

	mux := http.NewServeMux()
	mux.Handle("/_migration/", adminHandler)
	mux.Handle("/", requestLogger(logger, proxyHandler))

	server := &http.Server{
		Addr:         cfg.Server.Listen,
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout.Std(),
		WriteTimeout: cfg.Server.WriteTimeout.Std(),
	}

	go func() {
		<-rootCtx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("server shutdown failed", "error", err)
		}
	}()

	logger.Info("starting swift migration proxy", "listen", cfg.Server.Listen, "tls_enabled", cfg.Server.TLS.Enabled)
	var serveErr error
	if cfg.Server.TLS.Enabled {
		serveErr = server.ListenAndServeTLS(cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile)
	} else {
		serveErr = server.ListenAndServe()
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		logger.Error("server exited with error", "error", serveErr)
		return 1
	}
	logger.Info("server stopped")
	return 0
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("request completed",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"duration", time.Since(start).String(),
		)
	})
}

func fatalf(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	return 1
}
