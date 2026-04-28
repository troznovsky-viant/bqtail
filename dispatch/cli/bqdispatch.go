package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/bqtail/dispatch"
	"github.com/viant/bqtail/dispatch/contract"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// dispatchLivenessTimeout: /healthz reports unhealthy if no dispatch cycle
// has completed within this window. Normal cycles take ~10s, so 3min covers
// transient slowness while still surfacing a stuck dispatcher quickly enough
// for Cloud Run's liveness probe to restart the container.
const dispatchLivenessTimeout = 3 * time.Minute

// lastCycleUnixNano holds the wall-clock UnixNano of the most recently
// completed dispatch cycle, populated via the dispatch.Service cycle hook.
var lastCycleUnixNano atomic.Int64

// startupTime is used as a grace baseline before the first cycle completes,
// so /healthz returns 200 during initial probe windows.
var startupTime = time.Now()

func main() {
	config := os.Getenv("CONFIG")
	if config == "" {
		panic("env.CONFIG was empty\n")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Pre-warm the dispatch service and register the cycle hook before the
	// dispatch goroutine starts, so every cycle is observed by /healthz.
	service, err := dispatch.Singleton(ctx)
	if err != nil {
		log.Fatalf("dispatch init: %v", err)
	}
	service.SetCycleHook(func(t time.Time) {
		lastCycleUnixNano.Store(t.UnixNano())
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler)
	mux.HandleFunc("/", healthzHandler)
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	// When FUNCTION_TIMEOUT_SEC is set, the dispatch service runs cycles within
	// that window and returns; the process then exits. When unset, the process
	// runs forever (until SIGTERM), restarting Dispatch on errors.
	timeoutSet := os.Getenv("FUNCTION_TIMEOUT_SEC") != ""

	go func() {
		defer stop() // cancels root ctx so main's <-ctx.Done() fires when we return
		for ctx.Err() == nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("dispatch panic recovered: %v\n", r)
					}
				}()
				_, err := handleDispatchEvent(ctx)
				if err != nil && ctx.Err() == nil && !timeoutSet {
					log.Printf("dispatch error (restarting in 10s): %v\n", err)
					time.Sleep(10 * time.Second)
				}
			}()
			if timeoutSet {
				return
			}
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	ns := lastCycleUnixNano.Load()
	if ns == 0 {
		// no cycle has completed yet; allow startup grace
		age := time.Since(startupTime)
		if age > dispatchLivenessTimeout {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "dispatch stalled: no cycle completed in %s since startup (threshold %s)\n", age.Round(time.Second), dispatchLivenessTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "starting: no cycle completed yet, %s since startup\n", age.Round(time.Second))
		return
	}
	age := time.Since(time.Unix(0, ns))
	if age > dispatchLivenessTimeout {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "dispatch stalled: last cycle %s ago (threshold %s)\n", age.Round(time.Second), dispatchLivenessTimeout)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok: last cycle %s ago\n", age.Round(time.Second))
}

func handleDispatchEvent(ctx context.Context) (*contract.Response, error) {
	service, err := dispatch.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Dispatch(ctx)
	response.Lock()
	defer response.UnLock()
	defer func() {
		r := recover()
		if r != nil {
			log.Printf("recovered: %v\n", r)
		}
	}()
	data, _ := json.Marshal(response)
	fmt.Printf("%s\n", data)
	fmt.Printf("response size: %d bytes, jobs: %d, batched: %d, errors: %d, cycles: %d\n",
		len(data), len(response.Jobs.Jobs), len(response.Batched), len(response.Errors), response.Cycles)
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}
