package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"code.cloudfoundry.org/go-envstruct"
	gologcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/log-cache"
	"google.golang.org/grpc"
)

func main() {
	log.Print("Starting LogCache WebHook...")
	defer log.Print("Closing LogCache WebHook.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	client := gologcache.NewClient(
		cfg.LogCacheAddr,
		gologcache.WithViaGRPC(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
	)

	for _, t := range cfg.TemplatePaths {
		go startTemplate(t.SourceID, t.TemplatePath, false, client)
	}

	for _, t := range cfg.FollowTemplatePaths {
		go startTemplate(t.SourceID, t.TemplatePath, true, client)
	}

	// health endpoint pprof
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}

func startTemplate(sourceID, path string, follow bool, client *gologcache.Client) {
	templateFs, err := os.Open(path)
	if err != nil {
		log.Fatalf("failed to open template %s: %s", path, err)
	}

	template, err := ioutil.ReadAll(templateFs)
	if err != nil {
		log.Fatalf("failed to read template %s: %s", path, err)
	}

	loggr := log.New(os.Stderr, "[WEBHOOK] ", log.LstdFlags)

	opts := []logcache.WebHookOption{
		logcache.WithWebHookLogger(loggr),
		logcache.WithWebHookInterval(time.Second),
		logcache.WithWebHookErrorHandler(func(e error) {
			loggr.Print(e)
		}),
	}

	if follow {
		opts = append(opts, logcache.WithWebHookFollow())
	}

	webHook := logcache.NewWebHook(
		sourceID,
		string(template),
		client.Read,
		opts...,
	)

	webHook.Start()
}
