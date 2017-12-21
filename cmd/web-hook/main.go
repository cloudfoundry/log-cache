package main

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"code.cloudfoundry.org/go-envstruct"
	gologcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/log-cache"
)

func main() {
	log.Print("Starting LogCache WebHook...")
	defer log.Print("Closing LogCache WebHook.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	client := gologcache.NewClient(cfg.LogCacheAddr)

	templateFs, err := os.Open(cfg.TemplatePath)
	if err != nil {
		log.Fatalf("failed to open template %s: %s", cfg.TemplatePath, err)
	}

	template, err := ioutil.ReadAll(templateFs)
	if err != nil {
		log.Fatalf("failed to read template %s: %s", cfg.TemplatePath, err)
	}

	loggr := log.New(os.Stderr, "[WEBHOOK] ", log.LstdFlags)

	webHook := logcache.NewWebHook(
		cfg.SourceID,
		string(template),
		client.Read,
		logcache.WithWebHookLogger(loggr),
		logcache.WithWebHookInterval(time.Second),
		logcache.WithWebHookErrorHandler(func(e error) {
			loggr.Print(e)
		}),
	)

	webHook.Start()
}
