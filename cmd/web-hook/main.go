package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
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

	rand.Seed(time.Now().UnixNano())

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	client := gologcache.NewGroupReaderClient(
		cfg.LogCacheAddr,
		gologcache.WithViaGRPC(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
	)

	for _, t := range cfg.TemplatePaths {
		go startTemplate(t, cfg.GroupPrefix, false, client)
	}

	for _, t := range cfg.FollowTemplatePaths {
		go startTemplate(t, cfg.GroupPrefix, true, client)
	}

	// health endpoint pprof
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}

func startTemplate(info templateInfo, groupPrefix string, follow bool, client *gologcache.GroupReaderClient) {
	templateFs, err := os.Open(info.TemplatePath)
	if err != nil {
		log.Fatalf("failed to open template %s: %s", info.TemplatePath, err)
	}

	template, err := ioutil.ReadAll(templateFs)
	if err != nil {
		log.Fatalf("failed to read template %s: %s", info.TemplatePath, err)
	}

	loggr := log.New(os.Stderr, "[WEBHOOK] ", log.LstdFlags)

	opts := []logcache.WebHookOption{
		logcache.WithWebHookLogger(loggr),
		logcache.WithWebHookInterval(time.Second),
		logcache.WithWebHookErrorHandler(func(e error) {
			loggr.Print(e)
		}),
	}

	if !follow {
		opts = append(opts, logcache.WithWebHookWindowing(time.Hour))
	}

	// Manage the group
	groupName := encodeGroupName(groupPrefix, info)
	go func() {
		for range time.Tick(time.Minute) {
			for _, ID := range info.SourceIDs {
				ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
				if err := client.AddToGroup(ctx, groupName, ID); err != nil {
					loggr.Printf("error while adding source ID %s to group: %s", ID, err)
				}
			}
		}
	}()

	reader := client.BuildReader(rand.Uint64())

	webHook := logcache.NewWebHook(
		groupName,
		string(template),
		logcache.Reader(reader),
		opts...,
	)

	webHook.Start()
}

func encodeGroupName(prefix string, info templateInfo) string {
	f := fnv.New64()
	for _, s := range info.SourceIDs {
		f.Write([]byte(s))
	}
	f.Write([]byte(info.TemplatePath))

	return fmt.Sprintf("%s-%s", prefix, base64.StdEncoding.EncodeToString(f.Sum(nil)))
}
