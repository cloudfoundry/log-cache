package main

import (
	"fmt"
	"log"
	"os"

	"code.cloudfoundry.org/log-cache/internal/promql"
)

func main() {
	log := log.New(os.Stderr, "", 0)

	if len(os.Args) != 2 {
		log.Fatalf("usage: %s [Prometheus Query]", os.Args[0])
	}

	p := promql.New(nil, nil, log)
	sids, err := p.Parse(os.Args[1])
	if err != nil {
		log.Fatal(err.Error())
	}

	for _, sid := range sids {
		fmt.Println(sid)
	}
}
