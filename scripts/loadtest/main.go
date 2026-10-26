// Command loadtest submits many jobs to the API to exercise the system.
package main

import (
	"flag"
	"fmt"
)

func main() {
	jobs := flag.Int("jobs", 100, "number of jobs to submit")
	url := flag.String("url", "http://localhost:8080", "API base URL")
	flag.Parse()
	// TODO: POST /api/v1/jobs concurrently and report latency.
	fmt.Printf("loadtest: would submit %d jobs to %s\n", *jobs, *url)
}
