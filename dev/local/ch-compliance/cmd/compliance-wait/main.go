package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
)

func main() {
	fmt.Println(">> waiting for prometheus API")
	bo := backoff.NewExponentialBackOff()
	_ = backoff.RetryNotify(func() error {
		for _, s := range []string{
			"http://localhost:9090",
			"http://localhost:9091",
			"http://localhost:9095",
			"http://localhost:9096",
		} {
			res, err := http.Get(s) // #nosec G107
			if err != nil {
				return err
			}
			_ = res.Body.Close()
		}
		return nil
	}, bo, func(err error, d time.Duration) {
		fmt.Println(err)
	})
	fmt.Println(">> prometheus api ready")
	for i := 0; i < 3; i++ {
		fmt.Println(">> waiting for some scrapes")
		time.Sleep(time.Second * 1)
	}
	fmt.Println(">> READY")
}
