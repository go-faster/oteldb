package main

import (
	"fmt"
	"strconv"
)

var (
	names = []string{
		"grey",
		"red",
		"green",
		"yellow",
		"blue",
		"magenta",
		"cyan",
		"white",
	}

	resetColor = ansi("0")
	colors     = func() map[string]string {
		m := map[string]string{}
		for i, name := range names {
			m[name] = ansi(strconv.Itoa(30 + i))
			m["intense_"+name] = ansi(strconv.Itoa(30+i) + ";1")
		}
		return m
	}()
)

func ansi(code string) string {
	return fmt.Sprintf("\033[%sm", code)
}
