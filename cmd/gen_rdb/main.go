// +build ignore
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jmcvetta/randutil"
)

var (
	flHost string

	flNbLists   int
	flNbSets    int
	flNbHashes  int
	flNbStrings int

	flListsPrefix   string
	flSetsPrefix    string
	flHashesPrefix  string
	flStringsPrefix string

	pool *redis.Pool
	wg   sync.WaitGroup
)

func init() {
	flag.StringVar(&flHost, "H", "", "The redis hostname")

	flag.IntVar(&flNbLists, "nb-lists", 1000, "The number of lists to generate")
	flag.IntVar(&flNbSets, "nb-sets", 1000, "The number of sets to generate")
	flag.IntVar(&flNbHashes, "nb-hashes", 1000, "The number of hashes to generate")
	flag.IntVar(&flNbStrings, "nb-strings", 10000, "The number of strings to generate")

	flag.StringVar(&flListsPrefix, "lists-prefix", "", "The prefix for each list")
	flag.StringVar(&flSetsPrefix, "sets-prefix", "", "The prefix for each set")
	flag.StringVar(&flHashesPrefix, "hashes-prefix", "", "The prefix for each hash")
	flag.StringVar(&flStringsPrefix, "strings-prefix", "", "The prefix for each string")
}

func genRandomInts(min, max int) <-chan int {
	ch := make(chan int, 100000)
	go func() {
		for {
			n, _ := randutil.IntRange(min, max)
			ch <- n
		}
	}()
	return ch
}

func genRandomStrings() <-chan string {
	ch := make(chan string, 100000)
	go func() {
		intsCh := genRandomInts(20, 600)
		for {
			n := <-intsCh
			s, _ := randutil.AlphaString(n)

			ch <- s
		}
	}()
	return ch
}

type part struct {
	min int
	max int
}

func genLists() {
	var parts []part

	nbPerPart := flNbLists / 8
	for i := 0; i < flNbLists; i += nbPerPart {
		parts = append(parts, part{
			min: i,
			max: i + nbPerPart - 1,
		})
	}

	for _, p := range parts {
		// go func(p part) {
		conn := pool.Get()
		defer func() {
			conn.Close()
			wg.Done()
		}()

		intsCh := genRandomInts(5000, 12000)
		stringsCh := genRandomStrings()

		log.Println("wait 5s for the gen ints and strings to warm up")
		time.Sleep(5 * time.Second)
		log.Println("start sending commands")

		var nbQueued int
		queueOp := func(cmd string, args ...interface{}) {
			if nbQueued >= 16 {
				conn.Flush()
				nbQueued = 0
			}
			conn.Send(cmd, args...)
			nbQueued++
		}

		for i := 0; i < p.max-p.min; i++ {
			keyName := fmt.Sprintf("%slist%d", flListsPrefix, i)

			n := <-intsCh
			for i := 0; i < n; i++ {
				queueOp("LPUSH", keyName, <-stringsCh)
			}
		}
		// }(p)
	}
}

func printUsageAndAbort() {
	fmt.Println("Usage: gen_rdb [--nb-lists <nb> --nb-sets <nb> --nb-hashes <nb> --nb-strings <nb> --lists-prefix <prefix> --sets-prefix <prefix> --hashes-prefix <prefix> --strings-prefix <prefix>] -H <redis host>")
	os.Exit(1)
}

func main() {
	flag.Parse()

	if flHost == "" {
		printUsageAndAbort()
	}

	pool = &redis.Pool{
		MaxIdle:     32,
		IdleTimeout: 2 * time.Minute,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", flHost)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	wg.Add(1)
	go genLists()
	// go genSets()
	// go genHashes()
	// go genStrings()

	wg.Wait()

	log.Println(pool.Close())
}
