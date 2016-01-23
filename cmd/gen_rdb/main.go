// +build ignore
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type hashFields []string

func (f hashFields) String() string {
	return fmt.Sprintf("%s", []string(f))
}

func (f *hashFields) Set(s string) error {
	parts := strings.Split(s, ",")
	for _, p := range parts {
		*f = append(*f, p)
	}

	return nil
}

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

	flHashFields hashFields

	pool *redis.Pool
	wg   sync.WaitGroup
)

func init() {
	flag.StringVar(&flHost, "H", "", "The redis hostname")

	flag.IntVar(&flNbLists, "nb-lists", 0, "The number of lists to generate")
	flag.IntVar(&flNbSets, "nb-sets", 0, "The number of sets to generate")
	flag.IntVar(&flNbHashes, "nb-hashes", 0, "The number of hashes to generate")
	flag.IntVar(&flNbStrings, "nb-strings", 0, "The number of strings to generate")

	flag.StringVar(&flListsPrefix, "lists-prefix", "", "The prefix for each list")
	flag.StringVar(&flSetsPrefix, "sets-prefix", "", "The prefix for each set")
	flag.StringVar(&flHashesPrefix, "hashes-prefix", "", "The prefix for each hash")
	flag.StringVar(&flStringsPrefix, "strings-prefix", "", "The prefix for each string")

	flag.Var(&flHashFields, "hash-fields", "List of fields to put in every hash")
}

const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

var (
	bufPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

func randomString(rng *rand.Rand, n int64) string {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	buf.Reset()

	for i := int64(0); i < n; i++ {
		buf.WriteByte(alphabet[rng.Intn(len(alphabet))])
	}

	return buf.String()
}

func genRandomInts(min, max int64) <-chan int64 {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	ch := make(chan int64, 100000)
	go func() {
		for {
			n := rng.Int63n(max) + min
			ch <- n
		}
	}()
	return ch
}

func genRandomStrings() <-chan string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	ch := make(chan string, 100000)
	go func() {
		intsCh := genRandomInts(20, 600)
		for {
			n := <-intsCh
			s := randomString(rng, n)

			ch <- s
		}
	}()
	return ch
}

type queueOpFunc func(cmd string, args ...interface{})

type opFunc func(qu queueOpFunc, i int, stringsCh <-chan string, intsCh <-chan int64, smallIntsCh <-chan int64)

type part struct {
	min int
	max int
}

func generate(p part, op opFunc) {
	conn := pool.Get()
	defer func() {
		conn.Close()
		wg.Done()
	}()

	intsCh := genRandomInts(5000, 12000)
	smallIntsCh := genRandomInts(2, 10)
	stringsCh := genRandomStrings()

	time.Sleep(5 * time.Second)

	var nbQueued int
	queueOp := func(cmd string, args ...interface{}) {
		if nbQueued >= 16 {
			conn.Flush()
			nbQueued = 0
		}
		conn.Send(cmd, args...)
		nbQueued++
	}

	for i := p.min; i <= p.max; i++ {
		op(queueOp, i, stringsCh, intsCh, smallIntsCh)
	}

	conn.Flush()
}

const (
	maxNbListsWorkers   = 20
	maxNbSetsWorkers    = 20
	maxNbHashesWorkers  = 20
	maxNbStringsWorkers = 20
)

func partition(nb, maxNbWorkers int) (parts []part) {
	var nbWorkers int
	if nb > maxNbWorkers {
		nbWorkers = maxNbWorkers
	} else {
		nbWorkers = 1
	}

	nbPerPart := nb / nbWorkers

	for w := 0; w < nbWorkers; w++ {
		parts = append(parts, part{
			min: w * nbPerPart,
			max: (w+1)*nbPerPart - 1,
		})
	}
	if remainder := nb % nbWorkers; remainder != 0 {
		parts = append(parts, part{
			min: nbWorkers * nbPerPart,
			max: nbWorkers*nbPerPart + remainder - 1,
		})
	}

	return
}

func genLists() {
	if flNbLists == 0 {
		return
	}

	parts := partition(flNbLists, maxNbListsWorkers)
	wg.Add(len(parts))

	for _, p := range parts {
		go generate(p, func(qu queueOpFunc, i int, stringsCh <-chan string, intsCh, smallIntsCh <-chan int64) {
			keyName := fmt.Sprintf("%slist%d", flListsPrefix, i)
			n := <-intsCh
			for i := int64(0); i < n; i++ {
				qu("LPUSH", keyName, <-stringsCh)
			}
		})
	}
}

func genSets() {
	if flNbSets == 0 {
		return
	}

	parts := partition(flNbSets, maxNbSetsWorkers)
	wg.Add(len(parts))

	for _, p := range parts {
		go generate(p, func(qu queueOpFunc, i int, stringsCh <-chan string, intsCh, smallIntsCh <-chan int64) {
			keyName := fmt.Sprintf("%sset%d", flSetsPrefix, i)
			n := <-intsCh
			for i := int64(0); i < n; i++ {
				qu("SADD", keyName, <-stringsCh)
			}
		})
	}
}

func genHashes() {
	if flNbHashes == 0 {
		return
	}

	parts := partition(flNbHashes, maxNbHashesWorkers)
	wg.Add(len(parts))

	for _, p := range parts {
		go generate(p, func(qu queueOpFunc, i int, stringsCh <-chan string, intsCh, smallIntsCh <-chan int64) {
			keyName := fmt.Sprintf("%shash%d", flHashesPrefix, i)

			var args []interface{}
			if len(flHashFields) > 0 {
				args = make([]interface{}, len(flHashFields)*2+1)
				for i, f := range flHashFields {
					args[i*2+1] = f
					args[i*2+2] = <-stringsCh
				}
			} else {
				nbFields := int(<-smallIntsCh)

				args = make([]interface{}, nbFields*2+1)
				for j := 1; j <= nbFields*2; j += 2 {
					args[j] = <-stringsCh
					args[j+1] = <-stringsCh
				}
			}

			args[0] = keyName

			qu("HMSET", args...)
		})
	}
}

func genStrings() {
	if flNbStrings == 0 {
		return
	}

	parts := partition(flNbStrings, maxNbStringsWorkers)
	wg.Add(len(parts))

	for _, p := range parts {
		go generate(p, func(qu queueOpFunc, i int, stringsCh <-chan string, intsCh, smallIntsCh <-chan int64) {
			keyName := fmt.Sprintf("%sstring%d", flStringsPrefix, i)
			qu("SET", keyName, <-stringsCh)
		})
	}
}

func printUsageAndAbort() {
	fmt.Println("Usage: gen_rdb [--nb-lists <nb> --nb-sets <nb> --nb-hashes <nb> --nb-strings <nb> --lists-prefix <prefix> --sets-prefix <prefix> --hashes-prefix <prefix> --strings-prefix <prefix>] -H <redis host>")
	os.Exit(1)
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

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

	log.Println("starting lists generation")
	genLists()
	wg.Wait()
	log.Println("done generating lists")

	log.Println("starting sets generation")
	genSets()
	wg.Wait()
	log.Println("done generating sets")

	log.Println("starting hashes generation")
	genHashes()
	wg.Wait()
	log.Println("done generating hashes")

	log.Println("starting strings generation")
	genStrings()
	wg.Wait()
	log.Println("done generating strings")

	// TODO(vincent): generate sorted sets

	log.Println(pool.Close())
}
