package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/vrischmann/rdbtools"
)

var (
	flSVGOutput  string
	flListenAddr string

	dbCh                = make(chan int)
	stringObjectCh      = make(chan rdbtools.StringObject)
	listMetadataCh      = make(chan rdbtools.ListMetadata)
	listDataCh          = make(chan interface{})
	setMetadataCh       = make(chan rdbtools.SetMetadata)
	setDataCh           = make(chan interface{})
	hashMetadataCh      = make(chan rdbtools.HashMetadata)
	hashDataCh          = make(chan rdbtools.HashEntry)
	sortedSetMetadataCh = make(chan rdbtools.SortedSetMetadata)
	sortedSetEntriesCh  = make(chan rdbtools.SortedSetEntry)

	nbDbs        int
	nbStrings    int
	nbLists      int
	nbSets       int
	nbHashes     int
	nbSortedSets int

	wg sync.WaitGroup
)

func init() {
	flag.StringVar(&flSVGOutput, "o", "", "The SVG output file")
	flag.StringVar(&flListenAddr, "l", "", "The listen address of the web server")
}

func processDBs() {
	defer wg.Done()
	for range dbCh {
		nbDbs++
		log.Printf("databases: %d", nbDbs)
	}
}

func processStrings() {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case _, ok := <-stringObjectCh:
			if !ok {
				return
			}
			nbStrings++
		case <-ticker.C:
			log.Printf("strings: %d", nbStrings)
		}
	}
}

func processListMetadata() {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case _, ok := <-listMetadataCh:
			if !ok {
				return
			}
			nbLists++
		case <-ticker.C:
			log.Printf("lists: %d", nbLists)
		}
	}
}

func processListData() {
	defer wg.Done()
	for range listDataCh {
	}
}

func processSetMetadata() {
	defer wg.Done()
	for range setMetadataCh {
		nbSets++
	}
}

func processSetData() {
	defer wg.Done()
	for range setDataCh {
	}
}

func processHashMetadata() {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case _, ok := <-hashMetadataCh:
			if !ok {
				return
			}
			nbHashes++
		case <-ticker.C:
			log.Printf("hashes: %d", nbHashes)
		}
	}
}

func processHashData() {
	defer wg.Done()
	for range hashDataCh {
	}
}

func processSortedSetMetadata() {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case _, ok := <-sortedSetMetadataCh:
			if !ok {
				return
			}
			nbSortedSets++
		case <-ticker.C:
			log.Printf("sorted sets: %d", nbSortedSets)
		}
	}
}

func processSortedSetEntries() {
	defer wg.Done()
	for range sortedSetEntriesCh {
	}
}

func printUsageAndAbort() {
	fmt.Printf("Usage: rdbanalyzer (-o <output svg file>|-l <listen address>) <rdb file>\n\n")
	fmt.Println("There's two running modes:")
	fmt.Println(" - run and then output a SVG file on disk (with -o)")
	fmt.Println(" - run and then launch a web server which will serve a unique page with the SVG graph (with -l)")

	os.Exit(1)
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 || (flSVGOutput == "" && flListenAddr == "") {
		printUsageAndAbort()
	}

	file := flag.Arg(0)
	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("unable to open file '%s'. err=%v", file, err)
	}

	ctx := rdbtools.ParserContext{
		DbCh:                dbCh,
		StringObjectCh:      stringObjectCh,
		ListMetadataCh:      listMetadataCh,
		ListDataCh:          listDataCh,
		SetMetadataCh:       setMetadataCh,
		SetDataCh:           setDataCh,
		HashMetadataCh:      hashMetadataCh,
		HashDataCh:          hashDataCh,
		SortedSetMetadataCh: sortedSetMetadataCh,
		SortedSetEntriesCh:  sortedSetEntriesCh,
	}

	wg.Add(10)
	go processDBs()
	go processStrings()
	go processListMetadata()
	go processListData()
	go processSetMetadata()
	go processSetData()
	go processHashMetadata()
	go processHashData()
	go processSortedSetMetadata()
	go processSortedSetEntries()

	now := time.Now()

	parser := rdbtools.NewParser(ctx)
	if err := parser.Parse(f); err != nil {
		log.Fatalf("unable to parse RDB file. err=%v", err)
	}

	wg.Wait()

	fmt.Printf("Processing time: %s\n", time.Now().Sub(now))
}
