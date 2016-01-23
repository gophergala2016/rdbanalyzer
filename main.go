package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

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

	wg sync.WaitGroup
)

func init() {
	flag.StringVar(&flSVGOutput, "o", "", "The SVG output file")
	flag.StringVar(&flListenAddr, "l", "", "The listen address of the web server")
}

func processDBs() {
	for db := range dbCh {
		log.Printf("db: %d", db)
	}
}

func processStrings() {
	for str := range stringObjectCh {
		log.Printf("str: %+v", str)
	}
}

func processListMetadata() {
	for md := range listMetadataCh {
		log.Printf("md: %+v", md)
	}
}

func processListData() {
	for d := range listDataCh {
		log.Printf("list data %+v", d)
	}
}

func processSetMetadata() {
	for md := range setMetadataCh {
		log.Printf("%+v", md)
	}
}

func processSetData() {
	for d := range setDataCh {
		log.Printf("set data %+v", d)
	}
}

func processHashMetadata() {
	for md := range hashMetadataCh {
		log.Printf("%+v", md)
	}
}

func processHashData() {
	for d := range hashDataCh {
		log.Printf("hash data %+v", d)
	}
}

func processSortedSetMetadata() {
	for md := range sortedSetMetadataCh {
		log.Printf("%+v", md)
	}
}

func processSortedSetEntries() {
	for d := range sortedSetEntriesCh {
		log.Printf("sorted set entry: %+v", d)
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

	parser := rdbtools.NewParser(ctx)
	if err := parser.Parse(f); err != nil {
		log.Fatalf("unable to parse RDB file. err=%v", err)
	}

	wg.Wait()
}
