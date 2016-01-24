package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/vrischmann/rdbtools"
)

var (
	flSVGOutput  string
	flListenAddr string

	flDebugStatsOutput string
	flDebugOnlyStats   bool
	flDebugRender      string

	keysCh = make(chan rdbtools.KeyObject)

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

	stats Stats

	wg sync.WaitGroup
)

func init() {
	flag.StringVar(&flSVGOutput, "o", "", "The SVG output file")
	flag.StringVar(&flListenAddr, "l", "", "The listen address of the web server")

	flag.StringVar(&flDebugStatsOutput, "debug-stats-output", "", "DEBUG: the stats output file")
	flag.BoolVar(&flDebugOnlyStats, "debug-only-stats", false, "DEBUG: only generate statistics after parsing, without visualization")
	flag.StringVar(&flDebugRender, "debug-render", "", "DEBUG: only render the visualization of the stats from the provided file")
}

func processDBs() {
	defer wg.Done()
	for range dbCh {
		stats.Database.Count++
	}
}

func processKeys() {
	defer wg.Done()
	for key := range keysCh {
		stats.Keys.Count++
		now := time.Now()

		switch {
		case key.ExpiryTime.IsZero():
			break
		case key.ExpiryTime.After(now):
			stats.Keys.Expiring++
		case key.ExpiryTime.Before(now):
			stats.Keys.Expired++
		}
	}
}

func processStrings() {
	defer wg.Done()
	for obj := range stringObjectCh {
		keysCh <- obj.Key

		stats.Strings.Count++

		// TODO(vincent): this will fail if it's the wrong type, fix it !
		stringLength := len(obj.Value.([]uint8))
		stats.Strings.TotalByteSize += stringLength
	}
}

func processListMetadata() {
	defer wg.Done()
	for obj := range listMetadataCh {
		keysCh <- obj.Key

		stats.Lists.Count++
	}
}

func processListData() {
	defer wg.Done()
	for range listDataCh {
	}
}

func processSetMetadata() {
	defer wg.Done()
	for obj := range setMetadataCh {
		keysCh <- obj.Key
		stats.Sets.Count++
	}
}

func processSetData() {
	defer wg.Done()
	for range setDataCh {
	}
}

func processHashMetadata() {
	defer wg.Done()
	for obj := range hashMetadataCh {
		keysCh <- obj.Key
		stats.Hashes.Count++
	}
}

func processHashData() {
	defer wg.Done()
	for range hashDataCh {
	}
}

func processSortedSetMetadata() {
	defer wg.Done()
	for obj := range sortedSetMetadataCh {
		keysCh <- obj.Key
		stats.SortedSets.Count++
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

func parse(filename string) error {
	file := flag.Arg(0)
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("unable to open file '%s'. err=%v", file, err)
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
	go processKeys()
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

	// Parsing

	fmt.Printf("parsing RDB file %s\n", file)

	parser := rdbtools.NewParser(ctx)
	if err := parser.Parse(f); err != nil {
		return fmt.Errorf("unable to parse RDB file. err=%v", err)
	}

	wg.Wait()

	fmt.Printf("parsing time: %s\n", time.Now().Sub(now))

	return nil
}

func main() {
	flag.Parse()

	requireSVG := !flDebugOnlyStats && flDebugStatsOutput == ""
	hasSVG := flSVGOutput != "" || flListenAddr != ""

	switch {
	case flDebugRender != "":
		if flSVGOutput == "" {
			fmt.Println("With --debug-regen-svg you need to also pass the -o or -l option")
			os.Exit(1)
		}

		data, err := ioutil.ReadFile(flDebugRender)
		if err != nil {
			log.Fatalf("unable to read stats file '%s'. err=%v", flDebugRender, err)
		}

		if err := json.Unmarshal(data, &stats); err != nil {
			log.Fatalf("unable to unmarshal stats. err=%v", err)
		}

		if err := renderStats(); err != nil {
			log.Fatalf("unable to render stats. err=%v", err)
		}

	case (requireSVG && flag.NArg() < 1) || (requireSVG && !hasSVG):
		printUsageAndAbort()
	}

	if err := parse(flag.Arg(0)); err != nil {
		log.Fatal(err)
	}

	if flDebugStatsOutput != "" {
		if err := writeStats(flDebugStatsOutput); err != nil {
			log.Fatalf("unable to write stats. err=%v", err)
		}
	}

	// Rendering
	if !flDebugOnlyStats {
		if err := renderStats(); err != nil {
			log.Fatalf("unable to render stats. err=%v", err)
		}
	}
}
