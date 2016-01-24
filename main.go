package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ajstarks/svgo"
	"github.com/vrischmann/rdbtools"
)

var (
	flSVGOutput  string
	flListenAddr string

	flDebugStatsOutput string
	flDebugOnlyStats   bool
	flDebugRender      string

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

	flag.StringVar(&flDebugStatsOutput, "debug-stats-output", "", "DEBUG: the stats output file")
	flag.BoolVar(&flDebugOnlyStats, "debug-only-stats", false, "DEBUG: only generate statistics after parsing, without visualization")
	flag.StringVar(&flDebugRender, "debug-render", "", "DEBUG: only render the visualization of the stats from the provided file")
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

func generateSVGHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")

	var buf bytes.Buffer
	if err := generateSVG(&buf); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	io.Copy(w, &buf)
}

func generateSVG(w io.Writer) error {
	width := 1000
	height := 1000

	canvas := svg.New(w)
	canvas.Start(width, height)
	canvas.Circle(250, 250, 125, "fille:none,stroke:black")
	canvas.End()

	return nil
}

func renderStats(s *Stats) error {
	switch {
	case flSVGOutput != "":
		output, err := os.Create(flSVGOutput)
		if err != nil {
			return fmt.Errorf("unable to create SVG output file. err=%v", err)
		}

		fmt.Println("generating SVG file...")

		if err = generateSVG(output); err != nil {
			return fmt.Errorf("unable to generate SVG. err=%v", err)
		}
	case flListenAddr != "":
		http.HandleFunc("/", generateSVGHandler)
		if err := http.ListenAndServe(flListenAddr, nil); err != nil {
			return fmt.Errorf("unable to listen on %s. err=%v", flListenAddr, err)
		}
	}

	return nil
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
	case (requireSVG && flag.NArg() < 1) || (requireSVG && !hasSVG):
		printUsageAndAbort()
	}

	if err := parse(flag.Arg(0)); err != nil {
		log.Fatal(err)
	}

	stats := generateStats()
	if flDebugStatsOutput != "" {
		if err := writeStats(&stats, flDebugStatsOutput); err != nil {
			log.Fatalf("unable to write stats. err=%v", err)
		}
	}

	// Rendering
	if !flDebugOnlyStats {
		if err := renderStats(&stats); err != nil {
			log.Fatalf("unable to render stats. err=%v", err)
		}
	}
}
