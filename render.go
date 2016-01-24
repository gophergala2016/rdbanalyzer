package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"

	"github.com/ajstarks/svgo"
)

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

func toRadians(a float64) float64 {
	return math.Pi * a / 180
}

func xPosInCircle(theta float64) int {
	return int(180 * math.Cos(toRadians(theta)))
}

func yPosInCircle(theta float64) int {
	return int(180 * math.Sin(toRadians(theta)))
}

const (
	width  = 1200
	height = 900
	top    = 30
	left   = 30

	insideTextPadding = 10
	insidePiePadding  = 10

	globalStatsRectHeight  = 100
	globalStatsRowHeight   = 50
	globalStatsColumnWidth = (width - left*2 - insideTextPadding*2) / 4

	nbColumns = 3
	nbRows    = 2

	rowMargin     = 10
	columnSpacing = 30
	columnWidth   = (width - left*2 - columnSpacing*(nbColumns-1)) / nbColumns
	columnHeight  = (height - top*2 - globalStatsRectHeight - rowMargin*nbRows) / nbRows

	fontSize = 16
)

// TODO(vincent): figure out a way to put a margin. I suck at SVG.
func renderPiechart(canvas *svg.SVG, x, y int, percentages []float64) {
	var (
		startAngle = 0.0
		endAngle   = 0.0
	)

	colors := [...]string{
		"yellow", "red", "green",
	}

	for i, p := range percentages {
		startAngle = endAngle
		endAngle = startAngle + (p * 360 / 100)

		x1 := x + xPosInCircle(startAngle)
		y1 := y + yPosInCircle(startAngle)
		x2 := x + xPosInCircle(endAngle)
		y2 := y + yPosInCircle(endAngle)

		canvas.Path(fmt.Sprintf("M%d,%d L%d,%d A180,180 0 0,1 %d,%d z", x, y, x1, y1, x2, y2), fmt.Sprintf("fill:%s", colors[i]))
	}
}

func generateSVG(w io.Writer) error {
	canvas := svg.New(w)
	canvas.Start(width, height)
	canvas.Title("RDB statistics")
	canvas.Rect(0, 0, width, height, "fill:none;stroke:black;stroke-width:3") // global back rectangle

	// Global statistics
	//  - top row that spans all document
	//  - 100 high
	x := left
	y := top
	canvas.Rect(x, y, width-left*2, globalStatsRectHeight, "fill:black") // account for the margins

	canvas.Gstyle(fmt.Sprintf("font-family:Calibri,sans-serif;font-size:%dpt;fill:white", fontSize))

	// First row
	x = left + insideTextPadding
	y = top + insideTextPadding + fontSize
	canvas.Text(x, y, fmt.Sprintf("Databases: %d", stats.Database.Count))
	canvas.Text(x+globalStatsColumnWidth, y, fmt.Sprintf("Keys: %d", stats.Keys.Count))
	canvas.Text(x+globalStatsColumnWidth*2, y, fmt.Sprintf("Strings: %d", stats.Strings.Count))

	// Second row
	x = left + insideTextPadding
	y = top + insideTextPadding + fontSize + globalStatsRowHeight + insideTextPadding
	canvas.Text(x, y, fmt.Sprintf("Lists: %d", stats.Lists.Count))
	canvas.Text(x+globalStatsColumnWidth, y, fmt.Sprintf("Sets: %d", stats.Sets.Count))
	canvas.Text(x+globalStatsColumnWidth*2, y, fmt.Sprintf("Hashes: %d", stats.Hashes.Count))
	canvas.Text(x+globalStatsColumnWidth*3, y, fmt.Sprintf("Sorted Sets: %d", stats.SortedSets.Count))

	//
	// Details: first row
	//

	// First column - keys

	x = left
	y = top + globalStatsRectHeight + rowMargin

	canvas.Rect(x, y, columnWidth, columnHeight, "fill:black")

	expired := stats.Keys.ExpiredProportion()
	expiring := stats.Keys.ExpiringProportion()

	x = x + columnWidth/2
	y = y + columnHeight/2
	renderPiechart(canvas, x, y, []float64{
		expired, expiring, 100.0 - expired - expiring,
	})

	// Second column - strings

	x = left + columnWidth + columnSpacing
	y = top + globalStatsRectHeight + rowMargin
	canvas.Rect(x, y, columnWidth, columnHeight, "fill:green")

	// Third column - lists

	x = left + columnWidth*2 + columnSpacing*2
	y = top + globalStatsRectHeight + rowMargin
	canvas.Rect(x, y, columnWidth, columnHeight, "fill:blue")

	//
	// Details: second row
	//

	// First column - sets

	x = left
	y = top + globalStatsRectHeight + columnHeight + rowMargin*2

	canvas.Rect(x, y, columnWidth, columnHeight, "fill:red")

	// Second column - hashes

	x = left + columnWidth + columnSpacing
	canvas.Rect(x, y, columnWidth, columnHeight, "fill:green")

	// Third column - sorted sets

	x = left + columnWidth*2 + columnSpacing*2
	canvas.Rect(x, y, columnWidth, columnHeight, "fill:blue")

	canvas.Gend()
	canvas.End()

	return nil
}

func renderStats() error {
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
		// if err = generateSVG(os.Stdout); err != nil {
		// 	return fmt.Errorf("unable to generate SVG. err=%v", err)
		// }
	case flListenAddr != "":
		http.HandleFunc("/", generateSVGHandler)
		if err := http.ListenAndServe(flListenAddr, nil); err != nil {
			return fmt.Errorf("unable to listen on %s. err=%v", flListenAddr, err)
		}
	}

	return nil
}
