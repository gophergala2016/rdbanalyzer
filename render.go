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

	nbColumns = 2
	nbRows    = 1

	rowMargin     = 10
	columnSpacing = 30
	columnWidth   = (width - left*2 - columnSpacing*(nbColumns-1)) / nbColumns
	columnHeight  = (height - top*2 - globalStatsRectHeight - rowMargin*nbRows) / nbRows

	legendHeight       = 40
	legendPadding      = 5
	legendWidth        = columnWidth - insidePiePadding*2
	legendCircleRadius = (legendHeight - legendPadding*2) / 2
	legendColumnWidth  = (legendWidth - legendPadding*2) / 5

	titleHeight = 50

	fontSize = 16
)

var colors = [...]string{
	"FF0000", "00FF00", "0000FF", "FFFF00", "FF00FF", "00FFFF", "000000",
	"800000", "008000", "000080", "808000", "800080", "008080", "808080",
	"C00000", "00C000", "0000C0", "C0C000", "C000C0", "00C0C0", "C0C0C0",
	"400000", "004000", "000040", "404000", "400040", "004040", "404040",
	"200000", "002000", "000020", "202000", "200020", "002020", "202020",
	"600000", "006000", "000060", "606000", "600060", "006060", "606060",
	"A00000", "00A000", "0000A0", "A0A000", "A000A0", "00A0A0", "A0A0A0",
	"E00000", "00E000", "0000E0", "E0E000", "E000E0", "00E0E0", "E0E0E0",
}

func toRadians(a float64) float64 {
	return math.Pi * a / 180
}

func xPosInCircle(radius int, theta float64) int {
	return int(float64(radius) * math.Cos(toRadians(theta)))
}

func yPosInCircle(radius int, theta float64) int {
	return int(float64(radius) * math.Sin(toRadians(theta)))
}

type pieSlice struct {
	name  string
	value float64
	color string
}

func renderPiechart(canvas *svg.SVG, title string, x, y int, slices []pieSlice) {
	x1 := x + (columnWidth-insidePiePadding*2-60)/2 // 60 is a guesstimate of the width of the bounding box for the title. Don't know how to get it right now.
	y1 := y + insidePiePadding + titleHeight
	canvas.Text(x1, y1, title, "fill:white")

	x = x + columnWidth/2
	y = y + (columnHeight-legendHeight)/2

	radius := (columnWidth - legendHeight - insidePiePadding*3) / 2

	var (
		startAngle = 0.0
		endAngle   = 0.0
	)

	textTooltips := make([]struct {
		x     int
		y     int
		value float64
	}, len(slices))

	for i, p := range slices {
		if p.value <= 0.0 {
			continue
		}

		style := fmt.Sprintf("fill:#%s", p.color)

		if p.value >= 100.0 {
			canvas.Circle(x, y, radius, style)
		}

		startAngle = endAngle
		endAngle = startAngle + (p.value * 360 / 100)

		x1 := x + xPosInCircle(radius, startAngle)
		y1 := y + yPosInCircle(radius, startAngle)
		x2 := x + xPosInCircle(radius, endAngle)
		y2 := y + yPosInCircle(radius, endAngle)

		canvas.Path(fmt.Sprintf("M%d,%d L%d,%d A%d,%d 0 0,1 %d,%d z", x, y, x1, y1, radius, radius, x2, y2), style)

		textTooltips[i].x = x1
		textTooltips[i].y = y1
		textTooltips[i].value = p.value
	}

	for _, t := range textTooltips {
		canvas.Text(t.x, t.y, fmt.Sprintf("%0.2f%%", t.value), "fill:white;font-size:20pt;stroke:black;stroke-width:1px")
	}
}

func renderPiechartLegend(canvas *svg.SVG, x, y int, slices []pieSlice) {
	canvas.Gstyle("font-size:10pt;fill:black")
	canvas.Rect(x, y, legendWidth, legendHeight, "fill:white")

	y1 := y + legendCircleRadius + legendPadding
	for i, p := range slices {
		x1 := x + legendCircleRadius + legendPadding + (i * legendColumnWidth)

		canvas.Circle(x1, y1, legendCircleRadius, fmt.Sprintf("fill:#%s", p.color))
		canvas.Text(x1+legendCircleRadius+legendPadding, y1, p.name)
	}

	canvas.Gend()
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
	pie := []pieSlice{
		{"expired", expired, colors[0]},
		{"expiring", expiring, colors[1]},
		{"normal", 100.0 - expired - expiring, colors[2]},
	}

	renderPiechart(canvas, "keys status", x, y, pie)

	// Legend

	x = left + insidePiePadding
	y = top + globalStatsRectHeight + rowMargin + columnHeight - legendHeight - insidePiePadding
	renderPiechartLegend(canvas, x, y, pie)

	// Second column - space usage

	x = left + columnWidth + columnSpacing
	y = top + globalStatsRectHeight + rowMargin
	canvas.Rect(x, y, columnWidth, columnHeight, "fill:black")

	sup := stats.SpaceUsage()
	pie = []pieSlice{
		{"strings", sup.Strings, colors[0]},
		{"lists", sup.Lists, colors[1]},
		{"sets", sup.Sets, colors[2]},
		{"hashes", sup.Hashes, colors[3]},
		{"zsets", sup.SortedSets, colors[4]},
	}

	renderPiechart(canvas, "space usage", x, y, pie)

	// Legend

	x = left + columnWidth + columnSpacing + insidePiePadding
	y = top + globalStatsRectHeight + rowMargin + columnHeight - legendHeight - insidePiePadding
	renderPiechartLegend(canvas, x, y, pie)

	// Third column - lists

	x = left + columnWidth*2 + columnSpacing*2
	y = top + globalStatsRectHeight + rowMargin
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
	case flListenAddr != "":
		http.HandleFunc("/", generateSVGHandler)
		if err := http.ListenAndServe(flListenAddr, nil); err != nil {
			return fmt.Errorf("unable to listen on %s. err=%v", flListenAddr, err)
		}
	}

	return nil
}
