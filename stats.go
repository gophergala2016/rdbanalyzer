package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type DatabaseStats struct {
	Count int
}

type StringStats struct {
	Count         int
	TotalByteSize int
}

type Stats struct {
	Database DatabaseStats
	Strings  StringStats
}

func generateStats() (s Stats) {
	s.Database.Count = nbDbs
	s.Strings.Count = nbStrings

	return
}

func writeStats(s *Stats, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("unable to create file '%s'. err=%v", filename, err)
	}

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("unable to marshal stats. err=%v", err)
	}

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("unable to write data to file. err=%v", err)
	}

	return nil
}
