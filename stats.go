package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type DatabaseStats struct {
	Count int
}

type KeyStats struct {
	Count    int
	Expired  int
	Expiring int
}

func (s KeyStats) ExpiredProportion() float64 {
	return float64(s.Expired) / float64(s.Count) * 100
}

func (s KeyStats) ExpiringProportion() float64 {
	return float64(s.Expiring) / float64(s.Count) * 100
}

type StringStats struct {
	Count         int
	TotalByteSize int
}

type ListStats struct {
	Count int
}

type SetStats struct {
	Count int
}

type HashStats struct {
	Count int
}

type SortedSetStats struct {
	Count int
}

type Stats struct {
	// TODO(vincent): locking ?

	Database   DatabaseStats
	Keys       KeyStats
	Strings    StringStats
	Lists      ListStats
	Sets       SetStats
	Hashes     HashStats
	SortedSets SortedSetStats
}

func writeStats(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("unable to create file '%s'. err=%v", filename, err)
	}

	data, err := json.MarshalIndent(&stats, "", "  ")
	if err != nil {
		return fmt.Errorf("unable to marshal stats. err=%v", err)
	}

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("unable to write data to file. err=%v", err)
	}

	return nil
}
