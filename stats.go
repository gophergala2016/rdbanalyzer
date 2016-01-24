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
	Count         int
	TotalByteSize int
}

type SetStats struct {
	Count         int
	TotalByteSize int
}

type HashStats struct {
	Count         int
	TotalByteSize int
}

type SortedSetStats struct {
	Count         int
	TotalByteSize int
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

func (s Stats) SpaceUsage() SpaceUsageProportions {
	total := float64(s.Strings.TotalByteSize + s.Lists.TotalByteSize + s.Sets.TotalByteSize + s.Hashes.TotalByteSize + s.SortedSets.TotalByteSize)

	return SpaceUsageProportions{
		Strings:    float64(s.Strings.TotalByteSize) / total,
		Lists:      float64(s.Lists.TotalByteSize) / total,
		Sets:       float64(s.Sets.TotalByteSize) / total,
		Hashes:     float64(s.Hashes.TotalByteSize) / total,
		SortedSets: float64(s.SortedSets.TotalByteSize) / total,
	}
}

type SpaceUsageProportions struct {
	Strings    float64
	Lists      float64
	Sets       float64
	Hashes     float64
	SortedSets float64
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
