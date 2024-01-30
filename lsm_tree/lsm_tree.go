package lsmt

import (
	"fmt"
	"sort"
)

// SSTable predstavlja strukturu za čuvanje podataka u SSTables
type SSTable struct {
	level int
	data  map[string]string
}

// LSMTree predstavlja strukturu za čuvanje LSM stabla
type LSMTree struct {
	maxLevels int
	levels    map[int][]*SSTable
}

// CompactionAlgorithm je interfejs za algoritme kompaktiranja
type CompactionAlgorithm interface {
	compact(lsm *LSMTree, level int)
}

// SizeTieredCompaction je implementacija size-tiered algoritma za kompakciju
type SizeTieredCompaction struct{}

// Metoda za primenu size-tiered algoritma za kompakciju
func (stc SizeTieredCompaction) compact(lsm *LSMTree, level int) {
	// Sortira SSTables prema veličini podataka
	sort.Slice(lsm.levels[level], func(i, j int) bool {
		return len(lsm.levels[level][i].data) < len(lsm.levels[level][j].data)
	})

	// Spaja podatke iz svih SSTables u novu SSTable
	mergedData := make(map[string]string)
	for _, sstable := range lsm.levels[level] {
		for key, value := range sstable.data {
			mergedData[key] = value
		}
	}

	// Dodaje novu SSTable na sledeći nivo
	nextLevel := level + 1
	if _, exists := lsm.levels[nextLevel]; !exists {
		lsm.levels[nextLevel] = make([]*SSTable, 0)
	}
	newSSTable := &SSTable{level: nextLevel, data: mergedData}
	lsm.levels[nextLevel] = append(lsm.levels[nextLevel], newSSTable)

	// Briše stare SSTables sa trenutnog nivoa
	lsm.levels[level] = nil
}

// LeveledCompaction je implementacija leveled algoritma za kompakciju
type LeveledCompaction struct{}

// Metoda za primenu leveled algoritma za kompakciju
func (lc LeveledCompaction) compact(lsm *LSMTree, level int) {
	// Povećava nivo za 1
	nextLevel := level + 1

	// Proverava da li postoji nivo, ako ne, inicijalizuje ga
	if _, exists := lsm.levels[nextLevel]; !exists {
		lsm.levels[nextLevel] = make([]*SSTable, 0)
	}

	fmt.Println("Leveled Compaction - Nivo:", level)
	for _, sstable := range lsm.levels[level] {
		// Ispisuje podatke iz svake SSTable na trenutnom nivou
		fmt.Println("SSTable - Level:", sstable.level, "Data:", sstable.data)
	}
}

// Metoda za kompaktiranje nivoa koristeći odabrani algoritam
func (lsm *LSMTree) compactLevels(algorithm CompactionAlgorithm, level int) {
	// Proverava da li postoji nešto za kompaktiranje na trenutnom nivou
	if len(lsm.levels[level]) > 0 {
		// Pokreće odabrani algoritam za kompaktiranje
		algorithm.compact(lsm, level)
	}
}
/*
func main() {
	// Inicijalizuje LSM stablo sa maksimalnim brojem nivoa
	lsm := &LSMTree{
		maxLevels: 3,
		levels:    make(map[int][]*SSTable),
	}

	// Dodaje neke SSTables na početni nivo
	sstable1 := &SSTable{level: 0, data: map[string]string{"key1": "value1", "key2": "value2"}}
	sstable2 := &SSTable{level: 0, data: map[string]string{"key3": "value3", "key4": "value4"}}
	lsm.levels[0] = append(lsm.levels[0], sstable1, sstable2)

	// Odabira algoritam za kompaktiranje (SizeTiered ili Leveled)
	selectedAlgorithm := SizeTieredCompaction{} // Možete promeniti na LeveledCompaction{} ako želite leveled algoritam

	// Pokreće kompaktiranje nivoa sa odabranim algoritmom
	lsm.compactLevels(selectedAlgorithm, 0)

	// Ispisuje stanje nakon kompaktiranja
	fmt.Println("Nivo 0 nakon kompaktiranja:", lsm.levels[0])
	if lsm.levels[1] != nil {
		fmt.Println("Nivo 1 nakon kompaktiranja:", lsm.levels[1][0].data)
	}
}
*/