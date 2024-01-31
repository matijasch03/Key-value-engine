package main

import (
	"encoding/json"
	"fmt"
	"sort"
)

// SSTable predstavlja strukturu za čuvanje podataka u SSTables
type SSTable struct {
	GeneralFilename string
	SSTableFilename string
	IndexFilename   string
	SummaryFilename string
	FilterFilename  string
	Level           int
	Data            map[string]string
}

// LSMTree predstavlja strukturu za čuvanje LSM stabla
type LSMTree struct {
	MaxLevels int
	Levels    map[int][]*SSTable
}

// CompactionAlgorithm je interfejs za algoritme kompaktiranja
type CompactionAlgorithm interface {
	Compact(lsm *LSMTree, level int)
}

// SizeTieredCompaction je implementacija size-tiered algoritma za kompakciju
type SizeTieredCompaction struct{}

// Metoda za primenu size-tiered algoritma za kompakciju
func (stc SizeTieredCompaction) Compact(lsm *LSMTree, level int) {
	// Sortira SSTables prema veličini podataka
	sort.Slice(lsm.Levels[level], func(i, j int) bool {
		return len(lsm.Levels[level][i].Data) < len(lsm.Levels[level][j].Data)
	})

	// Spaja podatke iz svih SSTables u novu SSTable
	mergedData := make(map[string]string)
	for _, sstable := range lsm.Levels[level] {
		for key, value := range sstable.Data {
			mergedData[key] = value
		}
	}

	// Dodaje novu SSTable na sledeći nivo
	nextLevel := level + 1
	if _, exists := lsm.Levels[nextLevel]; !exists {
		lsm.Levels[nextLevel] = make([]*SSTable, 0)
	}
	newSSTable := &SSTable{
		Level:           nextLevel,
		Data:            mergedData,
		GeneralFilename: fmt.Sprintf("general_%d", nextLevel),
		SSTableFilename: fmt.Sprintf("sstable_%d", nextLevel),
		IndexFilename:   fmt.Sprintf("index_%d", nextLevel),
		SummaryFilename: fmt.Sprintf("summary_%d", nextLevel),
		FilterFilename:  fmt.Sprintf("filter_%d", nextLevel),
	}
	lsm.Levels[nextLevel] = append(lsm.Levels[nextLevel], newSSTable)

	// Briše stare SSTables sa trenutnog nivoa
	lsm.Levels[level] = nil
}

// LeveledCompaction je implementacija leveled algoritma za kompakciju
type LeveledCompaction struct{}

// Metoda za primenu leveled algoritma za kompakciju
func (lc LeveledCompaction) Compact(lsm *LSMTree, level int) {
	// Povećava nivo za 1
	nextLevel := level + 1

	// Proverava da li postoji nivo, ako ne, inicijalizuje ga
	if _, exists := lsm.Levels[nextLevel]; !exists {
		lsm.Levels[nextLevel] = make([]*SSTable, 0)
	}

	fmt.Println("Leveled Compaction - Nivo:", level)
	for _, sstable := range lsm.Levels[level] {
		// Ispisuje podatke iz svake SSTable na trenutnom nivou
		fmt.Printf("SSTable - Level: %d, Data: %v, General: %s, SSTable: %s, Index: %s, Summary: %s, Filter: %s\n",
			sstable.Level, sstable.Data, sstable.GeneralFilename, sstable.SSTableFilename,
			sstable.IndexFilename, sstable.SummaryFilename, sstable.FilterFilename)
	}
}

// Metoda za kompaktiranje nivoa koristeći odabrani algoritam
func (lsm *LSMTree) CompactLevels(algorithm CompactionAlgorithm, level int) {
	// Proverava da li postoji nešto za kompaktiranje na trenutnom nivou
	if len(lsm.Levels[level]) > 0 {
		// Pokreće odabrani algoritam za kompaktiranje
		algorithm.Compact(lsm, level)
	}
}

// Metoda za serijalizaciju LSMTree strukture
func (lsm *LSMTree) Serialize() ([]byte, error) {
	jsonData, err := json.Marshal(lsm)
	if err != nil {
		return nil, fmt.Errorf("Greška prilikom serijalizacije LSMTree: %v", err)
	}

	return jsonData, nil
}

// Metoda za deserijalizaciju LSMTree strukture
func (lsm *LSMTree) Deserialize(data []byte) error {
	err := json.Unmarshal(data, lsm)
	if err != nil {
		return fmt.Errorf("Greška prilikom deserijalizacije LSMTree: %v", err)
	}

	return nil
}
/* Testing 
func main() {
	// Inicijalizuje LSM stablo sa maksimalnim brojem nivoa
	lsm := &LSMTree{
		MaxLevels: 3,
		Levels:    make(map[int][]*SSTable),
	}

	// Dodaje neke SSTables na početni nivo
	sstable1 := &SSTable{
		Level:           0,
		Data:            map[string]string{"key1": "value1", "key2": "value2"},
		GeneralFilename: "general_file_0",
		SSTableFilename: "sstable_file_0",
		IndexFilename:   "index_file_0",
		SummaryFilename: "summary_file_0",
		FilterFilename:  "filter_file_0",
	}
	sstable2 := &SSTable{
		Level:           0,
		Data:            map[string]string{"key3": "value3", "key4": "value4"},
			GeneralFilename: "general_file_1",
			SSTableFilename: "sstable_file_1",
			IndexFilename:   "index_file_1",
			SummaryFilename: "summary_file_1",
			FilterFilename:  "filter_file_1",
	}
	lsm.Levels[0] = append(lsm.Levels[0], sstable1, sstable2)

	// Odabira algoritam za kompaktiranje (SizeTiered ili Leveled)
	selectedAlgorithm := SizeTieredCompaction{} // Možete promeniti na LeveledCompaction{} ako želite leveled algoritam

	// Pokreće kompaktiranje nivoa sa odabranim algoritmom
	lsm.CompactLevels(selectedAlgorithm, 0)

	// Ispisuje stanje nakon kompaktiranja
	fmt.Println("Nivo 0 nakon kompaktiranja:")
	for _, sstable := range lsm.Levels[0] {
		fmt.Printf("SSTable - Level: %d, Data: %v, General: %s, SSTable: %s, Index: %s, Summary: %s, Filter: %s\n",
			sstable.Level, sstable.Data, sstable.GeneralFilename, sstable.SSTableFilename,
			sstable.IndexFilename, sstable.SummaryFilename, sstable.FilterFilename)
	}
	if lsm.Levels[1] != nil {
		fmt.Println("Nivo 1 nakon kompaktiranja:")
		for _, sstable := range lsm.Levels[1] {
			fmt.Printf("SSTable - Level: %d, Data: %v, General: %s, SSTable: %s, Index: %s, Summary: %s, Filter: %s\n",
			sstable.Level, sstable.Data, sstable.GeneralFilename, sstable.SSTableFilename,
			sstable.IndexFilename, sstable.SummaryFilename, sstable.FilterFilename)
		}
	}
}*/
/* Primer korišćenja serijalizacije i deserijalizacije
func main() {
	// Kreiranje instance LSMTree
	lsm := &LSMTree{
		MaxLevels: 3,
		Levels:    make(map[int][]*SSTable),
	}

	// Serijalizacija LSMTree
	serializedData, err := lsm.Serialize()
	if err != nil {
		fmt.Println("Greška prilikom serijalizacije:", err)
		return
	}

	// Deserijalizacija LSMTree
	deserializedLSM := &LSMTree{}
	err = deserializedLSM.Deserialize(serializedData)
	if err != nil {
		fmt.Println("Greška prilikom deserijalizacije:", err)
		return
	}
}
*/