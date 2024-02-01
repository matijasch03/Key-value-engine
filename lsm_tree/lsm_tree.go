package lsm_tree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
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

// Metoda za serijalizaciju LSMTree strukture pomoću Goba
func (lsm *LSMTree) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(lsm)
	if err != nil {
		return nil, fmt.Errorf("Greška prilikom GobEncode: %v", err)
	}

	return buf.Bytes(), nil
}

// Metoda za deserijalizaciju LSMTree strukture pomoću Goba
func (lsm *LSMTree) Deserialize(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)

	err := decoder.Decode(lsm)
	if err != nil {
		return fmt.Errorf("Greška prilikom GobDecode: %v", err)
	}

	return nil
}

// Metoda za čuvanje LSMTree strukture u datoteku
func (lsm *LSMTree) SaveToFile(filename string) error {
	data, err := lsm.Serialize()
	if err != nil {
		return err
	}

	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		return fmt.Errorf("Greška prilikom pisanja u datoteku: %v", err)
	}

	return nil
}

// Metoda za učitavanje LSMTree strukture iz datoteke
func (lsm *LSMTree) LoadFromFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("Greška prilikom čitanja iz datoteke: %v", err)
	}

	err = lsm.Deserialize(data)
	if err != nil {
		return err
	}

	return nil
}

// Funkcija za dodavanje podataka u LSMTree
func (lsm *LSMTree) AddData(level int, key, value string) {
	// Proverava da li postoji nivo, ako ne, inicijalizuje ga
	if _, exists := lsm.Levels[level]; !exists {
		lsm.Levels[level] = make([]*SSTable, 0)
	}

	// Proverava da li postoji SSTable za dati nivo, ako ne, inicijalizuje ga
	if len(lsm.Levels[level]) == 0 || lsm.Levels[level][len(lsm.Levels[level])-1].Level != level {
		sstable := &SSTable{
			Level: level,
			Data:  make(map[string]string),
			// Dodajte ostale informacije za SSTable...
		}
		lsm.Levels[level] = append(lsm.Levels[level], sstable)
	}

	// Dodaje podatke u mapu Data unutar SSTable
	sstable := lsm.Levels[level][len(lsm.Levels[level])-1]
	sstable.Data[key] = value
}

/* Primer korišćenja:
func main() {
	lsm := &LSMTree{
		MaxLevels: 3,
		Levels:    make(map[int][]*SSTable),
	}

	// Čuvanje u datoteku
	err := lsm.SaveToFile("lsm_tree.gob")
	if err != nil {
		fmt.Println("Greška prilikom čuvanja u datoteku:", err)
		return
	}

	// Učitavanje iz datoteke
	lsm2 := &LSMTree{}
	err = lsm2.LoadFromFile("lsm_tree.gob")
	if err != nil {
		fmt.Println("Greška prilikom učitavanja iz datoteke:", err)
		return
	}

	// Ispisivanje učitanih podataka
	fmt.Println("Učitani LSMTree:", lsm2)
}*/
