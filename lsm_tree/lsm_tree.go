package lsm_tree

import (
	"encoding/binary"
	"fmt"
	"os"
	"projekat_nasp/memTable"
	"projekat_nasp/sstable"
	//"projekat_nasp/config"
	"sort"
	"strings"

)

const (
	SSTABLE_SIZE = 1500
)

type ByKey []memTable.MemTableEntry

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Less(i, j int) bool {
	return a[i].GetKey() < a[j].GetKey()
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type Levels struct {
	Levels   []*Level
	MaxLevel int
}

type Level struct {
	Level  int
	Tables []*os.File
	Size   int
}

func NewLevel(currentLvl, size int) *Level {
	var files []*os.File
	return &Level{currentLvl + 1, files, size * 2}
}

func (lvl *Level) AddToLevel(path string, levels *Levels) {
	var allRecords []memTable.MemTableEntry
	f, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}

	newRecords := GetRecordsOutOfSS(f)
	allRecords = append(allRecords, newRecords...)

	for _, table := range lvl.Tables {
		f, err := os.OpenFile(table.Name(), os.O_RDONLY, 0600)
		if err != nil {
			panic(err)
		}

		oldRecs := GetRecordsOutOfSS(f)
		for _, rec := range oldRecs {
			exists := false
			for _, record := range newRecords {
				if rec.GetKey() == record.GetKey() {
					exists = true
					break
				}
			}
			if !exists {
				allRecords = append(allRecords, rec)
			}
		}
		f.Close()
		err = os.Remove(table.Name())
		if err != nil {
			panic(err)
		}
		metaFile := strings.Replace(table.Name(), "file", "MetaData", 1)
		metaFile = metaFile[0 : len(metaFile)-5]
		metaFile = metaFile + ".txt"

		os.Remove(metaFile)
	}
	f.Close()
	err = os.Remove(path)
	if err != nil {
		panic(err)
	}
	metaFile := strings.Replace(path, "file", "MetaData", 1)
	metaFile = metaFile[0 : len(metaFile)-5]
	metaFile = metaFile + ".txt"

	os.Remove(metaFile)
	//println(err)

	lvl.Tables = []*os.File{}

	//Sortirani svi recordi po kljucu
	sort.Sort(ByKey(allRecords))
	current_size := 0
	level_size := 0
	var helper_list []memTable.MemTableEntry
	for _, record := range allRecords {
		if current_size > SSTABLE_SIZE {
			sstable.CreateSStable(helper_list, lvl.Level)

			newTable, _ := sstable.GetTables()
			newFileName := newTable[0]
			newFileName = "data/sstable" + newFileName

			newFile, err := os.OpenFile(newFileName, os.O_RDONLY, 0600)
			if err != nil {
				panic(err)
			}

			lvl.Tables = append(lvl.Tables, newFile)
			helper_list = helper_list[:0]
			helper_list = append(helper_list, record)
			level_size += current_size
			current_size = 0
			newFile.Close()

		} else {
			helper_list = append(helper_list, record)
			current_size += 25 + len(record.GetKey()) + len(record.GetValue())

		}
	}
	if len(helper_list) != 0 {
		sstable.CreateSStable(helper_list, lvl.Level)

		newTable, _ := sstable.GetTables()
		newFileName := newTable[0]
		newFileName = "../data/sstable" + newFileName

		newFile, err := os.OpenFile(newFileName, os.O_RDONLY, 0600)
		if err != nil {
			panic(err)
		}

		lvl.Tables = append(lvl.Tables, newFile)
		helper_list = helper_list[:0]
		level_size += current_size
		current_size = 0
		newFile.Close()

	}

	for {
		if lvl.Size < level_size && lvl.Level < levels.MaxLevel {
			nextLevel := &Level{}
			//TODO proveriti da li sme da pravi vise levela ili neka samo puni tu
			for i := 0; i < len(levels.Levels); i++ {
				if levels.Levels[i].Level == lvl.Level+1 {
					nextLevel = levels.Levels[i]
				}
			}
			if len(nextLevel.Tables) == 0 {
				nextLevel = NewLevel(lvl.Level, lvl.Size)
				levels.Levels = append(levels.Levels, nextLevel)
			}
			//dodati da vise tabela moze ici u sledeci nivo
			f, _ := os.OpenFile(lvl.Tables[0].Name(), os.O_RDONLY, 0600)
			f.Seek(0, 0)
			buffer := make([]byte, 8)
			_, err := f.Read(buffer)

			if err != nil {
				fmt.Println("Error while reading header")
			}
			f.Close()
			endOfRecords := binary.LittleEndian.Uint64(buffer)

			level_size -= (int(endOfRecords) - 32)

			nextLevel.AddToLevel(lvl.Tables[0].Name(), levels)
			// f.Close()
			// err = os.Remove(lvl.Tables[0].Name())
			// if err != nil {
			// 	fmt.Println("Error while reading header")
			// }
			// metaFile := strings.Replace(lvl.Tables[0].Name(), "file", "MetaData", 1)
			// metaFile = metaFile[0 : len(metaFile)-5]
			// metaFile = metaFile + ".txt"
			// os.Remove(metaFile)
			lvl.Tables = lvl.Tables[1:]
			continue

		} else {
			break
		}
	}

	//prebacivanje u drugi nivo odnosno proveravam da li postoji level vec, ako ne pravim ga i onda radim
	//ako postoji onda mu pristupam i prolazim kroz add to level v

}

func LeveledCompaction(maxLevels int) {
	var lev []*Level
	levels := Levels{lev, maxLevels}
	tables, _ := sstable.GetTables()
	//reverse the order from oldest to youngest
	for i, j := 0, len(tables)-1; i < j; i, j = i+1, j-1 {
		tables[i], tables[j] = tables[j], tables[i]
	}
	level1 := NewLevel(0, SSTABLE_SIZE)
	levels.Levels = append(levels.Levels, level1)
	for _, table := range tables {
		level1.AddToLevel("../data/sstable"+table, &levels)
	}
}

func GetRecordsOutOfSS(f *os.File) []memTable.MemTableEntry {
	var allRecords []memTable.MemTableEntry
	f.Seek(0, 0)
	buffer := make([]byte, 8)
	_, err := f.Read(buffer)

	if err != nil {
		fmt.Println("Error while reading header")
	}

	endOfRecords := binary.LittleEndian.Uint64(buffer)

	f.Seek(32, 0)
	for {
		pos, _ := f.Seek(0, os.SEEK_CUR)
		if uint64(pos) < endOfRecords {

			record, _, err := bytesToRecord(f)
			if err != nil {
				panic(err)
			}

			allRecords = append(allRecords, record)

			// _, err = f.Read(buffer)

			// if err != nil {
			// 	fmt.Println("Error while reading key size")
			// }
			// keySize := binary.LittleEndian.Uint64(buffer)

			// // Value size
			// _, err = f.Read(buffer)
			// if err != nil {
			// 	fmt.Println("Error while reading value size")
			// }
			// valueSize := binary.LittleEndian.Uint64(buffer)
		} else {
			return allRecords
		}
	}

}

//treba dodati da vise tabela ide u next level