package main

import (
	"fmt"
	"math/rand"
	"projekat_nasp/config"
	"projekat_nasp/memTable"
	"projekat_nasp/sstable"
	"projekat_nasp/util"
	"projekat_nasp/wal"
	"time"
)

func Test_DZ3(numberKeys uint) {
	myWal := wal.NewWal()
	var memtable memTable.MemTablesManager
	switch config.GlobalConfig.StructureType {
	case "hashmap":
		memtable = memTable.InitMemTablesHash(config.GlobalConfig.MaxTables, uint64(config.GlobalConfig.MemtableSize))
	case "btree":
		memtable = memTable.InitMemTablesBTree(config.GlobalConfig.MaxTables, uint64(config.GlobalConfig.MemtableSize), uint8(config.GlobalConfig.BTreeOrder))
	case "skiplist":
		memtable = memTable.InitMemTablesSkipList(config.GlobalConfig.MaxTables, uint64(config.GlobalConfig.MemtableSize), config.GlobalConfig.SkipListHeight)
	}
	
	keyList := generateKeyList(int(numberKeys))
	for i:=0;i<100000;i++{
		key:=keyList[i%100]
		value:=util.RandomString(i%100,i)
		walEntry := myWal.Write(key, []byte(value), 0)
		entry := memTable.NewMemTableEntry(key, []byte(value), 0, walEntry.Timestamp)
		full, sizeToDelete := memtable.Add(entry)
		if full != nil {
			if config.GlobalConfig.SStableAllInOne == false {
				if config.GlobalConfig.SStableDegree != 0 {
					sstable.CreateSStable_13(full, 1, config.GlobalConfig.SStableDegree)
					} else {
						sstable.CreateSStable(full, 1)
					}
					} else {
							sstable.NewSSTable(&full, 1)
						}
						fmt.Println(sizeToDelete) // Ovde treba pozvati brisanje wal segmenata
					}
	}
}

func generateKeyList(numKeys int) []string {
	keys := make([]string, numKeys)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}
