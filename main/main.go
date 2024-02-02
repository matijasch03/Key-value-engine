package main

import (
	"fmt"
	"os"
	"projekat_nasp/bloom_filter"
	"projekat_nasp/cache"
	"projekat_nasp/countMinSketch"
	"projekat_nasp/hyperloglog"
	"projekat_nasp/memTable"
	"projekat_nasp/simhash"
	"projekat_nasp/sstable"
	"projekat_nasp/token_bucket"
	"projekat_nasp/wal"
	"strings"
)

func main() {
	/*var broj int
	broj = 1
	myWal := wal.NewWal()
	memtable := memTable.InitMemTablesBTree(2, 1000, 3)
	tokenBucket := token_bucket.NewTokenBucket(1, 5000)
	for i := 0; i < 5000; i++ {
		it := strconv.FormatInt(int64(i), 10)
		key := "key" + it
		if tokenBucket.CheckRequest() {
			myWal.Write(key, []byte("value1"), 0)
			entry := memTable.NewMemTableEntry(key, []byte("value1"), 0, 12)
			full := memtable.Add(entry)
			if full != nil {
				sstable.NewSSTable(&full, 3)
				broj = broj + 1
			}
		} else {
			fmt.Println(key)
		}
	}

	myWal.Dump()
	myWal.DeleteSegments()*/
	keys := []string{"key2017"}
	path := "data/sstable/file_1706892349872607900_3.db"
	full := true
	files, _ := sstable.GetTables()
	fmt.Println(files)
	result := sstable.FindByKey(keys, path, full)
	fmt.Println("Rezultat pretrage po ključu:", result)
}
