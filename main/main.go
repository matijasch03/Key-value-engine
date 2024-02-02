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
	myWal, memtable, tokenBucket, cache, bloom_filter, hll, cms, simhash := Start()
	fmt.Println(bloom_filter, simhash) //Dodato da ne bi ispisivao gresku da nisu korisceni
	broj := 1
	for {
		fmt.Println("1. GET")
		fmt.Println("2. PUT")
		fmt.Println("3. DELETE")
		fmt.Println("4. Aproximate frequency of key")
		fmt.Println("5. Aproximate cardinality")
		fmt.Println("6. Exit")

		fmt.Print("Enter your choice: ")

		var choice int
		fmt.Scan(&choice)

		if tokenBucket.CheckRequest() {
			switch choice {
			case 1: // GET
				fmt.Print("Enter key: ")
				var key string
				fmt.Scan(&key)
				key = strings.TrimRight(key, "\n")

				foundMemTable, valueMemTable := memtable.Find(key)

				if foundMemTable {
					fmt.Println(valueMemTable)
				} else {
					foundCache, valueCache := cache.GetByKey(key)
					if foundCache {
						fmt.Println(valueCache)
					} else {
						/*
							pretraziti sstable, i onda dodati u cache
						*/
					}
				}

			case 2: // PUT
				fmt.Print("Enter key: ")
				var key string
				fmt.Scan(&key)

				fmt.Print("Enter value: ")
				var value string
				fmt.Scan(&value)

				walEntry := myWal.Write(key, []byte(value), 0)
				entry := memTable.NewMemTableEntry(key, []byte(value), 0, walEntry.Timestamp)
				full := memtable.Add(entry)
				if full != nil {
					sstable.CreateSStable(full,1)
					broj = broj + 1
				}
				cache.AddItem(key, value)
				hll.Add(key)
				cms.AddKey(key)
			case 3: // DELETE
				fmt.Print("Enter key: ")
				var key string
				fmt.Scan(&key)

				myWal.Delete(key, 1)
				memtable.Delete(key)
				cache.DeleteByKey(key)

			case 4: // EXIT
				fmt.Print("Enter key: ")
				var key string
				fmt.Scan(&key)
				cardinality := cms.FindKeyFrequency(key)
				fmt.Printf("Estimated cardinality of key %s: %d \n", key, cardinality)
			case 5:
				cardinality := hll.Prebroj()
				fmt.Printf("Estimated cardinality: %f \n", cardinality)
			case 6: //EXIT
				fmt.Println("Exiting...")
				myWal.Dump()
				data := memtable.Sort()
				sstable.CreateSStable(data, 1)
				//bloom_filter.SaveToFile("data/bloom_filter/bf.gob")
				hll.SacuvajHLL("./data/hyperloglog/hll.gob")
				countMinSketch.WriteGob("./data/count_min_sketch/cms.gob", cms)
				//simhash.SerializeSH()
				os.Exit(0)
			default:
				fmt.Println("Invalid choice. Please enter a valid option.")
				//memtable.Print()
			}
		} else {
			fmt.Println("You have reached request limit. Please wait a bit.")
		}
	}
}

func Start() (*wal.Wal, memTable.MemTablesManager, *token_bucket.TokenBucket, *cache.Cache, *bloom_filter.BloomFilter, hyperloglog.HLL, *countMinSketch.CountMinSketch, *simhash.SimHash) {

	//ucitati config file

	myWal := wal.NewWal()
	memtable := memTable.InitMemTablesHash(3, 1000)
	tokenBucket := token_bucket.NewTokenBucket(1, 5)
	cache := cache.NewCache(10)

	///treba srediti imena fajlova i dodati za errore
	myWal.Recovery(memtable)
	bloom_filter, _ := bloom_filter.LoadFromFile("data/bloom_filter/bf.gob")
	hll := hyperloglog.UcitajHLL("./data/hyperloglog/hll.gob")
	var cms = new(countMinSketch.CountMinSketch)
	_ = countMinSketch.ReadGob("./data/count_min_sketch/cms.gob", cms)
	simhash, _ := simhash.DeserializeSH([]byte("???"))

	return myWal, memtable, tokenBucket, cache, bloom_filter, hll, cms, simhash
}
