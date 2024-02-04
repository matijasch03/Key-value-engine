package main

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"projekat_nasp/bloom_filter"
	"projekat_nasp/cache"
	"projekat_nasp/config"
	"projekat_nasp/countMinSketch"
	"projekat_nasp/hyperloglog"
	"projekat_nasp/lsm_tree"
	"projekat_nasp/memTable"
	"projekat_nasp/simhash"
	"projekat_nasp/sstable"
	"projekat_nasp/token_bucket"
	"projekat_nasp/util"
	"projekat_nasp/wal"
	"strconv"
	"strings"
	"time"
)

func main() {

	myWal, memtable, tokenBucket, cache, bloom_filter, hll, cms, simhash := Start()
	fmt.Println(bloom_filter, simhash) //Dodato da ne bi ispisivao gresku da nisu korisceni
	for {
		fmt.Println("1. GET")
		fmt.Println("2. PUT")
		fmt.Println("3. DELETE")
		fmt.Println("4. Aproximate frequency of key")
		fmt.Println("5. Aproximate cardinality")
		fmt.Println("6. Compaction")
		fmt.Println("7. Exit")
		fmt.Println("8. With Compaction")
		fmt.Println("9. Without compaction")

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
					fmt.Println("Nasao u memtable-u: ", valueMemTable)
					cache.AddItem(valueMemTable.GetKey(), string(valueMemTable.GetValue()))
				} else {
					foundCache, valueCache := cache.GetByKey(key)
					if foundCache {
						fmt.Println("Nasao u cache-u: ", valueCache)
					} else {
						entry := sstable.Main_search([]string{key})
						if len(entry) == 0 {
							fmt.Println("Neuspesna pretraga")
						} else {
							for i := 0; i < len(entry); i++ {
								fmt.Println("Nasao u sstable-u: ", entry[i])
								cache.AddItem(entry[i].GetKey(), string(entry[i].GetValue()))
							}
						}
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
					//fmt.Println(sizeToDelete) // Ovde treba pozvati brisanje wal segmenata
					myWal.DeleteBytesFromFiles(sizeToDelete)
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
			case 6: //COMPACT
				if config.GlobalConfig.CompactionAlgorithm == "sizeTiered" {
					err := lsm_tree.SizeTiered()
					if err != nil {
						fmt.Println(err)
					}
				} else {
					lsm_tree.LeveledCompaction()
				}
			case 7: //EXIT
				fmt.Println("Exiting...")
				//data := memtable.Sort()
				//sstable.CreateSStable(data, 1)
				//bloom_filter.SaveToFile("data/bloom_filter/bf.gob")
				hll.SacuvajHLL("./data/hyperloglog/hll.gob")
				countMinSketch.WriteGob("./data/count_min_sketch/cms.gob", cms)
				//simhash.SerializeSH()
				os.Exit(0)
			case 8:
				Test_DZ3_compression(100)
			case 9:
				Test_DZ3_without_compression(100)
			default:
				fmt.Println("Invalid choice. Please enter a valid option.")
				memtable.Print()
			}
		} else {
			fmt.Println("You have reached request limit. Please wait a bit.")
		}
	}
}

func Start() (*wal.Wal, memTable.MemTablesManager, *token_bucket.TokenBucket, *cache.Cache, *bloom_filter.BloomFilter, hyperloglog.HLL, *countMinSketch.CountMinSketch, *simhash.SimHash) {

	config.Init()

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
	tokenBucket := token_bucket.NewTokenBucket(1, 5)
	cache := cache.NewCache(10)

	///treba srediti imena fajlova i dodati za errore
	myWal.Recovery(&memtable)
	bloom_filter, _ := bloom_filter.LoadFromFile("data/bloom_filter/bf.gob")
	hll := hyperloglog.UcitajHLL("./data/hyperloglog/hll.gob")
	var cms = new(countMinSketch.CountMinSketch)
	_ = countMinSketch.ReadGob("./data/count_min_sketch/cms.gob", cms)
	simhash, _ := simhash.DeserializeSH([]byte("???"))

	return myWal, memtable, tokenBucket, cache, bloom_filter, hll, cms, simhash
}

func asciiToText(asciiValues []int) string {
	var result string

	for _, asciiValue := range asciiValues {
		character := fmt.Sprintf("%c", asciiValue)
		result += character
	}

	return result
}

func Test_DZ3_compression(numberKeys uint) {
	newCompressor := NewCompressor()
	newCompressor.LoadFromFile()
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
	for i := 0; i < 100000; i++ {
		key := keyList[i%100]
		keyList := newCompressor.Compress([]string{key})
		//globalna kompresija
		if len(keyList) == 0 {
			fmt.Println("empty dict")
		} else {
			key = strconv.Itoa(keyList[0])
		}
		value := util.RandomString(i%100, i)
		walEntry := myWal.Write(key, []byte(value), 0)
		entry := memTable.NewMemTableEntry(key, []byte(value), 0, walEntry.Timestamp)
		full, _ := memtable.Add(entry)
		if full != nil {
			sstable.NewSSTable_DZ3(&full, 1)
		}
	}
}
func Test_DZ3_without_compression(numberKeys uint) {
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
	for i := 0; i < 100000; i++ {
		key := keyList[i%100]
		value := util.RandomString(i%100, i)
		walEntry := myWal.Write(key, []byte(value), 0)
		entry := memTable.NewMemTableEntry(key, []byte(value), 0, walEntry.Timestamp)
		full, _ := memtable.Add(entry)
		if full != nil {
			sstable.NewSSTable_DZ3(&full, 1)
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

type Compressor struct {
	Dictionary map[string]int
}

// NewCompressor kreira novi objekat Compressor
func NewCompressor() *Compressor {
	return &Compressor{
		Dictionary: make(map[string]int),
	}
}

// Compress kompresuje niz ključeva koristeći dictionary encoding
func (c *Compressor) Compress(keys []string) []int {
	var result []int

	for _, key := range keys {
		// Proveravamo da li ključ već postoji u rečniku
		if _, ok := c.Dictionary[key]; !ok {
			// Ako ne postoji, dodajemo ga u rečnik sa sledećim slobodnim brojem
			c.Dictionary[key] = len(c.Dictionary)
		}

		// Dodajemo numeričku vrednost ključa u rezultat
		result = append(result, c.Dictionary[key])
	}

	return result
}

// SaveToFile čuva kompresovanu mapu u fajl
func (c *Compressor) SaveToFile() error {
	filename := "data/globalCompressed.gob"
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(c.Dictionary)
	if err != nil {
		return err
	}

	return nil
}

// LoadFromFile učitava kompresovanu mapu iz fajla
func (c *Compressor) LoadFromFile() error {
	filename := "data/globalCompressed.gob"
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&c.Dictionary)
	if err != nil {
		return err
	}

	return nil
}
