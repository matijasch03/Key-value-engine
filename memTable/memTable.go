package memTable

import (
	"encoding/binary"
	"fmt"
	"os"
	"projekat_nasp/config"
	"strings"
	"time"
	"unicode/utf8"
)

/*
Interface for the memTable type.
Btree, skipList and hashMap memtables all implement these methods
*/
type MemTable interface {
	IsFull() bool
	Add(entry MemTableEntry)
	Find(key string) MemTableEntry
	Delete(key string)
	Sort() []MemTableEntry
	Reset()
	Print()
}

type MemTableEntry struct {
	key       string
	value     []byte
	tombstone byte
	timestamp uint64
}

func (entry *MemTableEntry) GetKey() string {
	return entry.key
}
func (entry *MemTableEntry) GetValue() []byte {
	return entry.value
}
func (entry *MemTableEntry) GetTimeStamp() uint64 {
	return entry.timestamp
}
func (entry *MemTableEntry) GetTombstone() byte {
	return entry.tombstone
}

// Added for Sort()
type memTableEntrySlice []MemTableEntry

func (s memTableEntrySlice) Len() int           { return len(s) }
func (s memTableEntrySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s memTableEntrySlice) Less(i, j int) bool { return s[i].key < s[j].key }

func NewMemTableEntry(key string, value []byte, tombstone byte, timestamp uint64) MemTableEntry {
	entry := MemTableEntry{
		key,
		value,
		tombstone,
		timestamp,
	}
	return entry
}
func FillWithParametersEntry(key string, value []byte, timestamp uint64, tombstone byte) MemTableEntry {
	entry := MemTableEntry{
		key,
		value,
		tombstone,
		timestamp,
	}
	return entry
}

// Manages instances of mem tables
type MemTablesManager struct {
	tables       []MemTable
	walSize      []int
	maxInstances int
	active       int
}

func InitMemTablesHash(maxInstances int, maxSize uint64) MemTablesManager {
	if maxInstances <= 0 {
		maxInstances = config.MAX_TABLES
	}
	if maxSize <= 0 {
		maxSize = config.MEMTABLE_SIZE
	}
	tables := make([]MemTable, maxInstances)
	walSize := make([]int, maxInstances)
	for i := 0; i < maxInstances; i++ {
		tables[i] = InitHashMemTable(maxSize)
	}
	memTables := MemTablesManager{
		tables,
		walSize,
		maxInstances,
		0,
	}
	return memTables
}

func InitMemTablesBTree(maxInstances int, maxSize uint64, order uint8) MemTablesManager {
	if maxInstances <= 0 {
		maxInstances = config.MAX_TABLES
	}
	if maxSize <= 0 {
		maxSize = config.MEMTABLE_SIZE
	}
	if order <= 1 {
		order = config.B_TREE_ORDER
	}
	tables := make([]MemTable, maxInstances)
	walSize := make([]int, maxInstances)

	for i := 0; i < maxInstances; i++ {
		tables[i] = InitBTreeMemTable(maxSize, order)
	}
	memTables := MemTablesManager{
		tables,
		walSize,
		maxInstances,
		0,
	}
	return memTables
}

func InitMemTablesSkipList(maxInstances int, maxSize uint64, maxHeight int) MemTablesManager {
	if maxInstances <= 0 {
		maxInstances = config.MAX_TABLES
	}
	if maxSize <= 0 {
		maxSize = config.MEMTABLE_SIZE
	}
	if maxHeight <= 1 {
		maxHeight = config.SKIP_LIST_HEIGHT
	}
	tables := make([]MemTable, maxInstances)
	walSize := make([]int, maxInstances)

	for i := 0; i < maxInstances; i++ {
		tables[i] = InitsSkipListMemTable(maxSize, maxHeight)
	}
	memTables := MemTablesManager{
		tables,
		walSize,
		maxInstances,
		0,
	}
	return memTables
}

// Adds entry to active memTable, if all are full returns them sorted as a sign to flush to SSTable
func (memTables *MemTablesManager) Add(entry MemTableEntry) ([]MemTableEntry, int) {
	activeTable := memTables.tables[memTables.active]
	activeTable.Add(entry)
	memTables.walSize[memTables.active] += 29 + len([]byte(entry.GetKey())) + len(entry.GetValue())
	fmt.Println(memTables.walSize[memTables.active])
	if activeTable.IsFull() {
		nextTable := (memTables.active + 1) % memTables.maxInstances
		if memTables.tables[nextTable].IsFull() {
			sorted := memTables.tables[nextTable].Sort()
			memTables.tables[nextTable].Reset()
			memTables.active = nextTable
			fmt.Println(sorted)
			toDelete := memTables.walSize[memTables.active]
			memTables.walSize[memTables.active] = 0
			return sorted, toDelete
		}
		memTables.active = nextTable
	}
	return nil, 0
}

// Resets all memtables to empty them after sort
func (memTables *MemTablesManager) Reset() {
	for i := 0; i < memTables.maxInstances; i++ {
		memTables.tables[i].Reset()
	}
	memTables.active = 0
}

func (memTables *MemTablesManager) Delete(key string) {
	memTables.Add(NewMemTableEntry(key, nil, 1, uint64(time.Now().Unix())))
}

func (memTables *MemTablesManager) Find(key string) (bool, MemTableEntry) {
	activeTable := memTables.tables[memTables.active]
	found := activeTable.Find(key)
	if found.key != "" {
		return true, found
	}
	for i := memTables.active - 1; i != memTables.active; i-- {
		if i == -1 {
			i = memTables.maxInstances - 1
		}
		activeTable := memTables.tables[i]
		found := activeTable.Find(key)
		if found.key != "" {
			return true, found
		}
		if i == memTables.active {
			break
		}
	}
	return false, MemTableEntry{}
}

// Sorts content of all tables and merges them to the slice already sorted
func (memTables *MemTablesManager) Sort() [][]MemTableEntry {
	var sortedAll [][]MemTableEntry

	for i := 0; i < memTables.maxInstances; i++ {
		sorted := memTables.tables[i].Sort()
		sortedAll = append(sortedAll, sorted)
	}

	return sortedAll
}

func (memTables *MemTablesManager) IsFull() bool {
	return false
}
func (memTables *MemTablesManager) Print() {
	fmt.Printf("Active table: %d \n", memTables.active)
	for i := 0; i < memTables.maxInstances; i++ {
		fmt.Printf("Tabela %d : \n", i)
		memTables.tables[i].Print()
	}
}

func BytesToRecord(f *os.File) (MemTableEntry, int64, error) {
	// Struktura: KS(8), VS(8), TIME(8), TB(1), K(...), V(...)
	buffer := make([]byte, 8)
	tombstoneBuffer := make([]byte, 1)
	// Key size
	_, err := f.Read(buffer)
	if err != nil {
		return MemTableEntry{}, 0, err
	}
	keySize := binary.LittleEndian.Uint64(buffer)

	// Value size
	_, err = f.Read(buffer)
	if err != nil {
		return MemTableEntry{}, 0, err
	}
	valueSize := binary.LittleEndian.Uint64(buffer)

	// Timestamp
	_, err = f.Read(buffer)
	if err != nil {
		return MemTableEntry{}, 0, err
	}
	timestamp := binary.LittleEndian.Uint64(buffer)

	// Tombstone
	_, err = f.Read(tombstoneBuffer)
	if err != nil {
		return MemTableEntry{}, 0, err
	}
	tombstone := tombstoneBuffer[0]

	// Key
	keyBuffer := make([]byte, keySize)
	_, err = f.Read(keyBuffer)
	if err != nil {
		return MemTableEntry{}, 0, err
	}
	key := string(keyBuffer)

	// Value
	value := make([]byte, valueSize)
	_, err = f.Read(value)
	if err != nil {
		return MemTableEntry{}, 0, err
	}

	readBytes := 25 + len(key) + len(value) // 25 je fiksna duzina prvih 4 polja

	return FillWithParametersEntry(key, value, timestamp, tombstone), int64(readBytes), nil
}

// Prints only part of entries whose keys contain defined prefix.
// This is like a book: each page has the same number of keys, and only keys from one page will be printed.
func (memTables *MemTablesManager) PrefixScan(prefix string, pageNumber int, pageSize int) {
	fmt.Print("Content of page ", pageNumber, ": ")

	dirPath := "data/sstable"

	dir, err := os.Open(dirPath)
	if err != nil {
		fmt.Println("Error opening directory:", err)
		return
	}
	defer dir.Close()

	fileInfos, err := dir.Readdir(-1) //-1 is sign to read all filenames from the directory
	if err != nil {
		fmt.Println("Error reading directory contents:", err)
		return
	}

	const MAX_SSTABLES = 3000
	offsetArray := make([]int64, MAX_SSTABLES) // array of offsets for each file
	var filePositionArray [MAX_SSTABLES]int64

	for j := 0; j < MAX_SSTABLES; j++ {
		offsetArray[j] = 32 // start offsets for each file
		filePositionArray[j] = 0
	}

	//memOffsetArray := make([]int64, config.MAX_TABLES)
	//for j := 0; j < config.MAX_TABLES; j++ {
	// memOffsetArray[j] = 0 // start offsets for memTable arrays
	//}

	processedEntries := 0 // total number of processed entries from all tables
	keyOrdinalNum := 1
	startKey := (pageNumber-1)*pageSize + 1
	endKey := pageNumber * pageSize

	for {
		maxCodePoint := utf8.MaxRune
		minKey := string(maxCodePoint) // string, from whom each one will be lower
		var minRecord MemTableEntry
		i := 0               // ordinal number of current SSTable
		var minIndices []int // ordinal numbers of table where is minKey - this file should be seeked
		var minLens []int64  // extra offsets in files with minKey (equals to length of this entry)

		// SSTABLES SCANNER (from files - disk)
		for _, fileInfo := range fileInfos {
			// each file starting with "file_" contains data from one SSTable

			if strings.HasPrefix(fileInfo.Name(), "file_") {
				file, err := os.OpenFile("data/sstable/"+fileInfo.Name(), os.O_RDONLY, 0600)
				if err != nil {
					panic(err)
				}

				for { // loop for finding the first entry in the file with defined prefix (skip other)
					if filePositionArray[i] >= config.MEMTABLE_SIZE {
						break
					} // the end of the file

					file.Seek(offsetArray[i], 0)
					record, length, err := BytesToRecord(file)
					if err != nil {
						panic(err)
					}

					if strings.HasPrefix(record.GetKey(), prefix) {
						// 2 cases when key is lower:
						// - key is lower
						// - keys are same, but current key is newer (lower timestamp)
						if record.GetKey() < minKey {
							minKey = record.GetKey()
							minRecord = record

							// deleting old min indices and appending new one
							minIndices = minIndices[:0]
							minLens = minLens[:0]
							minIndices = append(minIndices, i)
							minLens = append(minLens, length)

						} else if record.GetKey() == minKey {
							minIndices = append(minIndices, i)
							minLens = append(minLens, length)

							if record.GetTimeStamp() < minRecord.GetTimeStamp() {
								minKey = record.GetKey()
								minRecord = record
							}
						}
						break

					} else { // skip all entries that haven't defined prefix
						offsetArray[i] += length
						processedEntries++
						filePositionArray[i]++
					}
				}
				i++
			}
		}

		if processedEntries >= config.MEMTABLE_SIZE*i || i >= MAX_SSTABLES {
			break // all files are read - break the outer loop
		}

		if keyOrdinalNum >= startKey && keyOrdinalNum <= endKey {
			fmt.Print(minKey, " ")
		}
		keyOrdinalNum++

		for j := 0; j < len(minLens); j++ {
			offsetArray[minIndices[j]] += minLens[j]
			filePositionArray[minIndices[j]]++
			processedEntries++
		}
	}
	fmt.Println()
}
