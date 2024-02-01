package memTable

import (
	"fmt"
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

// Manages instances of mem tables
type MemTablesManager struct {
	tables       []MemTable
	maxInstances int
	active       int
}

func InitMemTablesHash(maxInstances int, maxSize uint64) MemTablesManager {
	tables := make([]MemTable, maxInstances)
	for i := 0; i < maxInstances; i++ {
		tables[i] = InitHashMemTable(maxSize)
	}
	memTables := MemTablesManager{
		tables,
		maxInstances,
		0,
	}
	return memTables
}

func InitMemTablesBTree(maxInstances int, maxSize uint64, order uint8) MemTablesManager {
	tables := make([]MemTable, maxInstances)
	for i := 0; i < maxInstances; i++ {
		tables[i] = InitBTreeMemTable(maxSize, order)
	}
	memTables := MemTablesManager{
		tables,
		maxInstances,
		0,
	}
	return memTables
}

func InitMemTablesSkipList(maxInstances int, maxSize uint64, maxHeight int) MemTablesManager {
	tables := make([]MemTable, maxInstances)
	for i := 0; i < maxInstances; i++ {
		tables[i] = InitsSkipListMemTable(maxSize, maxHeight)
	}
	memTables := MemTablesManager{
		tables,
		maxInstances,
		0,
	}
	return memTables
}

// Adds entry to active memTable, if all are full returns them sorted as a sign to flush to SSTable
func (memTables *MemTablesManager) Add(entry MemTableEntry) []MemTableEntry {
	activeTable := memTables.tables[memTables.active]
	activeTable.Add(entry)
	if activeTable.IsFull() {
		if memTables.active == memTables.maxInstances-1 {
			sorted := memTables.Sort()
			memTables.Reset()
			return sorted
		} else {
			memTables.active += 1
		}
	}
	return nil
}

// Resets all memtables to empty them after sort
func (memTables *MemTablesManager) Reset() {
	for i := 0; i < memTables.maxInstances; i++ {
		memTables.tables[i].Reset()
	}
	memTables.active = 0
}

func (memTables *MemTablesManager) Delete(key string) {
	activeTable := memTables.tables[memTables.active]
	activeTable.Delete(key)
}

func (memTables *MemTablesManager) Find(key string) (bool, MemTableEntry) {
	for i := 0; i < memTables.maxInstances; i++ {
		activeTable := memTables.tables[i]
		found := activeTable.Find(key)
		if found.key != "" {
			return true, found
		}
	}
	return false, MemTableEntry{}
}

// Sorts content of all tables and merges them to the slice already sorted
func (memTables *MemTablesManager) Sort() []MemTableEntry {
	var sortedAll []MemTableEntry
	for i := 0; i < memTables.maxInstances; i++ {
		sorted := memTables.tables[i].Sort()
		sortedNew := make([]MemTableEntry, 0, len(sortedAll)+len(sorted))
		n, m := 0, 0
		for n < len(sortedAll) && m < len(sorted) {
			if sortedAll[n].key <= sorted[m].key {
				sortedNew = append(sortedNew, sortedAll[n])
				n++
			} else {
				sortedNew = append(sortedNew, sorted[m])
				m++
			}
		}
		for n < len(sortedAll) {
			sortedNew = append(sortedNew, sortedAll[n])
			n++
		}
		for m < len(sorted) {
			sortedNew = append(sortedNew, sorted[m])
			m++
		}
		sortedAll = sortedNew
	}
	return sortedAll
}
func (memTables *MemTablesManager) IsFull() bool {
	return false
}
func (memTables *MemTablesManager) Print() {
	for i := 0; i < memTables.maxInstances; i++ {
		fmt.Printf("Tabela %d : \n", i)
		memTables.tables[i].Print()
	}
}
