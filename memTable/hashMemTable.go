package memTable

import (
	"fmt"
	"sort"
)

type hashMemTable struct {
	maxSize     uint64
	currentSize uint64
	data        map[string]MemTableEntry
}

func (h *hashMemTable) IsFull() bool {
	return h.maxSize == h.currentSize
}

func InitHashMemTable(maxSize uint64) *hashMemTable {
	data := make(map[string]MemTableEntry, maxSize)
	table := hashMemTable{
		maxSize,
		0,
		data,
	}
	return &table
}

func (table *hashMemTable) Add(entry MemTableEntry) {
	key := entry.key
	_, exists := table.data[key]
	if !exists {
		table.currentSize += 1
	}
	table.data[key] = entry
}

func (table *hashMemTable) Delete(key string) {
	entry := table.data[key]
	entry.tombstone = true
	table.data[key] = entry
}

func (table *hashMemTable) Find(key string) MemTableEntry {
	entry := table.data[key]
	return entry
}

func (table *hashMemTable) Sort() []MemTableEntry {
	var values []MemTableEntry
	for _, value := range table.data {
		values = append(values, value)
	}

	memTableSlice := memTableEntrySlice(values)

	sort.Sort(memTableSlice)

	return memTableSlice
}

func (table *hashMemTable) Print() {
	fmt.Println(table)
}
