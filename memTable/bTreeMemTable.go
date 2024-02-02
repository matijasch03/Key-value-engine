package memTable

import "fmt"

type bTreeMemTable struct {
	maxSize     uint64
	currentSize uint64
	data        bTree
}

func (h *bTreeMemTable) IsFull() bool {
	return h.maxSize == h.currentSize
}

func InitBTreeMemTable(maxSize uint64, order uint8) *bTreeMemTable {
	data := InitBTree(order)
	table := bTreeMemTable{
		maxSize,
		0,
		data,
	}
	return &table
}

func (table *bTreeMemTable) Reset() {
	table.data = InitBTree(table.data.order)
	table.currentSize = 0
}

func (table *bTreeMemTable) Add(entry MemTableEntry) {
	added := table.data.Insert(entry)
	if added {
		table.currentSize += 1
	}
}

func (table *bTreeMemTable) Delete(key string) {
	entry := table.data.Find(key)
	if entry != nil {
		entry.tombstone = 1
	}
}

func (table *bTreeMemTable) Find(key string) MemTableEntry {
	entry := table.data.Find(key)
	if entry == nil {
		return MemTableEntry{}
	}
	return *entry
}

func (table *bTreeMemTable) Sort() []MemTableEntry {
	return table.data.SortTree()
}

func (table *bTreeMemTable) Print() {
	fmt.Println(table.currentSize, table.maxSize)
	table.data.PrintTree()
}
