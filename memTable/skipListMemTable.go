package memTable

type skipListMemTable struct {
	maxSize     uint64
	currentSize uint64
	data        SkipList
}

func (h *skipListMemTable) IsFull() bool {
	return h.maxSize == h.currentSize
}

func InitsSkipListMemTable(maxSize uint64, maxHeight int) *skipListMemTable {
	data := NewSkipList(maxHeight)
	table := skipListMemTable{
		maxSize,
		0,
		*data,
	}
	return &table
}

func (table *skipListMemTable) Reset() {
	table.data = *NewSkipList(table.data.maxHeight)
	table.currentSize = 0
	return
}

func (table *skipListMemTable) Add(entry MemTableEntry) {
	added := table.data.InsertElement(entry.key, entry)
	if added {
		table.currentSize += 1

	}

}

func (table *skipListMemTable) Delete(key string) {
	entry, _ := table.data.SearchElement(key)
	entry.tombstone = 1
}

func (table *skipListMemTable) Find(key string) MemTableEntry {
	entry, _ := table.data.SearchElement(key)
	return *entry
}

func (table *skipListMemTable) Sort() []MemTableEntry {
	return table.data.Sort()
}

func (table *skipListMemTable) Print() {
	table.data.Display()
}
