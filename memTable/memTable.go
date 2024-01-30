package memTable

import "fmt"

type memTable interface {
	IsFull() bool
	Add(entry MemTableEntry)
	Find(key string) MemTableEntry
	Delete(key string)
	Sort() []MemTableEntry
	Print()
}

type MemTableEntry struct {
	key       string
	value     []byte
	tombstone bool
	timestamp uint64
}

type memTableEntrySlice []MemTableEntry

func (s memTableEntrySlice) Len() int           { return len(s) }
func (s memTableEntrySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s memTableEntrySlice) Less(i, j int) bool { return s[i].key < s[j].key }

func NewMemTableEntry(key string) MemTableEntry {
	entry := MemTableEntry{
		key,
		make([]byte, 0),
		false,
		123,
	}
	return entry
}

type memTablesManager struct {
	tables       []memTable
	maxInstances int
	active       int
}

func InitMemTablesHash(maxInstances int, maxSize uint64) memTablesManager {
	tables := make([]memTable, maxInstances)
	for i := 0; i < maxInstances; i++ {
		tables[i] = InitHashMemTable(maxSize)
	}
	memTables := memTablesManager{
		tables,
		maxInstances,
		0,
	}
	return memTables
}

func InitMemTablesBTree(maxInstances int, maxSize uint64, order uint8) memTablesManager {
	tables := make([]memTable, maxInstances)
	for i := 0; i < maxInstances; i++ {
		tables[i] = InitBTreeMemTable(maxSize, order)
	}
	memTables := memTablesManager{
		tables,
		maxInstances,
		0,
	}
	return memTables
}

func InitMemTablesSkipList(maxInstances int, maxSize uint64, maxHeight int) memTablesManager {
	tables := make([]memTable, maxInstances)
	for i := 0; i < maxInstances; i++ {
		tables[i] = InitsSkipListMemTable(maxSize, maxHeight)
	}
	memTables := memTablesManager{
		tables,
		maxInstances,
		0,
	}
	return memTables
}

func (memTables *memTablesManager) Add(entry MemTableEntry) {
	activeTable := memTables.tables[memTables.active]
	activeTable.Add(entry)
	if activeTable.IsFull() {
		if memTables.active == memTables.maxInstances-1 {
			memTables.Flush()
			memTables.active = 0
		} else {
			memTables.active += 1
		}
	}
}

func (memTables *memTablesManager) Delete(key string) {
	activeTable := memTables.tables[memTables.active]
	activeTable.Delete(key)
}

func (memTables *memTablesManager) Find(key string) MemTableEntry {
	activeTable := memTables.tables[memTables.active]
	found := activeTable.Find(key)
	return found
}

func (memTables *memTablesManager) Sort() []MemTableEntry {
	var sortedAll []MemTableEntry
	for i := 0; i < memTables.maxInstances; i++ {
		sorted := memTables.tables[i].Sort()
		fmt.Printf("Tabela %d : \n", i)
		fmt.Println(sorted)
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

func (memTables *memTablesManager) Flush() {
	fmt.Println("pusti pusti vodu")
	return
}

func (memTables *memTablesManager) Print() {
	for i := 0; i < memTables.maxInstances; i++ {
		fmt.Printf("Tabela %d : \n", i)
		memTables.tables[i].Print()
	}
}

/*
	entry := memTable.NewMemTableEntry("abc")
	entry2 := memTable.NewMemTableEntry("bbc")
	entry3 := memTable.NewMemTableEntry("cbc")
	entry4 := memTable.NewMemTableEntry("dbc")
	entry5 := memTable.NewMemTableEntry("aac")
	entry6 := memTable.NewMemTableEntry("aaa")
	entry7 := memTable.NewMemTableEntry("bac")
	entry8 := memTable.NewMemTableEntry("bag")
	entry9 := memTable.NewMemTableEntry("ggg")
	entry10 := memTable.NewMemTableEntry("bba")
	entry11 := memTable.NewMemTableEntry("bad")
	entry12 := memTable.NewMemTableEntry("bbb")
	entry13 := memTable.NewMemTableEntry("bae")
	entry14 := memTable.NewMemTableEntry("gae")
	entry15 := memTable.NewMemTableEntry("gxx")
	entry16 := memTable.NewMemTableEntry("yyy")
	entry17 := memTable.NewMemTableEntry("aba")

	tables := memTable.InitMemTablesSkipList(3, 7, 3)

	tables.Add(entry)
	tables.Add(entry2)
	tables.Add(entry)
	tables.Add(entry)
	tables.Add(entry)
	tables.Add(entry)

	tables.Add(entry3)
	tables.Add(entry4)
	tables.Add(entry5)
	tables.Add(entry6)
	tables.Add(entry7)
	tables.Add(entry8)
	tables.Add(entry9)
	tables.Add(entry10)
	tables.Add(entry11)
	tables.Add(entry12)
	tables.Add(entry13)
	tables.Add(entry14)
	tables.Add(entry15)
	tables.Add(entry16)
	tables.Add(entry17)
	tables.Print()

	tables.Sort()
*/
