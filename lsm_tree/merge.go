package lsm_tree

import (
	"encoding/binary"
	"os"
	"projekat_nasp/memTable"
	"projekat_nasp/sstable"
)

// Spajanje dve ss tabele u jednu novu, first i second su putanje do tabela
func MergeTables(first, second string, level int) error {
	// ucitaj fajlove
	// redom idi kroz fajlove i izvrsavaj merge
	// napisi novu tabelu

	firstFile, err := os.Open(first)
	if err != nil {
		return err
	}

	secondFile, err := os.Open(second)
	if err != nil {
		return err
	}

	// Dobavljanje duzine data segmenta
	firstLength, err := getDataSegmentLength(firstFile)
	if err != nil {
		return err
	}

	secondLength, err := getDataSegmentLength(secondFile)
	if err != nil {
		return err
	}

	// Pozicioniranje na data segment (zaglavlje je 32 bajtova)
	_, err = firstFile.Seek(32, 0)
	if err != nil {
		return err
	}

	_, err = secondFile.Seek(32, 0)
	if err != nil {
		return err
	}

	// Redosledna obrada
	records := sequentialUpdate(firstFile, secondFile, firstLength, secondLength)

	sstable.NewSSTable(&records, level)

	err = firstFile.Close()
	if err != nil {
		return err
	}

	err = secondFile.Close()
	if err != nil {
		return err
	}

	err = os.Remove(first)
	if err != nil {
		return err
	}

	err = os.Remove(second)
	if err != nil {
		return err
	}

	return nil
}

func getDataSegmentLength(f *os.File) (int64, error) {
	b := make([]byte, 8)

	_, err := f.Read(b)
	if err != nil {
		return 0, err
	}

	length := int64(binary.LittleEndian.Uint64(b)) - 32 // Oduzimamo zaglavlje

	return length, nil
}

type Unit struct {
	r     *memTable.MemTableEntry
	end   bool
	f     *os.File
	top   int
	count int
}

func (u *Unit) isNewer(other Unit) bool {
	return u.r.GetTimeStamp()> other.r.GetTimeStamp()
}

func (u *Unit) isAlive() bool {
	return u.r.GetTombstone()==1
}

func (u *Unit) nextRecord() {
	u.count++
	if u.count > u.top {
		u.end = true
	}

	if !u.end {
		rec, _, err := bytesToRecord(u.f)
		if err != nil {
			panic(err)
		}
		u.r = &rec
	}
}

func sequentialUpdate(first, second *os.File, firstLength, secondLength int64) []memTable.MemTableEntry {
	records := make([]memTable.MemTableEntry, 0)

	firstRecordsNum := sstable.CountRecords(first.Name())
	secondRecordsNum := sstable.CountRecords(second.Name())

	firstUnit := Unit{r: nil, end: false, f: first, top: firstRecordsNum, count: 0}
	secondUnit := Unit{r: nil, end: false, f: second, top: secondRecordsNum, count: 0}

	for !firstUnit.end || !secondUnit.end {
		// Prvo citanje
		if firstUnit.count == 0 && secondUnit.count == 0 {
			firstUnit.nextRecord()
			secondUnit.nextRecord()
		}

		// Uporedjivanje
		if firstUnit.r.GetKey() == secondUnit.r.GetKey() {
			if firstUnit.isNewer(secondUnit) {
				records = append(records, *firstUnit.r)
			} else {
				records = append(records, *secondUnit.r)
			}

			firstUnit.nextRecord()
			secondUnit.nextRecord()
		} else if firstUnit.r.GetKey() > secondUnit.r.GetKey() {
			if secondUnit.end {
				records = append(records, *firstUnit.r)
				firstUnit.nextRecord()
			} else {
				records = append(records, *secondUnit.r)
				secondUnit.nextRecord()
			}
		} else if secondUnit.r.GetKey() > firstUnit.r.GetKey() {
			if firstUnit.end {
				records = append(records, *secondUnit.r)
				secondUnit.nextRecord()
			} else {
				records = append(records, *firstUnit.r)
				firstUnit.nextRecord()
			}
		}
	}

	return records
}

func bytesToRecord(f *os.File) (memTable.MemTableEntry, int64, error) {
	// Struktura: KS(8), VS(8), TIME(8), TB(1), K(...), V(...)
	buffer := make([]byte, 8)
	tombstoneBuffer := make([]byte, 1)
	// Key size
	_, err := f.Read(buffer)
	if err != nil {
		return memTable.MemTableEntry{}, 0, err
	}
	keySize := binary.LittleEndian.Uint64(buffer)

	// Value size
	_, err = f.Read(buffer)
	if err != nil {
		return memTable.MemTableEntry{}, 0, err
	}
	valueSize := binary.LittleEndian.Uint64(buffer)

	// Timestamp
	_, err = f.Read(buffer)
	if err != nil {
		return memTable.MemTableEntry{}, 0, err
	}
	timestamp := binary.LittleEndian.Uint64(buffer)

	// Tombstone
	_, err = f.Read(tombstoneBuffer)
	if err != nil {
		return memTable.MemTableEntry{}, 0, err
	}
	tombstone := tombstoneBuffer[0] 

	// Key
	keyBuffer := make([]byte, keySize)
	_, err = f.Read(keyBuffer)
	if err != nil {
		return memTable.MemTableEntry{}, 0, err
	}
	key := string(keyBuffer)

	// Value
	value := make([]byte, valueSize)
	_, err = f.Read(value)
	if err != nil {
		return memTable.MemTableEntry{}, 0, err
	}

	readBytes := 25 + len(key) + len(value) // 25 je fiksna duzina prvih 4 polja

	return memTable.FillWithParametersEntry(key,value,timestamp,tombstone), int64(readBytes), nil
}