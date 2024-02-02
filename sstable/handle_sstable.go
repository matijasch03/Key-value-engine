package sstable

import (
	"encoding/binary"
	"os"
	"projekat_nasp/memTable"
	"projekat_nasp/bloom_filter"
	"bytes"
	"bufio"
)

// Citanje ss tabela
func ReadTables(keys []string, full bool) []memTable.MemTableEntry{
	tables, err := GetTables()
	if err != nil {
		panic(err)
	}
	if len(tables) <= 0 {
		return nil
	}

	var records [][]memTable.MemTableEntry

	for _, table := range tables {
		data := FindByKey(keys, "data/sstable"+table, full)
		records = append(records, data)
	}

	result := mergeData(records)

	return result
}

func mergeData(data [][]memTable.MemTableEntry) []memTable.MemTableEntry {
	freshTable := data[0] // Najsvezija tabela

	for i := 1; i < len(data); i++ {
		for _, rec := range data[i] {
			if !ContainsRecord(freshTable, rec) {
				freshTable = append(freshTable, rec)
			} else {
				swapNewerRecord(&freshTable, rec)
			}
		}
	}

	var result []memTable.MemTableEntry
	for _, rec := range freshTable {
		if rec.GetTombstone()!=0 {
			result = append(result, rec)
		}
	}

	return result
}

func swapNewerRecord(table *[]memTable.MemTableEntry, rec memTable.MemTableEntry) {
	for i, r := range *table {
		if rec.GetKey() == r.GetKey() && rec.GetTimeStamp() > r.GetTimeStamp(){
			(*table)[i] = rec
		}
	}
}

func ContainsRecord(table []memTable.MemTableEntry, target memTable.MemTableEntry) bool {
	found := false

	for _, rec := range table {
		if rec.GetKey() == target.GetKey() {
			found = true
			break
		}
	}

	return found
}


// f-ja prima kljuc putanju do fajla i vrednost da li se trazi prefix
func FindByKey(keys []string, path string, full bool) []memTable.MemTableEntry {
	f, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}
	var key string
	var keySec string
	if len(keys) == 1 { // provera da li se trazi range ili samo jedan kljuc ili prefix
		key = keys[0]
		keySec = ""
	} else {
		key = keys[0]
		keySec = keys[1]
	}

	maybeInFile := true
	if full && len(keys) == 1 {
		maybeInFile = checkBloomFilter(f, key)
	}
	defer f.Close()

	if maybeInFile {
		// idi u summary
		result := checkSummary(f, key, full, keySec)
		if len(result) != 0 {
			return result
		} else {
			return []memTable.MemTableEntry{}
		}
	} else {
		return []memTable.MemTableEntry{}
	}

}

// provera da li se nalazi u bloom filteru
func checkBloomFilter(file *os.File, key string) bool {
	var bF bloom_filter.BloomFilterUnique
	var bfpos int64
	var bfDS int64
	// cita gde je bf
	file.Seek(16, 0)
	bufferedReader := bufio.NewReader(file)
	byteSlice := make([]byte, M_SIZE)
	_, err := bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfpos) // u bfPos se smesta pozicija Bloom Filtera da bi znali da pocnemo sa citanjem

	// cita velicinu data u bf
	file.Seek(24, 0)
	bufferedReader = bufio.NewReader(file)
	byteSlice = make([]byte, M_SIZE)
	_, err = bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfDS) // da bi se kretali kroz data sgment bloom filetra

	// ide na poziciju bf i cita M
	file.Seek(bfpos, 0)
	bufferedReader = bufio.NewReader(file)
	byteSlice = make([]byte, M_SIZE)
	_, err = bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	var bfM int64
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfM)
	bF.M = uint(bfM)

	byteSlice = make([]byte, bfDS)
	_, err = bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	bF.Data = byteSlice

	var forRead int64
	for { // redom da citamo data iz bloom filtera dok ne proveri sve
		forRead = 0
		// citam velicnu jendog podatka
		byteSlice = make([]byte, K_SIZE)
		_, err = bufferedReader.Read(byteSlice)
		if err != nil {
			break
		}
		binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &forRead)

		// pravim prostor unapred poznate velicine i citam toliko bajtova
		byteSlice = make([]byte, forRead)
		_, err = bufferedReader.Read(byteSlice)
		if err != nil {
			break
		}
		bF.HashFunctions = append(bF.HashFunctions, bloom_filter.HashWithSeed{Seed: byteSlice})

	}
	// procitamo da bloom filter da dobijemo informaciju da li je mozda unutra kljuc
	if bF.Read([]byte(key)) {
		return true
	} else {
		return false
	}
}

// citanje summary
func checkSummary(file *os.File, key string, full bool, keySec string) []memTable.MemTableEntry {
	file.Seek(0, 0)
	bufferedReader := bufio.NewReader(file)
	dsb := make([]byte, K_SIZE)
	isb := make([]byte, K_SIZE)
	bfb := make([]byte, K_SIZE)
	_, err := bufferedReader.Read(dsb)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	_, err = bufferedReader.Read(isb)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	_, err = bufferedReader.Read(bfb)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	var ds int64
	var is int64
	var bf int64
	binary.Read(bytes.NewReader(dsb), binary.LittleEndian, &ds) // ove tri linije smestaju podatke iz hedera
	binary.Read(bytes.NewReader(isb), binary.LittleEndian, &is)
	binary.Read(bytes.NewReader(bfb), binary.LittleEndian, &bf)
	sumPos := ds + is - HEADER_SIZE

	file.Seek(int64(sumPos), 0)
	bufferedReader = bufio.NewReader(file)
	var keyLen int64
	keyLenB := make([]byte, K_SIZE)
	_, err = bufferedReader.Read(keyLenB)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(otherLenB)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	key1 := string(otherLenB[0:keyLen])
	if full && key < key1 && keySec == "" {
		return []memTable.MemTableEntry{}
	}
	var index1 int64
	binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index1)
	sumPos += K_SIZE + keyLen + VALUE_SIZE_LEN

	key2 := key1
	index2 := index1
	for sumPos < bf { // vrtimo se po summary dok ne nadjemo opseg u kom nastavljamo trazenje
		var keyLen int64
		keyLenB := make([]byte, K_SIZE)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return []memTable.MemTableEntry{}
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(otherLenB)
		if err != nil {
			return []memTable.MemTableEntry{}
		}
		key2 = string(otherLenB[0:keyLen])
		binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
		// odvajamo slucaj kad se trazi ceo kljuc a ne samo njegov prefix
		if full {
			if key >= key1 && key2 > key && keySec == "" {
				return checkIndexZone(key, index1, index2, file, ds, is, full, keySec) // nasli opseg pa idemo u index zonu
			}
			if len(key) <= len(key1) {
				if key <= key1[:len(key)] && keySec != "" {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec) // nasli opseg pa idemo u index zonu
				}
				if key >= key1[:len(key)] && keySec != "" && key < key2 {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec) // nasli opseg pa idemo u index zonu
				}
			} else {
				if key <= key1 && keySec != "" {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec) // nasli opseg pa idemo u index zonu
				}
				if key >= key1 && keySec != "" && key < key2 {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec) // nasli opseg pa idemo u index zonu
				}
			}
		} else {
			if len(key) <= len(key1) {
				if key >= key1[:len(key)] && key2 > key {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec)
				}
			} else {
				if key >= key1 && key2 > key {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec)
				}
			}
		}
		sumPos += K_SIZE + keyLen + VALUE_SIZE_LEN
		key1 = key2
		index1 = index2
	}
	return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec) // vrattiti nes
	// if full {
	// 	if len(key) <= len(key2) {
	// 		if key >= key2[:len(key)] {
	// 			return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec) // vrattiti nes
	// 		}
	// 	} else {
	// 		if key >= key2 {
	// 			return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec) // vrattiti nes
	// 		}
	// 	}
	// } else {
	// 	if len(key) <= len(key2) {
	// 		if key >= key2[:len(key)] {
	// 			return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec) // vrattiti nes
	// 		}
	// 	} else {
	// 		if key >= key2 {
	// 			return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec) // vrattiti nes
	// 		}
	// 	}
	// }

	// return []record.Record{}
}

// listamo index zonu da bi nasli opseg i data zoni
func checkIndexZone(key string, iPos int64, maxPos int64, file *os.File, ds int64, is int64, full bool, keySec string) []memTable.MemTableEntry {
	file.Seek(int64(iPos), 0)
	bufferedReader := bufio.NewReader(file)
	var keyLen int64
	keyLenB := make([]byte, K_SIZE)
	_, err := bufferedReader.Read(keyLenB)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(otherLenB)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	key1 := string(otherLenB[0:keyLen])
	if full && key < key1 && keySec == "" {
		return []memTable.MemTableEntry{}
	}
	var index1 int64
	binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index1)
	iPos += K_SIZE + keyLen + VALUE_SIZE_LEN

	key2 := key1
	index2 := index1
	for iPos < maxPos { // vrtimo se kroz index zonu dok ne upadnemo u opseg neki
		var keyLen int64
		keyLenB := make([]byte, K_SIZE)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return []memTable.MemTableEntry{}
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(otherLenB)
		if err != nil {
			return []memTable.MemTableEntry{}
		}
		key2 = string(otherLenB[0:keyLen])
		binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
		if full {
			if key >= key1 && key2 > key && keySec == "" {
				return checkDataZone(key, index1, index2, file, ds, full, keySec) // nasli smo opseg ulazimo u data zonu
			}
			if len(key) <= len(key1) {
				if key <= key1[:len(key)] && keySec != "" {
					return checkDataZone(key, index1, index2, file, ds, full, keySec) // nasli opseg pa idemo u index zonu
				}
				if key >= key1[:len(key)] && keySec != "" && key < key2 {
					return checkDataZone(key, index1, index2, file, ds, full, keySec) // nasli opseg pa idemo u index zonu
				}
			} else {
				if key <= key1 && keySec != "" {
					return checkDataZone(key, index1, index2, file, ds, full, keySec) // nasli opseg pa idemo u index zonu
				}
				if key >= key1 && keySec != "" && key < key2 {
					return checkDataZone(key, index1, index2, file, ds, full, keySec) // nasli opseg pa idemo u index zonu
				}
			}
		} else {
			if len(key) <= len(key1) {
				if key >= key1[:len(key)] && key2 > key {
					return checkDataZone(key, index1, index2, file, ds, full, keySec)
				}
			} else {
				if key >= key1 && key2 > key {
					return checkDataZone(key, index1, index2, file, ds, full, keySec)
				}
			}
		}

		iPos += K_SIZE + keyLen + VALUE_SIZE_LEN
		key1 = key2
		index1 = index2
	}
	// kao i u summary odvajamo slucajeve da li se trazi prefix
	if full {
		if len(key) <= len(key2) {
			if key >= key2[:len(key)] && maxPos == ds+is-HEADER_SIZE {
				return checkDataZone(key, index2, ds, file, ds, full, keySec)
			} else {
				var keyLen int64
				keyLenB := make([]byte, K_SIZE)
				_, err = bufferedReader.Read(keyLenB)
				if err != nil {
					return []memTable.MemTableEntry{}
				}
				binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
				otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
				_, err = bufferedReader.Read(otherLenB)
				if err != nil {
					return []memTable.MemTableEntry{}
				}
				// key2 = string(otherLenB[0:keyLen])
				binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
				return checkDataZone(key, index1, index2, file, ds, full, keySec)
			}
		} else {
			if key >= key2 && maxPos == ds+is-HEADER_SIZE {
				return checkDataZone(key, index2, ds, file, ds, full, keySec)
			} else {
				var keyLen int64
				keyLenB := make([]byte, K_SIZE)
				_, err = bufferedReader.Read(keyLenB)
				if err != nil {
					return []memTable.MemTableEntry{}
				}
				binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
				otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
				_, err = bufferedReader.Read(otherLenB)
				if err != nil {
					return []memTable.MemTableEntry{}
				}
				// key2 = string(otherLenB[0:keyLen])
				binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
				return checkDataZone(key, index1, index2, file, ds, full, keySec)
			}
		}

	} else {
		if key >= key2 && maxPos == ds+is-HEADER_SIZE {
			return checkDataZone(key, index2, ds, file, ds, full, keySec)
		} else {
			var keyLen int64
			keyLenB := make([]byte, K_SIZE)
			_, err = bufferedReader.Read(keyLenB)
			if err != nil {
				return []memTable.MemTableEntry{}
			}
			binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
			otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
			_, err = bufferedReader.Read(otherLenB)
			if err != nil {
				return []memTable.MemTableEntry{}
			}
			// key2 = string(otherLenB[0:keyLen])
			binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
			return checkDataZone(key, index1, index2, file, ds, full, keySec)
		}
	}
}

// koncno trazenje u data zoni na osnovu opsega ustanovljenih u prethodne dve zone
func checkDataZone(key string, iPos int64, maxPos int64, file *os.File, ds int64, full bool, keySec string) []memTable.MemTableEntry {
	file.Seek(int64(iPos), 0)
	var keyLen int64
	var valueLen int64
	var newKey string
	var timestamp int64
	var tombstone byte
	var vrednosti []memTable.MemTableEntry
	bufferedReader := bufio.NewReader(file)
	keyLenB := make([]byte, KEY_SIZE_LEN)
	_, err := bufferedReader.Read(keyLenB)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	valueLenB := make([]byte, VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(valueLenB)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	binary.Read(bytes.NewReader(valueLenB), binary.LittleEndian, &valueLen)

	otherB := make([]byte, keyLen+valueLen+TIMESTAMP_LEN+TOMBSTONE_LEN)
	_, err = bufferedReader.Read(otherB)
	if err != nil {
		return []memTable.MemTableEntry{}
	}
	newKey = string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN : TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen])
	if full {
		if key == newKey && keySec == "" {
			vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
			binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
			binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
			var tombstoneb byte
			if tombstone == 0 {
				tombstoneb = 0
			} else {
				tombstoneb = 1
			}
			vrednosti = append(vrednosti, memTable.NewMemTableEntry(key,vrednost,tombstoneb,uint64(timestamp)))

			return vrednosti
		}
		if keySec != "" && key <= newKey && key <= keySec {
			vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
			binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
			binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
			var tombstoneb byte
			if tombstone == 0 {
				tombstoneb = 0
			} else {
				tombstoneb = 1
			}
			vrednosti = append(vrednosti, memTable.NewMemTableEntry(key,vrednost,tombstoneb,uint64(timestamp)))
		}
	} else {
		if len(key) <= len(newKey) {
			if key == newKey[:len(key)] {
				vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
				var tombstoneb byte
				if tombstone == 0 {
					tombstoneb = 0
				} else {
					tombstoneb = 1
				}
				vrednosti = append(vrednosti, memTable.NewMemTableEntry(key,vrednost,tombstoneb,uint64(timestamp)))
			}
		}
	}
	iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
	if iPos >= ds {
		return vrednosti
	}
	// u slucaju da se trazi samo kljuc sa odredjeno vrednoscu vrtimo se dok njega ne pronadjemo ili dok ne dodjemo do kraja te zone
	// ako se trazi range ili list onda se vrtimo do kraja opsega u ubacujemo poklapanja u listu koju vracamo
	for iPos < maxPos {
		file.Seek(iPos, 0)
		bufferedReader = bufio.NewReader(file)
		keyLenB = make([]byte, KEY_SIZE_LEN)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return []memTable.MemTableEntry{}
		}
		valueLenB = make([]byte, VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(valueLenB)
		if err != nil {
			return []memTable.MemTableEntry{}
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		binary.Read(bytes.NewReader(valueLenB), binary.LittleEndian, &valueLen)

		otherB = make([]byte, keyLen+valueLen+TIMESTAMP_LEN+TOMBSTONE_LEN)
		_, err = bufferedReader.Read(otherB)
		if err != nil {
			return []memTable.MemTableEntry{}
		}
		newKey = string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN : TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen])
		if (newKey > keySec || iPos > ds) && keySec != "" {
			return vrednosti
		}
		if full {
			if key == newKey && keySec == "" {
				vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
				var tombstoneb byte
				if tombstone == 0 {
					tombstoneb = 0
				} else {
					tombstoneb = 1
				}
				vrednosti = append(vrednosti, memTable.NewMemTableEntry(key,vrednost,tombstoneb,uint64(timestamp)))

				return vrednosti
			}
			if keySec != "" && key <= newKey && key <= keySec {
				vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
				var tombstoneb byte
				if tombstone == 0 {
					tombstoneb = 0
				} else {
					tombstoneb = 1
				}
				vrednosti = append(vrednosti, memTable.NewMemTableEntry(key,vrednost,tombstoneb,uint64(timestamp)))

			}

		} else {
			if len(key) <= len(newKey) {
				if key < newKey[:len(key)] {
					return vrednosti
				}
				if key == newKey[:len(key)] {
					vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
					binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
					binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
					var tombstoneb byte
					if tombstone == 1 {
						tombstoneb = 1
					} else {
						tombstoneb = 0
					}
					vrednosti = append(vrednosti, memTable.NewMemTableEntry(key,vrednost,tombstoneb,uint64(timestamp)))
				}
			}
		}
		maxPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN

		iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
		if iPos >= ds {
			return vrednosti
		}
	}
	return vrednosti
}