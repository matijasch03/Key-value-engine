package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"projekat_nasp/bloom_filter"
	"projekat_nasp/memTable"
)

func FindByKey(keys []string, path string, full bool) []memTable.MemTableEntry {
	f, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}
	var key string
	var keySec string
	if len(keys) == 1 {
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
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfpos)
	file.Seek(24, 0)
	bufferedReader = bufio.NewReader(file)
	byteSlice = make([]byte, M_SIZE)
	_, err = bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfDS)
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
	for {
		forRead = 0
		byteSlice = make([]byte, K_SIZE)
		_, err = bufferedReader.Read(byteSlice)
		if err != nil {
			break
		}
		binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &forRead)
		byteSlice = make([]byte, forRead)
		_, err = bufferedReader.Read(byteSlice)
		if err != nil {
			break
		}
		bF.HashFunctions = append(bF.HashFunctions, bloom_filter.HashWithSeed{Seed: byteSlice})

	}
	if bF.Read([]byte(key)) {
		return true
	} else {
		return false
	}
}

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
	binary.Read(bytes.NewReader(dsb), binary.LittleEndian, &ds)
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
	for sumPos < bf {
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
				return checkIndexZone(key, index1, index2, file, ds, is, full, keySec)
			}
			if len(key) <= len(key1) {
				if key <= key1[:len(key)] && keySec != "" {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec)
				}
				if key >= key1[:len(key)] && keySec != "" && key < key2 {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec)
				}
			} else {
				if key <= key1 && keySec != "" {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec)
				}
				if key >= key1 && keySec != "" && key < key2 {
					return checkIndexZone(key, index1, index2, file, ds, is, full, keySec)
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
	return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec)
}

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
	for iPos < maxPos {
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
				return checkDataZone(key, index1, index2, file, ds, full, keySec)
			}
			if len(key) <= len(key1) {
				if key <= key1[:len(key)] && keySec != "" {
					return checkDataZone(key, index1, index2, file, ds, full, keySec)
				}
				if key >= key1[:len(key)] && keySec != "" && key < key2 {
					return checkDataZone(key, index1, index2, file, ds, full, keySec)
				}
			} else {
				if key <= key1 && keySec != "" {
					return checkDataZone(key, index1, index2, file, ds, full, keySec)
				}
				if key >= key1 && keySec != "" && key < key2 {
					return checkDataZone(key, index1, index2, file, ds, full, keySec)
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

			binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
			return checkDataZone(key, index1, index2, file, ds, full, keySec)
		}
	}
}

func checkDataZone(key string, iPos int64, maxPos int64, file *os.File, ds int64, full bool, keySec string) []memTable.MemTableEntry {
	file.Seek(int64(iPos), 0)
	var keyLen int64
	var valueLen int64
	var newKey string
	var timestamp int64
	var tombstone byte
	var values []memTable.MemTableEntry
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
			value := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
			binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
			binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)

			values = append(values, memTable.NewMemTableEntry(newKey, value, tombstone, uint64(timestamp)))

			return values
		}
		if keySec != "" && key <= newKey && key <= keySec {
			value := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
			binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
			binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)

			values = append(values, memTable.NewMemTableEntry(newKey, value, tombstone, uint64(timestamp)))

		}
	} else {
		if len(key) <= len(newKey) {
			if key == newKey[:len(key)] {
				value := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)

				values = append(values, memTable.NewMemTableEntry(newKey, value, tombstone, uint64(timestamp)))

			}
		}
	}
	iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
	if iPos >= ds {
		return values
	}

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
			return values
		}
		if full {
			if key == newKey && keySec == "" {
				value := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)

				values = append(values, memTable.NewMemTableEntry(newKey, value, tombstone, uint64(timestamp)))

				return values
			}
			if keySec != "" && key <= newKey && key <= keySec {
				value := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)

				values = append(values, memTable.NewMemTableEntry(newKey, value, tombstone, uint64(timestamp)))

			}

		} else {
			if len(key) <= len(newKey) {
				if key < newKey[:len(key)] {
					return values
				}
				if key == newKey[:len(key)] {
					value := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
					binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
					binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
					values = append(values, memTable.NewMemTableEntry(newKey, value, tombstone, uint64(timestamp)))
				}
			}
		}
		maxPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN

		iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
		if iPos >= ds {
			return values
		}
	}
	return values
}
