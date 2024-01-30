package sstable

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
)

type SSTable struct {
	generalFilename string
	SSTableFilename string
	indexFilename   string
	summaryFilename string
	filterFilename  string
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CreateSStable(data memTableEntry, filename string) (table *SSTable) {
	generalFilename := "data/sstable/usertable" + filename + "-lev1-" //
	table = &SSTable{generalFilename, generalFilename + "Data.db", generalFilename + "Index.db",
		generalFilename + "Summary.db", generalFilename + "Filter.gob"}

	filter := NewBloomFilter(data.Size(), 2)
	keys := make([]string, 0)
	offset := make([]uint, 0) //position in the data
	values := make([][]byte, 0)
	currentOffset := uint(0)
	file, err := os.Create(table.SSTableFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, uint64(data.Size()))
	bytesWritten, err := writer.Write(bytesLen)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	for node := data.data.head.Next[0]; node != nil; node = node.Next[0] {
		key := node.Key
		value := node.Value
		keys = append(keys, key)
		offset = append(offset, currentOffset)
		values = append(values, value)

		filter.Add(node)
		crc := CRC32(value)
		crcBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(crcBytes, crc)
		bytesWritten, err := writer.Write(crcBytes)
		currentOffset += uint(bytesWritten)
		if err != nil {
			return
		}

		//Timestamp
		timestamp := node.Timestamp
		timestampBytes := make([]byte, 19)
		copy(timestampBytes, timestamp)

		bytesWritten, err = writer.Write(timestampBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		//Tombstone
		tombstone := node.Tombstone
		tombstoneInt := uint8(0)
		if tombstone {
			tombstoneInt = 1
		}

		err = writer.WriteByte(tombstoneInt)
		currentOffset += 1
		if err != nil {
			return
		}

		keyBytes := []byte(key)

		keyLen := uint64(len(keyBytes))
		keyLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLenBytes, keyLen)
		bytesWritten, err = writer.Write(keyLenBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		valueLen := uint64(len(value))
		valueLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueLenBytes, valueLen)
		bytesWritten, err = writer.Write(valueLenBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(keyBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(value)
		if err != nil {
			return
		}
		currentOffset += uint(bytesWritten)

		err = writer.Flush()
		if err != nil {
			return
		}
	}

	index := CreateIndex(keys, offset, table.indexFilename)
	keys, offsets := index.Write()
	WriteSummary(keys, offsets, table.summaryFilename)
	//upis !

	return
}

func (st *SSTable) SStableFind(key string, offset int64) (validator bool, value []byte, timestamp string) {
	validator = false
	timestamp = ""

	file, err := os.Open(st.SSTableFilename)
	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(bytes) //for file size
	_, err = file.Seek(offset, 0)                //start search
	if err != nil {
		return false, nil, ""
	}
	reader = bufio.NewReader(file)

	var i uint64
	for i = 0; i < fileLen; i++ {
		deleted := false

		// crc
		crcBytes := make([]byte, 4)
		_, err = reader.Read(crcBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		crcValue := binary.LittleEndian.Uint32(crcBytes)

		// Timestamp
		timestampBytes := make([]byte, 19)
		_, err = reader.Read(timestampBytes)
		if err != nil {
			panic(err)
		}
		timestamp = string(timestampBytes[:])

		//Tombstone
		tombstone, err := reader.ReadByte()
		if err != nil {
			panic(err)
		}

		if tombstone == 1 {
			deleted = true
		}

		// keyLen
		keyLenBytes := make([]byte, 8)
		_, err = reader.Read(keyLenBytes)
		if err != nil {
			panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(keyLenBytes)

		valueLenBytes := make([]byte, 8)
		_, err = reader.Read(valueLenBytes)
		if err != nil {
			panic(err)
		}
		valueLen := binary.LittleEndian.Uint64(valueLenBytes)

		keyBytes := make([]byte, keyLen)
		_, err = reader.Read(keyBytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(keyBytes[:])

		if nodeKey == key { //matching
			validator = true
		}

		valueBytes := make([]byte, valueLen)
		_, err = reader.Read(valueBytes)
		if err != nil {
			panic(err)
		}

		if validator && !deleted && CRC32(valueBytes) == crcValue {
			value = valueBytes
			break
		} else if validator && deleted { //matching but key has been delited
			return false, nil, ""
		}
	}
	file.Close()
	return validator, value, timestamp
}
