package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"os"
	"projekat_nasp/bloom_filter"
	"projekat_nasp/memTable"
	"strconv"
	"strings"
)

type SSTable struct {
	generalFilename string
	SSTableFilename string
	indexFilename   string
	summaryFilename string
	filterFilename  string
}

func (st *SSTable) WriteTOC() {
	filename := st.generalFilename + "TOC.txt" //table of contents
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	_, err = writer.WriteString(st.SSTableFilename + "\n") //1
	if err != nil {
		return
	}
	_, err = writer.WriteString(st.indexFilename + "\n") //2
	if err != nil {
		return
	}
	_, err = writer.WriteString(st.summaryFilename + "\n") //3
	if err != nil {
		return
	}
	_, err = writer.WriteString(st.filterFilename) //4
	if err != nil {
		return
	}

	err = writer.Flush()
	if err != nil {
		return
	}
}

func readSSTable(filename, level string) (table *SSTable) {
	filename = "data/sstable/usertable" + filename + "-lev" + level + "-TOC.txt"

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	SSTableFilename, _ := reader.ReadString('\n') //1
	indexFilename, _ := reader.ReadString('\n')   //2
	summaryFilename, _ := reader.ReadString('\n') //3
	filterFilename, _ := reader.ReadString('\n')  //4
	generalFilename := strings.ReplaceAll(SSTableFilename, "Data.db\n", "")

	table = &SSTable{generalFilename: generalFilename,
		SSTableFilename: SSTableFilename[:len(SSTableFilename)-1], indexFilename: indexFilename[:len(indexFilename)-1],
		summaryFilename: summaryFilename[:len(summaryFilename)-1], filterFilename: filterFilename}

	return
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CreateSStable(data []memTable.MemTableEntry, filename string) (table *SSTable) {
	generalFilename := "data/sstable/usertable" + filename + "-lev1-" //
	table = &SSTable{generalFilename, generalFilename + "Data.db", generalFilename + "Index.db",
		generalFilename + "Summary.db", generalFilename + "Filter.gob"}

	filter := bloom_filter.NewBloomFilter(len(data), 2)
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
	binary.LittleEndian.PutUint64(bytesLen, uint64(len(data)))
	bytesWritten, err := writer.Write(bytesLen)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	err = writer.Flush()
	if err != nil {
		return
	}
	//node := data.data.head.Next[0]; node != nil; node = node.Next[0]
	for i := 0; i < len(data); i++ {
		node := data[i]
		key := node.GetKey()
		value := node.GetValue()
		keys = append(keys, key)
		offset = append(offset, currentOffset)
		values = append(values, value)

		filter.Add(key)
		crc := CRC32(value)
		crcBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(crcBytes, crc)
		bytesWritten, err := writer.Write(crcBytes)
		currentOffset += uint(bytesWritten)
		if err != nil {
			return
		}

		//Timestamp
		timestamp := node.GetTimeStamp()
		timestampBytes := make([]byte, 64)
		binary.LittleEndian.PutUint64(timestampBytes, timestamp)

		//copy(timestampBytes, timestamp)

		bytesWritten, err = writer.Write(timestampBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		//Tombstone
		tombstone := node.GetTombstone()
		/*
			tombstoneInt := uint8(0)
			if tombstone {
				tombstoneInt = 1
			}
		*/
		err = writer.WriteByte(tombstone)
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
	table.WriteTOC()

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

func (st *SSTable) SSTableQuery(key string) (ok bool, value []byte, timestamp string) { //za ključ
	ok = false
	value = nil
	bf := ReadBloomFilter(st.filterFilename)
	ok = bf.Query(key)
	if ok {
		ok, offset := FindSummary(key, st.summaryFilename)
		if ok {
			ok, offset = FindIndex(key, offset, st.indexFilename)
			if ok {
				ok, value, timestamp = st.SStableFind(key, offset)
				if ok {
					return true, value, timestamp
				}
			}
		}
	}
	return false, nil, ""
}

func findSSTableFilename(level string) (filename string) {
	filenameNum := 1
	filename = strconv.Itoa(filenameNum)
	possibleFilename := "./data/sstable/usertable" + filename + "-lev" + level + "-TOC.txt"

	for {
		_, err := os.Stat(possibleFilename)
		if err == nil {
			filenameNum += 1
			filename = strconv.Itoa(filenameNum)
		} else if errors.Is(err, os.ErrNotExist) {
			return
		}
		possibleFilename = "./data/sstable/usertable" + filename + "-lev" + level + "-TOC.txt"
	}

}

func SearchThroughSSTables(key string, maxLevels int) (found bool, oldValue []byte) {
	oldTimestamp := ""
	found = false
	levelNum := maxLevels
	for ; levelNum >= 1; levelNum-- {
		level := strconv.Itoa(levelNum)
		maxFilename := findSSTableFilename(level)
		maxFilenameNum, _ := strconv.Atoi(maxFilename)
		filenameNum := maxFilenameNum - 1
		for ; filenameNum > 0; filenameNum-- {
			filename := strconv.Itoa(filenameNum)
			table := readSSTable(filename, level)
			ok, value, timestamp := table.SSTableQuery(key)
			if oldTimestamp == "" && ok {
				oldTimestamp = timestamp
				found = true
				oldValue = value
			} else if oldTimestamp != "" && ok {
				if timestamp > oldTimestamp {
					oldValue = value
					found = true
				}
			}
		}
	}
	return
}
