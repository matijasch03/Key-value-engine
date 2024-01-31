package sstable

import (
	"bufio"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
)

type SSIndex struct {
	OffsetSize    uint
	KeySizeNumber uint
	DataKeys      []string
	DataOffset    []uint
	filename      string
}

func (index *SSIndex) Add(key string, offset uint) {
	index.DataKeys = append(index.DataKeys, key)
	index.DataOffset = append(index.DataOffset, offset)
}

func CreateIndex(keys []string, offset []uint, filename string) *SSIndex {
	index := SSIndex{filename: filename}
	for i, key := range keys {
		index.Add(key, offset[i])
	}
	return &index
}

func (index *SSIndex) Write() (keys []string, offsets []uint) {
	currentOffset := uint(0)
	file, err := os.Create(index.filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, uint64(len(index.DataKeys)))
	bytesWritten, err := writer.Write(bytesLen)
	if err != nil {
		log.Fatal(err)
	}

	currentOffset += uint(bytesWritten)

	err = writer.Flush()
	if err != nil {
		return
	}

	rangeKeys := make([]string, 0)
	rangeOffsets := make([]uint, 0)
	sampleKeys := make([]string, 0)
	sampleOffsets := make([]uint, 0)
	for i := range index.DataKeys {
		key := index.DataKeys[i]
		offset := index.DataOffset[i]
		if i == 0 || i == (len(index.DataKeys)-1) {
			rangeKeys = append(rangeKeys, key)
			rangeOffsets = append(rangeOffsets, currentOffset)
		} else if rand.Intn(100) > 50 {
			sampleKeys = append(sampleKeys, key)
			sampleOffsets = append(sampleOffsets, currentOffset)
		}
		bytes := []byte(key)

		keyLen := uint64(len(bytes))
		bytesLen := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytesLen, keyLen)
		bytesWritten, err := writer.Write(bytesLen)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(offset))
		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

	}
	err = writer.Flush()
	if err != nil {
		return
	}

	keys = append(rangeKeys, sampleKeys...)
	offsets = append(rangeOffsets, sampleOffsets...)
	return
}

/*func WriteSummary(keys []string, offsets []uint, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	fileLen := uint64(len(keys))
	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, fileLen)
	_, err = writer.Write(bytesLen)
	if err != nil {
		log.Fatal(err)
	}

	for i := range keys {
		key := keys[i]
		offset := offsets[i]

		bytes := []byte(key)

		keyLen := uint64(len(bytes))
		bytesLen := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytesLen, keyLen)
		_, err := writer.Write(bytesLen)
		if err != nil {
			log.Fatal(err)
		}

		_, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}

		if i >= 2 {
			bytes = make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(offset))
			_, err = writer.Write(bytes)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = writer.Flush()
		if err != nil {
			return
		}
	}
}*/

func WriteSummary(keys []string, offsets []uint, filename string, step int) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	fileLen := uint64(len(keys))
	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, fileLen)
	if _, err := writer.Write(bytesLen); err != nil {
		log.Fatal(err)
	}

	for i := range keys {
		key := keys[i]
		offset := offsets[i]

		// Dodajte proveru koraka proređenosti
		if i%step != 0 {
			continue
		}

		bytes := []byte(key)

		keyLen := uint64(len(bytes))
		binary.LittleEndian.PutUint64(bytesLen, keyLen)
		if _, err := writer.Write(bytesLen); err != nil {
			log.Fatal(err)
		}

		if _, err := writer.Write(bytes); err != nil {
			log.Fatal(err)
		}

		if i >= 2 {
			bytes = make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(offset))
			if _, err := writer.Write(bytes); err != nil {
				log.Fatal(err)
			}
		}

		if err := writer.Flush(); err != nil {
			log.Fatal(err)
		}
	}
}

func FindSummary(key, filename string) (ok bool, offset int64) {
	ok = false
	offset = int64(8)

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	bytes := make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(bytes)

	//start Key
	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	keyLen := binary.LittleEndian.Uint64(bytes)

	bytes = make([]byte, keyLen)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	startKey := string(bytes[:])

	if key < startKey {
		return false, 0
	}

	//end Key
	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	keyLen = binary.LittleEndian.Uint64(bytes)

	bytes = make([]byte, keyLen)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	endKey := string(bytes[:])

	if key > endKey {
		return false, 0
	}

	ok = true
	var i uint64
	for i = 0; i < fileLen-2; i++ {
		good := false
		bytes := make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(bytes)

		bytes = make([]byte, keyLen)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(bytes[:])

		if nodeKey <= key {
			good = true
		}

		bytes = make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)

		if good {
			offset = int64(newOffset)
		} else if !good {
			break
		}
	}
	return
}

func FindIndex(key string, offset int64, filename string) (ok bool, dataOffset int64) {
	ok = false
	dataOffset = 0

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(bytes)

	_, err = file.Seek(offset, 0)
	if err != nil {
		return false, 0
	}

	reader = bufio.NewReader(file)

	var i uint64
	for i = 0; i < fileLen; i++ {
		bytes := make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(bytes)

		bytes = make([]byte, keyLen)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(bytes[:])

		if nodeKey == key {
			ok = true
		} else if nodeKey > key {
			return false, 0
		}

		bytes = make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)

		if ok {
			dataOffset = int64(newOffset)
			break
		}
	}

	return
}
