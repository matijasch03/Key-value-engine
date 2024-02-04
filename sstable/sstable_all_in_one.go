package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"projekat_nasp/bloom_filter"
	"projekat_nasp/memTable"
	merkletree "projekat_nasp/merkle_tree"
	"time"
)

const (
	KEY_SIZE_LEN        = 8
	VALUE_SIZE_LEN      = 8
	TOMBSTONE_LEN       = 1
	TIMESTAMP_LEN       = 8
	KEY_VALUE_START     = KEY_SIZE_LEN + VALUE_SIZE_LEN + TOMBSTONE_LEN + TIMESTAMP_LEN
	HEADER_SIZE         = 32
	M_SIZE              = 8
	K_SIZE              = 8
	FALSE_POSITIVE_RATE = 0.001
)

type SSTable_Unique struct {
	dataSize     uint64
	indexSize    uint64
	summarySize  uint64
	summary      uint64
	blockLeaders []string
	blockIndexes []uint64
	indexLeaders []string
	IndexIndexes []uint64
	bF           bloom_filter.BloomFilterUnique
	bFPosition   uint64
	bFDataSize   uint64
	merkleData   [][]byte
	path         string
	unixTime     int64
}

func writeBlock(recordByte *[]byte, path string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	w.Write(*recordByte)
	w.Flush()
}

func writeSSTable(data *[]memTable.MemTableEntry, sstable *SSTable_Unique) {
	block_size := 2
	//for i := 0; i < len(*data); i++ {
	//node := data[i]
	for i, node := range *data {
		in := append([]byte(node.GetKey()), node.GetValue()...)
		sstable.merkleData = append(sstable.merkleData, in)
		in = nil

		sstable.bF.Add(([]byte(node.GetKey())))
		if i%int(block_size) == 0 {
			sstable.blockLeaders = append(sstable.blockLeaders, node.GetKey())
			sstable.blockIndexes = append(sstable.blockIndexes, sstable.dataSize+HEADER_SIZE)
		}
		var thumbstoneByte byte
		if node.GetTombstone() == 1 {
			thumbstoneByte = 1
		} else {
			thumbstoneByte = 0
		}

		recordByte := make([]byte, len([]byte(node.GetKey()))+len(node.GetValue())+KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN+TOMBSTONE_LEN)
		sstable.dataSize += uint64(len(recordByte))

		binary.LittleEndian.PutUint64(recordByte[0:KEY_SIZE_LEN], uint64(len([]byte(node.GetKey()))))
		binary.LittleEndian.PutUint64(recordByte[KEY_SIZE_LEN:KEY_SIZE_LEN+VALUE_SIZE_LEN], uint64(len(node.GetValue())))
		binary.LittleEndian.PutUint64(recordByte[KEY_SIZE_LEN+VALUE_SIZE_LEN:KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN], uint64(node.GetTimeStamp()))
		recordByte[KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN] = byte(thumbstoneByte)
		copy(recordByte[KEY_VALUE_START:KEY_VALUE_START+len([]byte(node.GetKey()))], []byte(node.GetKey()))
		copy(recordByte[KEY_VALUE_START+len([]byte(node.GetKey())):KEY_VALUE_START+len([]byte(node.GetKey()))+len(node.GetValue())], node.GetValue())

		writeBlock(&recordByte, sstable.path)
	}
	writeIndex(sstable)
	writeHeader(sstable)
	writeSummary(sstable)
	writeBloomFilter(sstable)
	merkletree.BuildMerkleTree(sstable.merkleData, sstable.unixTime)
}

func writeHeader(sstable *SSTable_Unique) {
	f, err := os.OpenFile(sstable.path, os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.Seek(0, 0)

	err = binary.Write(f, binary.LittleEndian, sstable.dataSize+HEADER_SIZE)
	if err != nil {
		println(err)
		return
	}

	err = binary.Write(f, binary.LittleEndian, sstable.indexSize+HEADER_SIZE)
	if err != nil {
		println(err)
		return
	}

	err = binary.Write(f, binary.LittleEndian, sstable.bFPosition)
	if err != nil {
		println(err)
		return
	}

	err = binary.Write(f, binary.LittleEndian, sstable.bFDataSize)
	if err != nil {
		println(err)
		return
	}

}

func writeIndex(sstable *SSTable_Unique) {

	block_size := 2
	for i, key := range sstable.blockLeaders {
		if i%int(block_size) == 0 {
			sstable.indexLeaders = append(sstable.indexLeaders, key)
			sstable.IndexIndexes = append(sstable.IndexIndexes, sstable.dataSize+HEADER_SIZE+sstable.indexSize)
		}
		recordByte := make([]byte, K_SIZE+len([]byte(key))+VALUE_SIZE_LEN)
		sstable.indexSize += uint64(len(recordByte))
		binary.LittleEndian.PutUint64(recordByte[0:K_SIZE], uint64(len([]byte(key))))
		copy(recordByte[K_SIZE:K_SIZE+len([]byte(key))], []byte(key))
		binary.LittleEndian.PutUint64(recordByte[K_SIZE+len([]byte(key)):], sstable.blockIndexes[i])

		writeBlock(&recordByte, sstable.path)
	}

	sstable.summary = sstable.dataSize + sstable.indexSize + HEADER_SIZE
}

func writeSummary(sstable *SSTable_Unique) {
	for i, key := range sstable.indexLeaders {
		recordByte := make([]byte, K_SIZE+len([]byte(key))+VALUE_SIZE_LEN)
		sstable.summarySize += uint64(len(recordByte))
		binary.LittleEndian.PutUint64(recordByte[0:K_SIZE], uint64(len([]byte(key))))
		copy(recordByte[K_SIZE:K_SIZE+len([]byte(key))], []byte(key))
		binary.LittleEndian.PutUint64(recordByte[K_SIZE+len([]byte(key)):], sstable.IndexIndexes[i])

		writeBlock(&recordByte, sstable.path)
	}
	sstable.bFPosition = sstable.summary + sstable.summarySize
}

func writeBloomFilter(sstable *SSTable_Unique) {
	recordByte := make([]byte, M_SIZE+len(sstable.bF.Data))
	binary.LittleEndian.PutUint64(recordByte[0:M_SIZE], uint64(sstable.bF.M))
	copy(recordByte[M_SIZE:], sstable.bF.Data)
	writeBlock(&recordByte, sstable.path)
	sstable.bFDataSize = uint64(len(sstable.bF.Data))
	writeHeader(sstable)
	for _, hashFunc := range sstable.bF.HashFunctions {
		recordByte := make([]byte, K_SIZE+len(hashFunc.Seed))
		binary.LittleEndian.PutUint64(recordByte[0:K_SIZE], uint64(len(hashFunc.Seed)))
		copy(recordByte[K_SIZE:], hashFunc.Seed)
		writeBlock(&recordByte, sstable.path)
	}
}

func NewSSTable(data *[]memTable.MemTableEntry, level int) {
	var sstable SSTable_Unique
	sstable.unixTime = time.Now().UnixNano()
	sstable.path = "data/sstable/file_" + fmt.Sprint(sstable.unixTime) + "_" + fmt.Sprint(level) + ".db"
	file, err := os.Create(sstable.path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	sstable.dataSize = 0
	sstable.indexSize = 0
	sstable.summarySize = 0
	sstable.bFDataSize = 0
	writeHeader(&sstable)

	sstable.bF = *bloom_filter.NewBloomFilterUnique(len(*data), FALSE_POSITIVE_RATE)
	writeSSTable(data, &sstable)
}

// dz3
func writeSSTable_DZ3(data *[]memTable.MemTableEntry, sstable *SSTable_Unique) {
	block_size := 2

	for i, node := range *data {
		// Check if tombstone is true, if true skip the entry
		if node.GetTombstone() == 1 {
			continue
		}

		in := append([]byte(node.GetKey()), node.GetValue()...)
		sstable.merkleData = append(sstable.merkleData, in)
		in = nil

		sstable.bF.Add(([]byte(node.GetKey())))
		if i%int(block_size) == 0 {
			sstable.blockLeaders = append(sstable.blockLeaders, node.GetKey())
			sstable.blockIndexes = append(sstable.blockIndexes, sstable.dataSize+HEADER_SIZE)
		}

		recordByte := make([]byte, KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN+TOMBSTONE_LEN)

		binary.LittleEndian.PutUint64(recordByte[0:KEY_SIZE_LEN], uint64(len([]byte(node.GetKey()))))
		binary.LittleEndian.PutUint64(recordByte[KEY_SIZE_LEN:KEY_SIZE_LEN+VALUE_SIZE_LEN], uint64(len(node.GetValue())))
		binary.LittleEndian.PutUint64(recordByte[KEY_SIZE_LEN+VALUE_SIZE_LEN:KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN], uint64(node.GetTimeStamp()))
		recordByte[KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN] = byte(node.GetTombstone())

		copy(recordByte[KEY_VALUE_START:KEY_VALUE_START+len([]byte(node.GetKey()))], []byte(node.GetKey()))
		copy(recordByte[KEY_VALUE_START+len([]byte(node.GetKey())):KEY_VALUE_START+len([]byte(node.GetKey()))+len(node.GetValue())], node.GetValue())

		writeBlock(&recordByte, sstable.path)
	}

	writeIndex(sstable)
	writeHeader(sstable)
	writeSummary(sstable)
	writeBloomFilter(sstable)
	merkletree.BuildMerkleTree(sstable.merkleData, sstable.unixTime)
}
