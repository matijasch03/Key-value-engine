package countMinSketch

import (
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"projekat_nasp/util"
)

type CountMinSketch struct {
	K             uint           // number of hash functions (rows)
	M             uint           // number of columns
	Counters      [][]uint       // matrix with cells
	HashFunctions []HashWithSeed //k hash functions
}

func NewCountMinSketch(precision float64, safety float64) *CountMinSketch {
	delta := 1 - safety
	k := CalculateK(delta)     // rows
	m := CalculateM(precision) // columns

	matrix := make([][]uint, k)
	for i := range matrix {
		matrix[i] = make([]uint, m)
		for j := 0; j < int(m); j++ {
			matrix[i][j] = 0
		}
	}

	return &CountMinSketch{
		K:             k,
		M:             m,
		Counters:      matrix,
		HashFunctions: CreateHashFunctions(k),
	}
}

func (cms *CountMinSketch) Delete() {
	cms.K = 0
	cms.M = 0
	cms.Counters = nil
	cms.HashFunctions = nil
}

func (cms *CountMinSketch) Print() {
	for i := 0; i < int(cms.K); i++ {
		for j := 0; j < int(cms.M); j++ {
			fmt.Print(cms.Counters[i][j], " ")
		}
		fmt.Println()
	}
}

// (i, j) - coordinates in cms matrix
func (cms *CountMinSketch) AddKey(key string) {
	for i, hashFunction := range cms.HashFunctions {
		hashValue := hashFunction.Hash([]byte(key))
		j := hashValue % uint64(cms.M)
		cms.Counters[i][j]++
	}
}

func (cms *CountMinSketch) FindKeyFrequency(key string) uint {
	minFreqValue := uint(math.MaxUint64)
	for i, hashFunction := range cms.HashFunctions {
		hashValue := hashFunction.Hash([]byte(key))
		j := hashValue % uint64(cms.M)

		if minFreqValue > cms.Counters[i][j] {
			minFreqValue = cms.Counters[i][j]
		}
	}
	return minFreqValue
}

func WriteGob(filePath string, object interface{}) error {
	file, err := os.Create(filePath)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(object)
	}
	file.Close()
	return err
}

func ReadGob(filePath string, object interface{}) error {
	file, err := os.Open(filePath)
	if err == nil {
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(object)
	}
	file.Close()
	return err
}

/*
-test function for cms that 1000 times adds random different keys and 1000 times the same key
-serialize to test.gob file
-print the frequency of test object, which should be near 1000
-deserialize the file and print its content to check the correctness of the serialization
*/
func RunExample() {
	cms := NewCountMinSketch(0.05, 0.99)

	test := util.RandomString(12, 0)
	for i := 0; i < 1000; i++ {
		a := util.RandomString(16, i)
		cms.AddKey(a)
		cms.AddKey(test)
	}
	cms.Print()
	fmt.Println(cms.FindKeyFrequency(test))

	err := WriteGob("./test.gob", cms)
	if err != nil {
		fmt.Println(err)
	}

	var newCms = new(CountMinSketch)
	err = ReadGob("./test.gob", newCms)
	if err != nil {
		fmt.Println(err)
	} else {
		newCms.Print()
	}
}
