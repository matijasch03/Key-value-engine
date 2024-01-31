package bloom_filter

import (
	"bytes"
	"encoding/gob"
	"hash/fnv"
	"io"
	"os"
)

type BloomFilter struct {
	Size         int // promenjeno Size
	NumHashFuncs int
	HashSeeds    []int
	Bits         []bool
}

func NewBloomFilter(size int, numHashFuncs int) *BloomFilter {
	bloom := BloomFilter{
		Size:         size,
		NumHashFuncs: numHashFuncs,
		HashSeeds:    make([]int, numHashFuncs),
		Bits:         make([]bool, size),
	}

	for i := 0; i < numHashFuncs; i++ {
		bloom.HashSeeds[i] = i
	}

	return &bloom
}

func (b *BloomFilter) createHashFunc(seed int) func(string) int {
	return func(data string) int {
		hash := fnv.New32a()
		hash.Write([]byte(data))
		return int(hash.Sum32() ^ uint32(seed))
	}
}

func (b *BloomFilter) Add(value string) {
	for _, seed := range b.HashSeeds {
		index := b.createHashFunc(seed)(value) % b.Size
		b.Bits[index] = true
	}
}

func (b *BloomFilter) Contains(value string) bool {
	for _, seed := range b.HashSeeds {
		index := b.createHashFunc(seed)(value) % b.Size
		if !b.Bits[index] {
			return false
		}
	}
	return true
}

// Serijalizacija BloomFilter-a u bajt niz koristeći gob
func (b *BloomFilter) Serialize() ([]byte, error) {
	var data bytes.Buffer
	encoder := gob.NewEncoder(&data)

	err := encoder.Encode(b)
	if err != nil {
		return nil, err
	}

	return data.Bytes(), nil
}

// Deserijalizacija BloomFilter-a iz bajt niza koristeći gob
func Deserialize(data []byte) (*BloomFilter, error) {
	var bloom BloomFilter
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&bloom)
	if err != nil {
		return nil, err
	}

	return &bloom, nil
}

// Čuvanje BloomFilter-a u datoteku koristeći gob
func (b *BloomFilter) SaveToFile(filename string) error {
	data, err := b.Serialize()
	if err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

// Učitavanje BloomFilter-a iz datoteke koristeći gob
func LoadFromFile(filename string) (*BloomFilter, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return Deserialize(data)
}

/*
func main() {
	bloomFilter := NewBloomFilter(100, 3)
	bloomFilter.Add("element1")
	bloomFilter.Add("element2")
	bloomFilter.Add("element3")

	// Čuvanje BloomFilter-a u datoteku
	err := bloomFilter.SaveToFile("data.gob")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("BloomFilter je uspešno sačuvan u datoteku.")
}

func ReadBloomFilter(filename string) (bf *BloomFilter) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var size int32
	if err := binary.Read(file, binary.BigEndian, &size); err != nil {
		panic(err)
	}
	defer file.Close()

	bf = NewBloomFilter(int(size), len(bf.hashFunc))

	for i := 0; i < bf.size; i++ {
		var bit bool
		if err := binary.Read(file, binary.BigEndian, &bit); err != nil {
			panic(err)
		}
		bf.bits[i] = bit
	}

	return bf
}

/*func main() {
	size := 20
	numHashFuncs := 3

	bloomFilter := NewBloomFilter(size, numHashFuncs)

	valuesToAdd := []string{"apple", "banana", "cherry"}
	for _, value := range valuesToAdd {
		bloomFilter.Add(value)
	}

	valuesToCheck := []string{"apple", "orange", "banana"}
	for _, value := range valuesToCheck {
		if bloomFilter.Contains(value) {
			fmt.Printf("%s may be in the set.\n", value)
		} else {
			fmt.Printf("%s is definitely not in the set.\n", value)
		}
	}
}*/
