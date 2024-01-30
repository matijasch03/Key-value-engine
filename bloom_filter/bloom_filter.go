package bloomfilter

import (
	"hash/fnv"
	"bytes"
	"encoding/gob"
	"os"
)

type BloomFilter struct {
	size     int
	hashFunc []func(string) int
	bits     []bool
}

func NewBloomFilter(size int, numHashFuncs int) *BloomFilter {
	bloom := BloomFilter{
		size:     size,
		hashFunc: make([]func(string) int, numHashFuncs),
		bits:     make([]bool, size),
	}

	for i := 0; i < numHashFuncs; i++ {
		bloom.hashFunc[i] = createHashFunc(i)
	}

	return &bloom
}

func (b *BloomFilter) Add(value string) {
	for _, hashFunc := range b.hashFunc {
		index := hashFunc(value) % b.size
		b.bits[index] = true
	}
}

func (b *BloomFilter) Contains(value string) bool {
	for _, hashFunc := range b.hashFunc {
		index := hashFunc(value) % b.size
		if !b.bits[index] {
			return false
		}
	}
	return true
}

func createHashFunc(seed int) func(string) int {
	return func(data string) int {
		hash := fnv.New32a()
		hash.Write([]byte(data))
		return int(hash.Sum32() ^ uint32(seed))
	}
}
// Serijalizacija BloomFilter-a u bajt niz
func (b *BloomFilter) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(b)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserijalizacija BloomFilter-a iz bajt niza
func Deserialize(data []byte) (*BloomFilter, error) {
	var bloom BloomFilter
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	err := decoder.Decode(&bloom)
	if err != nil {
		return nil, err
	}
	return &bloom, nil
}

// Čuvanje BloomFilter-a u datoteku
func (b *BloomFilter) SaveToFile(filename string) error {
	data, err := b.Serialize()
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// Učitavanje BloomFilter-a iz datoteke
func LoadFromFile(filename string) (*BloomFilter, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return Deserialize(data)
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
