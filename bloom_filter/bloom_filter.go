package bloomfilter

import (
	"hash/fnv"
	"encoding/json"
	"os"
	"io"
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
// Serijalizacija BloomFilter-a u bajt niz koristeći JSON
func (b *BloomFilter) Serialize() ([]byte, error) {
	jsonData, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	return jsonData, nil
}

// Deserijalizacija BloomFilter-a iz bajt niza koristeći JSON
func Deserialize(data []byte) (*BloomFilter, error) {
	var bloom BloomFilter
	err := json.Unmarshal(data, &bloom)
	if err != nil {
		return nil, err
	}
	return &bloom, nil
}

// Čuvanje BloomFilter-a u datoteku koristeći JSON
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

// Učitavanje BloomFilter-a iz datoteke koristeći JSON
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
