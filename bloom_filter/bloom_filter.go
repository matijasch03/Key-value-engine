package bloom_filter

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
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

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (b *BloomFilter) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	// Write size to the buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(b.size)); err != nil {
		return nil, err
	}

	// Write bits to the buffer
	for _, bit := range b.bits {
		if err := binary.Write(&buf, binary.BigEndian, bit); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (b *BloomFilter) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)

	// Read size from the buffer
	var size int32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return err
	}
	b.size = int(size)

	// Read bits from the buffer
	b.bits = make([]bool, b.size)
	for i := 0; i < b.size; i++ {
		var bit bool
		if err := binary.Read(buf, binary.BigEndian, &bit); err != nil {
			return err
		}
		b.bits[i] = bit
	}

	return nil
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
