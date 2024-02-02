package bloom_filter

import (
	"bytes"
	"encoding/gob"
	"math"
	"crypto/md5"
	"encoding/binary"
	"time"
)
type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}
type BloomFilterUnique struct {
	M             uint
	Data          []byte
	HashFunctions []HashWithSeed
}
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}
// Konstruktor za bloomfilter
// expectedElements -> ocekivani broj elemenata
// falsePositiveRate -> tolerancija na gresku
func NewBloomFilterUnique(expectedElements int, falsePositiveRate float64) *BloomFilterUnique {
	m := CalculateM(expectedElements, falsePositiveRate) // broj bitova
	k := CalculateK(expectedElements, m)                 // broj hash funkcija

	hashFunctions := CreateHashFunctions(k) // hash funkcije
	bytesNum := math.Ceil(float64(m) / 8)   // broj bajtova
	data := make([]byte, int(bytesNum))     // niz velicine m

	b := BloomFilterUnique{m, data, hashFunctions}

	return &b
}

// Dodavanje elementa u bloomfilter
// data -> element za dodavanje
func (b BloomFilterUnique) Add(data []byte) {
	for _, hashFunction := range b.HashFunctions {
		hashed := hashFunction.Hash(data)
		bit := hashed % uint64(b.M) // bit u nizu

		targetByte := bit / 8     // bajt u kome se bit nalazi
		bitMask := 1 << (bit % 8) // maska
		index := int(targetByte)
		b.Data[index] = b.Data[index] | byte(bitMask) // bitwise OR kako bi upisali jedinicu
	}
}

// Citanje elementa
// data -> element za citanje
func (b BloomFilterUnique) Read(data []byte) bool {
	for _, hashFunction := range b.HashFunctions {
		// Isto kao kod pisanja
		hashed := hashFunction.Hash(data)
		bit := hashed % uint64(b.M)

		targetByte := bit / 8
		bitMask := 1 << (bit % 8)
		index := int(targetByte)

		// bitwise AND kako bi proverili da li je bit na datoj poziciji
		if b.Data[index]&byte(bitMask) == 0 {
			return false
		}
	}

	return true
}

func (b BloomFilterUnique) Save() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(b)

	return buffer.Bytes()
}

func Load(data []byte) *BloomFilterUnique {
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)

	b := &BloomFilterUnique{}
	err := decoder.Decode(b)
	if err != nil {
		panic("error while decoding")
	}

	return b
}