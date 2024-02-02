package hyperloglog

import (
	"bytes"
	"encoding/gob"
	"hash"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
)

type HLL struct {
	p        uint8       // preciznost
	m        uint64      // duzina seta
	b64      bool        // da li da koristimo 64 vrednost za hes ili ne (32 ako ne)
	hasher32 hash.Hash32 // za koriscenje hash funkcija
	hasher64 hash.Hash64
	set      []uint8 // slice gde cuvamo najvece ponavljanje nula
}

func InitHLL(p uint8, b64 bool) HLL { // "konstruktor" za HLL, b64 je da li zelimo da koristimo 64-obitno hesiranje ili ne (32 ako ne)
	m := uint64(math.Pow(2, float64(p))) // m = 2^p
	set := make([]uint8, m, m)
	hll := HLL{
		p,
		m,
		b64,
		fnv.New32a(),
		fnv.New64a(),
		set,
	}
	return hll
}

func (hll *HLL) Add(key string) {
	var bucket uint64
	var nule uint8

	if hll.b64 {
		hll.hasher64.Write([]byte(key))
		hes := hll.hasher64.Sum64()                 // dobijanje hes vrednosti
		hll.hasher64.Reset()                        // resetujemo heser da bi mogli opet da ga koristimo
		bucket = uint64(hes >> (64 - hll.p))        // shiftovanjem u desno ostaju samo prvi bitovi
		nule = 1 + uint8(bits.TrailingZeros64(hes)) // funkcija broji koliko nula ima na kraju
		nule = min((64 - hll.p), nule)              // posto cifre koje definisu bucket ne treba da gledamo,ako dodje do toga da je niz nula toliko velik da dodje do njih znaci da je broj nula 64 - p

	} else {
		hll.hasher32.Write([]byte(key)) // sve isto samo za 32 bita
		hes := hll.hasher32.Sum32()
		hll.hasher32.Reset()
		bucket = uint64(hes >> (32 - hll.p))
		nule = 1 + uint8(bits.TrailingZeros32(hes))
		nule = min((32 - hll.p), nule)
	}

	if nule > hll.set[bucket] { // ako je veci niz nula nego sto je zabelezeno, zameni
		hll.set[bucket] = nule
	}
}

func (hll *HLL) Prebroj() float64 { // vraca aproksimaciju kardinalnosti koristeci formule
	suma := 0.0
	prazni := 0

	k := 0.7213 / (1.0 + 1.079/float64(hll.m))

	for _, val := range hll.set {
		suma += math.Pow(math.Pow(2.0, float64(val)), -1)
		if val == 0 { //brojimo prazne ukoliko bude bilo potrebno (koristi se u formuli ako je kardinalnost mala)
			prazni++
		}
	}

	broj := k * math.Pow(float64(hll.m), 2.0) / suma

	if broj <= 2.5*float64(hll.m) { // za male vrednosti umesto formule za hyperloglog koristi se formula za linear counting
		if prazni > 0 {
			broj = float64(hll.m) * math.Log(float64(hll.m)/float64(prazni))
		}
	} else if broj > 1/30.0*math.Pow(2.0, 32.0) { // za bas velike vrednosti ova formula "popravlja" rezultat
		broj = -math.Pow(2.0, 32.0) * math.Log(1.0-broj/math.Pow(2.0, 32.0))
	}

	return broj
}

func (hll *HLL) SacuvajHLL(putanja string) {
	file, err := os.OpenFile(putanja, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err.Error())
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(hll)
}

func UcitajHLL(putanja string) HLL {
	file, err := os.OpenFile(putanja, os.O_RDONLY, 0666)
	if err != nil {
		panic(err.Error())
	}
	decoder := gob.NewDecoder(file)
	var hll = new(HLL)
	file.Seek(0, 0)

	err = decoder.Decode(hll)

	hll.hasher32 = fnv.New32a()
	hll.hasher64 = fnv.New64a()

	return *hll
}

func (hll HLL) GobEncode() ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(hll.m)
	err = enc.Encode(hll.p)
	err = enc.Encode(hll.b64)
	err = enc.Encode(hll.set)

	return b.Bytes(), err
}

func (hll *HLL) GobDecode(data []byte) error {
	b := bytes.NewBuffer(data)
	enc := gob.NewDecoder(b)
	err := enc.Decode(&hll.m)
	err = enc.Decode(&hll.p)
	err = enc.Decode(&hll.b64)
	err = enc.Decode(&hll.set)

	return err
}
