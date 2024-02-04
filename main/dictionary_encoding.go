package main

import (
	"encoding/gob"
	"os"
)

// Compressor je struktura koja predstavlja mapu kompresije
type Compressor struct {
	Dictionary map[string]int
}

// NewCompressor kreira novi objekat Compressor
func NewCompressor() *Compressor {
	return &Compressor{
		Dictionary: make(map[string]int),
	}
}

// Compress kompresuje niz ključeva koristeći dictionary encoding
func (c *Compressor) Compress(keys []string) []int {
	var result []int

	for _, key := range keys {
		// Proveravamo da li ključ već postoji u rečniku
		if _, ok := c.Dictionary[key]; !ok {
			// Ako ne postoji, dodajemo ga u rečnik sa sledećim slobodnim brojem
			c.Dictionary[key] = len(c.Dictionary)
		}

		// Dodajemo numeričku vrednost ključa u rezultat
		result = append(result, c.Dictionary[key])
	}

	return result
}

// SaveToFile čuva kompresovanu mapu u fajl
func (c *Compressor) SaveToFile() error {
	filename:="data/globalCompressed.gob"
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(c.Dictionary)
	if err != nil {
		return err
	}

	return nil
}

// LoadFromFile učitava kompresovanu mapu iz fajla
func (c *Compressor) LoadFromFile() error {
	filename:="data/globalCompressed.gob"
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&c.Dictionary)
	if err != nil {
		return err
	}

	return nil
}
/*
func main() {
	// Primer korišćenja
	compressor := NewCompressor()

	// Niz ključeva koje želimo da kompresujemo
	keys := []string{"kljuc1", "kljuc2", "kljuc3", "kljuc1", "kljuc2"}

	// Kompresujemo niz ključeva
	compressed := compressor.Compress(keys)

	// Ispisujemo rezultat
	fmt.Println("Originalni ključevi:", keys)
	fmt.Println("Kompresovani ključevi:", compressed)

	// Čuvamo kompresovanu mapu u fajl
	if err := compressor.SaveToFile("compressed_map.gob"); err != nil {
		fmt.Println("Greška pri čuvanju u fajl:", err)
		return
	}

	// Kreiramo novi objekat Compressor za učitavanje iz fajla
	newCompressor := NewCompressor()

	// Učitavamo kompresovanu mapu iz fajla
	if err := newCompressor.LoadFromFile("compressed_map.gob"); err != nil {
		fmt.Println("Greška pri učitavanju iz fajla:", err)
		return
	}

	// Ispisujemo učitanu mapu
	fmt.Println("Učitana kompresovana mapa:", newCompressor.Dictionary)
}
*/