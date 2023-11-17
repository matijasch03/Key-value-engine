package util

import (
	"math/rand"
	"time"
)

func RandomString(length int, seed int) string { // pomocna funkcija koja generise random string duzine length, seed je potreban da bi radilo lepo u for-u
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	source := rand.NewSource(time.Now().UnixNano() + int64(seed)) // posto se random pravi preko time.Now() u petlji nece lepo raditi pa zato dodaje broj
	rng := rand.New(source)

	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rng.Intn(len(charset))]
	}
	return string(result)
}

/* ###################################### koristiti ovako nekako, dodas i u parametar da bi pravio random u for-u
for i := 0; i < 1000000; i++ {
		a:= util.RandomString(16, i) // pravi random string i stavlja u a, korisno da testiras svoju strukturu (npr ja sam za svoje koristio hll.Add(util.RandomString(16, i)))
	}
*/
