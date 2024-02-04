package main

import (
	"fmt"
	"math/rand"
	"time"
)

func Test_DZ3() {
	keyList := generateKeyList(100)

}

func generateKeyList(numKeys int) []string {
	keys := make([]string, numKeys)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}
