package cache

import (
	"container/list"
	"fmt"
	"projekat_nasp/util"
)

/*
Cache struct implemented by two structures - map and list
  - map - for fast searching elements by key (string)
  - list - for pushing new elements and moving frequently elements on the first places

LRU - algorithm used for this cache that moves the newest elements to the front of the list
*/
type Cache struct {
	MaxLength int
	Length    int
	MapItems  map[string]interface{}
	ListLRU   *list.List // double-linked list from imported library
}

func NewCache(maxLength int) *Cache {
	return &Cache{
		MaxLength: maxLength,
		Length:    0,
		MapItems:  make(map[string]interface{}),
		ListLRU:   list.New(),
	}
}

func (cache *Cache) AddItem(key string, value interface{}) {

	// 1. case: the object that should be added already exists in cache
	for k, _ := range cache.MapItems {
		if k == key {
			currentElement := findByValue(cache.ListLRU, value)
			cache.ListLRU.MoveToFront(currentElement)
			// cache.ListLRU.MoveToFront(value.(*list.Element))
			// unsuccessfully - string or some other type and *list.Element are different types
			return
		}
	}

	// 2. case: add new object (simply push front)
	if cache.Length == cache.MaxLength { // list is full
		lastElem := cache.ListLRU.Back()
		cache.ListLRU.Remove(lastElem)
		cache.Length--

		for k, v := range cache.MapItems {
			if v == lastElem.Value {
				delete(cache.MapItems, k)
			}
		}
	}

	cache.ListLRU.PushFront(value)
	cache.MapItems[key] = value
	cache.Length++
}

// helper function that finds an element in list by value and returns it
func findByValue(listLRU *list.List, value interface{}) *list.Element {
	for elem := listLRU.Front(); elem != nil; elem = elem.Next() {
		if elem.Value == value {
			return elem
		}
	}
	return nil
}

// iterate through the map and try to find by key
// return: (value, true) if exists, else (nil, false)
func (cache *Cache) GetByKey(key string) (bool, interface{}) {
	for k := range cache.MapItems {
		if k == key {
			// each read element should be put on the start as the newest
			movingElem := findByValue(cache.ListLRU, cache.MapItems[key])
			cache.ListLRU.MoveToFront(movingElem)
			return true, cache.MapItems[key]
		}
	}
	return false, nil
}

// print elements from the cache list from the newest to the oldest
func (cache *Cache) Print() {
	for elem := cache.ListLRU.Front(); elem != nil; elem = elem.Next() {
		fmt.Print(elem.Value, " ")
	}
	fmt.Println()
}

func TestCache() {
	cache := NewCache(10)
	for i := 0; i < 40; i++ {
		key := util.RandomString(1, i)
		fmt.Println("Element to add:", key)

		exist := false
		if exist, _ = cache.GetByKey(key); exist {
			fmt.Println("Element", cache.MapItems[key], "already exists.")
		}
		if !exist {
			cache.AddItem(key, key)
		}

		fmt.Print("Cache: ")
		cache.Print()
		fmt.Println("Length:", cache.Length, "\n")
	}
}
