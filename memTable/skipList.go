package memTable

import (
	"fmt"
	"math/rand"
	"time"
)

type SkipList struct {
	maxHeight int
	height    int
	Head      *SkipListNode
	rand      *rand.Rand
}

func NewSkipList(maxHeight int) *SkipList {
	source := rand.NewSource(time.Now().UnixNano())
	skipList := SkipList{
		maxHeight: maxHeight,
		height:    0,
		Head:      NewSkipListNode("-inf", MemTableEntry{}, maxHeight),
		rand:      rand.New(source),
	}
	return &skipList

}

type SkipListNode struct {
	key   string
	value *MemTableEntry
	next  []*SkipListNode
}

func NewSkipListNode(key string, value MemTableEntry, level int) *SkipListNode {
	skipListNode := SkipListNode{
		key:   key,
		value: &value,
		next:  make([]*SkipListNode, level+1),
	}
	return &skipListNode
}

func (s *SkipList) roll() int {
	level := 0 // alwasy start from level 0

	for rand.Int31n(2) == 1 {
		level++
		if level > s.maxHeight {
			level = s.maxHeight
			break
		}
	}
	return level
}
func (s *SkipList) InsertElement(key string, value MemTableEntry) bool {
	_, found := s.SearchElement(key)
	if found {
		s.UpdateElement(key, value)
		return false
	}
	update := make([]*SkipListNode, s.maxHeight+1)
	current := s.Head

	for i := s.maxHeight; i != -1; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]
	if current == nil || current.key != key {
		rlevel := s.roll()
		if rlevel > s.height {
			for i := s.height + 1; i < rlevel+1; i++ {
				update[i] = s.Head
			}
			s.height = rlevel
		}
		n := NewSkipListNode(key, value, rlevel)

		for i := 0; i < rlevel+1; i++ {
			n.next[i] = update[i].next[i]
			update[i].next[i] = n
		}
	}
	return true
}

func (s *SkipList) SearchElement(key string) (*MemTableEntry, bool) {
	current := s.Head

	for i := s.maxHeight; i != -1; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}

	}

	current = current.next[0]
	if current != nil {
		if current.key == key {
			return current.value, true
		}
	}
	return nil, false

}
func (s *SkipList) UpdateElement(key string, newValue MemTableEntry) {
	current := s.Head

	for i := s.maxHeight; i != -1; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}

	}

	current = current.next[0]
	if current != nil {
		if current.key == key {
			current.value = &newValue
		}
	}

}

func (s *SkipList) Display() {
	fmt.Println("skip lista")
	head := s.Head
	for i := 0; i != s.height+1; i++ {
		fmt.Print("Level ", i, " ")
		node := head.next[i]
		for node != nil {
			fmt.Print(node.key, " ")
			node = node.next[i]
		}
		fmt.Println("")
	}

}

func (s *SkipList) GetAll() []SkipListNode {
	node := s.Head.next[0]
	list := make([]SkipListNode, 0)
	for node != nil {
		list = append(list, *node)
		node = node.next[0]
	}
	return list
}

func (s *SkipList) Sort() []MemTableEntry {
	node := s.Head.next[0]
	list := make([]MemTableEntry, 0)
	for node != nil {
		list = append(list, *node.value)
		node = node.next[0]
	}
	return list
}
