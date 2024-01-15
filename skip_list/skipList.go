package skip_list

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
		Head:      NewSkipListNode("-inf", []byte("-1"), maxHeight),
		rand:      rand.New(source),
	}
	return &skipList

}

type SkipListNode struct {
	key   string
	value []byte
	next  []*SkipListNode
}

func NewSkipListNode(key string, value []byte, level int) *SkipListNode {
	skipListNode := SkipListNode{
		key:   key,
		value: value,
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
func (s *SkipList) InsertElement(key string, value []byte) {
	found := s.SearchElement(key)
	if found != nil {
		s.UpdateElement(key, value)
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

}

func (s *SkipList) SearchElement(key string) []byte {
	current := s.Head

	for i := s.maxHeight; i != -1; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}

	}

	current = current.next[0]
	if current != nil {
		if current.key == key {
			return current.value
		}
	}
	return nil

}
func (s *SkipList) UpdateElement(key string, newValue []byte) {
	current := s.Head

	for i := s.maxHeight; i != -1; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}

	}

	current = current.next[0]
	if current != nil {
		if current.key == key {
			current.value = newValue
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