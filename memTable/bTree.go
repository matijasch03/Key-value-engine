package memTable

import (
	"fmt"
	"slices"
)

type bTree struct {
	root  *bTreeNode
	order uint8
}

type bTreeNode struct {
	keys     []string
	values   []*MemTableEntry
	parent   *bTreeNode
	children []*bTreeNode
}

func InitBTree(order uint8) bTree {
	keys := make([]string, 0, order)
	values := make([]*MemTableEntry, 0, order)
	children := make([]*bTreeNode, 0, order+1)
	root := bTreeNode{
		keys,
		values,
		nil,
		children,
	}
	tree := bTree{
		&root,
		order,
	}
	return tree
}

func (tree *bTree) NewNode(value MemTableEntry) bTreeNode {
	key := value.key
	keys := make([]string, 1, tree.order)
	keys[0] = key
	values := make([]*MemTableEntry, 1, tree.order)
	values[0] = &value
	children := make([]*bTreeNode, 1, tree.order+1)
	node := bTreeNode{
		keys,
		values,
		nil,
		children,
	}
	return node
}

func (tree *bTree) EmptyNode() bTreeNode {
	keys := make([]string, 0, tree.order)
	values := make([]*MemTableEntry, 0, tree.order)
	children := make([]*bTreeNode, 0, tree.order+1)
	node := bTreeNode{
		keys,
		values,
		nil,
		children,
	}
	return node
}

func (tree *bTree) Find(key string) *MemTableEntry {
	current := tree.root
	for true {
		if len(current.children) == 0 {
			break
		}
		for i := 0; i < len(current.keys); i++ {

			if key == current.keys[i] {
				return current.values[i]
			}
			if key < current.keys[i] {
				current = current.children[i]
				break
			}
			if i == len(current.keys)-1 {
				current = current.children[i+1]
				break
			}
		}
	}
	for i := 0; i < len(current.keys); i++ {
		if key == current.keys[i] {
			return current.values[i]
		}
	}
	return nil
}

func (tree *bTree) Insert(value MemTableEntry) bool {
	key := value.key
	current := tree.root

	for true {
		if len(current.children) == 0 {
			break
		}
		for i := 0; i < len(current.keys); i++ {
			if key == current.keys[i] {
				current.values[i] = &value
				return false
			}
			if key < current.keys[i] {
				current = current.children[i]
				break
			}
			if i == len(current.keys)-1 {
				current = current.children[i+1]
				break
			}
		}
	}
	if len(current.keys) == 0 {
		current.keys = append(current.keys, key)
		current.values = append(current.values, &value)
		return true
	}

	for i := 0; i < len(current.keys); i++ {
		if key == current.keys[i] {
			current.values[i] = &value
			break
		}
		if key < current.keys[i] {
			current.keys = slices.Insert(current.keys, i, key)
			current.values = slices.Insert(current.values, i, &value)

			break
		}
		if i == len(current.keys)-1 {
			fmt.Println()
			current.keys = append(current.keys, key)
			current.values = append(current.values, &value)
			break
		}
	}
	if len(current.keys) == int(tree.order) {
		tree.overflow(current)
	}
	return true

}

func (tree *bTree) PrintTree() {
	current := tree.root
	tree.printNode(current, 0)
	fmt.Println()
}

func (tree *bTree) printNode(node *bTreeNode, level int) {
	fmt.Printf("Level %d (", level)
	for i := 0; i < len(node.keys); i++ {
		fmt.Print(node.keys[i])
		if i != len(node.keys)-1 {
			fmt.Print(" | ")
		}
	}
	fmt.Print("  ")
	fmt.Print(node.parent)
	fmt.Print(") ")
	fmt.Println()
	for i := 0; i < len(node.children); i++ {
		tree.printNode(node.children[i], level+1)
	}
}

func (tree *bTree) overflow(current *bTreeNode) {
	parent := current.parent
	if parent == nil {
		tree.split(current)
		return
	}
	index := slices.Index(parent.children, current)

	if index != 0 {
		sibling := parent.children[index-1]
		if len(sibling.keys) < int(tree.order-1) {
			sibling.keys = append(sibling.keys, parent.keys[index-1])
			sibling.values = append(sibling.values, parent.values[index-1])

			if len(current.children) != 0 {
				child := current.children[0]
				child.parent = sibling
				current.children = slices.Delete(current.children, 0, 1)
				sibling.children = append(sibling.children, child)

			}
			parent.keys[index-1] = current.keys[0]
			parent.values[index-1] = current.values[0]

			current.keys = slices.Delete(current.keys, 0, 1)
			current.values = slices.Delete(current.values, 0, 1)

			return
		}
	}

	if index != len(parent.children)-1 {
		sibling := parent.children[index+1]
		if len(sibling.keys) < int(tree.order-1) {
			sibling.keys = slices.Insert(sibling.keys, 0, parent.keys[index])
			sibling.values = slices.Insert(sibling.values, 0, parent.values[index])

			if len(current.children) != 0 {
				child := current.children[len(current.children)-1]
				child.parent = sibling
				current.children = slices.Delete(current.children, len(current.children)-1, len(current.children))
				sibling.children = slices.Insert(sibling.children, 0, child)

			}

			parent.keys[index] = current.keys[len(current.keys)-1]
			parent.values[index] = current.values[len(current.values)-1]

			current.keys = slices.Delete(current.keys, len(current.keys)-1, len(current.keys))
			current.values = slices.Delete(current.values, len(current.values)-1, len(current.values))
			return
		}
	}

	if len(current.keys) == int(tree.order) {
		tree.split(current)
	}
}

func (tree *bTree) split(current *bTreeNode) {
	half := len(current.keys) / 2
	middleKey := current.keys[half]
	middleValue := current.values[half]
	parent := current.parent
	if parent == nil {
		empty := tree.EmptyNode()
		current.parent = &empty
		empty.children = append(empty.children, current)
		parent = &empty
		tree.root = parent
	}
	node := tree.EmptyNode()
	node.keys = make([]string, int(tree.order)-half-1, tree.order)
	copy(node.keys, current.keys[half+1:])
	node.values = make([]*MemTableEntry, int(tree.order)-half-1, tree.order)
	copy(node.values, current.values[half+1:])
	//node.keys = current.keys[half+1:]
	//node.values = current.values[half+1:]

	if len(current.children) >= int(tree.order) {
		node.children = make([]*bTreeNode, int(tree.order)-half, tree.order+1)
		copy(node.children, current.children[half+1:])
		//node.children = current.children[half+1:]
		for i := 0; i < len(node.children); i++ {
			child := node.children[i]
			child.parent = &node
		}
		current.children = current.children[:half+1]
	}

	node.parent = parent
	current.keys = current.keys[:half]
	current.values = current.values[:half]

	if len(parent.keys) == 0 {
		parent.keys = append(parent.keys, middleKey)
		parent.values = append(parent.values, middleValue)
		parent.children = append(parent.children, &node)
	} else {
		for i := 0; i < len(parent.keys); i++ {
			if middleKey < parent.keys[i] {
				parent.keys = slices.Insert(parent.keys, i, middleKey)
				parent.values = slices.Insert(parent.values, i, middleValue)
				parent.children = slices.Insert(parent.children, i+1, &node)
				break
			}
			if i == len(parent.keys)-1 {
				parent.keys = append(parent.keys, middleKey)
				parent.values = append(parent.values, middleValue)
				parent.children = append(parent.children, &node)

				break
			}
		}
	}

	if len(parent.keys) == int(tree.order) {
		tree.overflow(parent)
	}
}

func (tree *bTree) SortTree() []MemTableEntry {
	var entries []MemTableEntry
	node := tree.root
	for i := 0; i < len(node.keys); i++ {
		if len(node.children) != 0 {
			entries = append(entries, tree.Sort(*node.children[i])...)
		}
		entries = append(entries, *node.values[i])
	}
	if len(node.children) != 0 {
		entries = append(entries, tree.Sort(*node.children[len(node.children)-1])...)
	}
	return entries
}

func (tree *bTree) Sort(node bTreeNode) []MemTableEntry {
	var entries []MemTableEntry
	for i := 0; i < len(node.keys); i++ {
		if len(node.children) != 0 {
			entries = append(entries, tree.Sort(*node.children[i])...)
		}
		entries = append(entries, *node.values[i])
	}
	if len(node.children) != 0 {
		entries = append(entries, tree.Sort(*node.children[len(node.children)-1])...)
	}
	return entries
}

/*
	parent := current.parent
	if parent == nil {
		tree.split(current)
		return
	}
	for i := 0; i < len(parent.children); i++ {
		sibling := parent.children[i]

		if len(sibling.keys) < int(tree.order)-1 {

			if len(current.children) != 0 && i == slices.Index(parent.children, current)+1 {
				child := current.children[len(current.children)-1]
				child.parent = sibling
				current.children = slices.Delete(current.children, len(current.children)-1, len(current.children))
				sibling.children = slices.Insert(sibling.children, 0, child)
			} else if len(current.children) != 0 && i == slices.Index(parent.children, current)-1 {
				child := current.children[0]
				child.parent = sibling
				current.children = slices.Delete(current.children, 0, 1)
				sibling.children = append(sibling.children, child)
			}

			if slices.Index(parent.children, current) == 0 {
				if i == len(parent.children)-1 {
					tree.PrintTree()
					sibling.keys = slices.Insert(sibling.keys, 0, parent.keys[i-1])
					sibling.values = slices.Insert(sibling.values, 0, parent.values[i-1])

					siblingLast := parent.children[len(parent.children)-2]
					parent.keys[i-1] = siblingLast.keys[len(siblingLast.keys)-1]
					parent.values[i-1] = siblingLast.values[len(siblingLast.values)-1]
					siblingLast.keys = slices.Delete(siblingLast.keys, len(siblingLast.keys)-1, len(siblingLast.keys))
					siblingLast.values = slices.Delete(siblingLast.values, len(siblingLast.values)-1, len(siblingLast.values))
					if current != siblingLast {
						fmt.Println("upsi")

						tree.overflow(current)
					}
					break
				} else {
					sibling.keys = slices.Insert(sibling.keys, 0, parent.keys[0])
					sibling.values = slices.Insert(sibling.values, 0, parent.values[0])
					parent.keys[0] = current.keys[len(current.keys)-1]
					parent.values[0] = current.values[len(current.values)-1]
					current.keys = slices.Delete(current.keys, len(current.keys)-1, len(current.keys))
					current.values = slices.Delete(current.values, len(current.values)-1, len(current.values))
					break
				}
			} else {
				if i == 0 {
					sibling.keys = append(sibling.keys, parent.keys[0])
					sibling.values = append(sibling.values, parent.values[0])
					parent.keys[0] = current.keys[0]
					parent.values[0] = current.values[0]
					current.keys = slices.Delete(current.keys, 0, 1)
					current.values = slices.Delete(current.values, 0, 1)
					break
				} else {

					if i == len(parent.children)-1 {
						fmt.Println("aa")
						sibling.keys = slices.Insert(sibling.keys, 0, parent.keys[i-1])
						sibling.values = slices.Insert(sibling.values, 0, parent.values[i-1])
						siblingLast := parent.children[len(parent.children)-2]
						parent.keys[i-1] = siblingLast.keys[len(siblingLast.keys)-1]
						parent.values[i-1] = siblingLast.values[len(siblingLast.values)-1]
						siblingLast.keys = slices.Delete(siblingLast.keys, len(siblingLast.keys)-1, len(siblingLast.keys))
						siblingLast.values = slices.Delete(siblingLast.values, len(siblingLast.values)-1, len(siblingLast.values))
						if current != siblingLast {
							tree.overflow(current)
						}
						break
					} else {
						sibling.keys = slices.Insert(sibling.keys, 0, parent.keys[i-1])
						sibling.values = slices.Insert(sibling.values, 0, parent.values[i-1])
						parent.keys[i-1] = parent.keys[i]
						parent.values[i-1] = parent.values[i]
						parent.keys[i] = current.keys[0]
						parent.values[i] = current.values[0]
						current.keys = slices.Delete(current.keys, 0, 1)
						current.values = slices.Delete(current.values, 0, 1)
						break
					}

				}
			}
		}
	}
*/
