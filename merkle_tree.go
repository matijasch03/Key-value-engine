package merkletree

import (
	"crypto/sha1"
	"encoding/hex"
	"os"
	"strconv"
)

type MerkleRoot struct {
	Root *NodeMerkle //top of the tree
}

func (mr *MerkleRoot) String() string { //returns the hexadecimal representation of the byte-value of the root node
	return mr.Root.String()
}

type NodeMerkle struct { // node in the tree
	value []byte
	left  *NodeMerkle //pointer to the left child
	right *NodeMerkle //pointer to the right child
}

func (n *NodeMerkle) String() string { //returns the hexadecimal representation of the node's byte-value
	return hex.EncodeToString(n.value[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func NewMerkleTree(parts []NodeMerkle) *MerkleRoot {
	elems := MakeNodes(parts)
	return &MerkleRoot{Root: &elems[0]}
}

func MakeNodes(parts []NodeMerkle) []NodeMerkle { //parts - list of nodes
	next_gen := []NodeMerkle{} //for parents
	if len(parts)%2 == 1 {
		parts = append(parts, NodeMerkle{value: []byte("")}) //add one more, because the number of nodes must be even
	}
	counter := 0
	for len(parts) > counter {
		currentParents := parts[counter : counter+2]
		left := currentParents[0]
		right := currentParents[1]
		childrenVal := append(left.value[:], right.value[:]...)
		hashVal := Hash(childrenVal)
		if len(right.value) == 0 {
			next_gen = append(next_gen, NodeMerkle{value: hashVal[:], left: &left, right: nil})
		} else {
			next_gen = append(next_gen, NodeMerkle{value: hashVal[:], left: &left, right: &right})
		}
		counter += 2
	}
	if len(next_gen) == 1 {
		return next_gen
	} else {
		return MakeNodes(next_gen) //until the root is obtained
	}
}

func (n *NodeMerkle) WriteToFile(level int, index int) {
	file, err := os.OpenFile("Data/merkle_tree_lvl"+strconv.Itoa(level)+"_idx"+strconv.Itoa(index)+".txt", os.O_WRONLY|os.O_CREATE, 0777)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	if err != nil {
		panic(err)
	}
	n.WriteToFileHelper(file)
}

func (n *NodeMerkle) WriteToFileHelper(file *os.File) {
	file.Write([]byte(n.String()))
	file.Write([]byte(";"))
	if n.left != nil { //preorder
		n.left.WriteToFileHelper(file)
	}
	if n.right != nil {
		n.right.WriteToFileHelper(file)
	}
}
