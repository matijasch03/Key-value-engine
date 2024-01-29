package merkletree

import (
	"crypto/sha1"
	"encoding/hex"
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
