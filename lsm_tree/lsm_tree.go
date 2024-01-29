package lsmt

import (
  "fmt"
  "log"
  "sync"
  "time"
  "bytes"
  "encoding/gob"
  "io"
  "strconv"
)
//First,needed binary tree implementation
type TreeNode struct {
	Elem Element
	Left *TreeNode
	Right *TreeNode
	Size int
  }
  
// NewTree accepts a sorted element slice and returns a binary tree representation.
func NewTree(elems []Element) *TreeNode {
	size := len(elems)
	if size == 0 {
	  return nil
	}
	root := &TreeNode{
	  Elem: elems[size/2],
	  Left: NewTree(elems[0:size/2]),
	  Size: size,
	}
	if rightIndex := size/2+1; rightIndex < size {
	  root.Right = NewTree(elems[rightIndex:size])
	}
	return root
}
  
  func Upsert(tree **TreeNode, elem Element) {
	if *tree == nil {
	  *tree = &TreeNode{Elem: elem, Size: 1}
	} else if elem.Key < (*tree).Elem.Key {
	  Upsert(&((*tree).Left), elem)
	  (*tree).Size++
	} else if elem.Key > (*tree).Elem.Key {
	  Upsert(&((*tree).Right), elem)
	  (*tree).Size++
	} else {
	  (*tree).Elem.Value = elem.Value
	}
  }
  
  func Find(tree *TreeNode, key string) (Element, error) {
	if tree == nil {
	  // Not found.
	  return Element{}, fmt.Errorf("key %s not found", key)
	} else if tree.Elem.Key == key {
	  return tree.Elem, nil
	}
	if key <= tree.Elem.Key {
	  return Find(tree.Left, key)
	} else {
	  return Find(tree.Right, key)
	}
  }
  
  // Traverse returns all the elements in key order.
  func Traverse(tree *TreeNode) []Element {
	var elems []Element
	if tree == nil {
	  return elems
	}
	left := Traverse(tree.Left)
	right := Traverse(tree.Right)
	elems = append(elems, left...)
	elems = append(elems, tree.Elem)
	return append(elems, right...)
  }
  
  func JustSmallerOrEqual(tree *TreeNode, key string) (Element, error) {
	if tree == nil {
	  return Element{}, fmt.Errorf("key %s is smaller than any key in the tree", key)
	}
	current := tree.Elem
	if current.Key <= key {
	  right, err := JustSmallerOrEqual(tree.Right, key)
	  if err == nil && current.Key < right.Key {
		current = right
	  }
	} else {
	  left, err := JustSmallerOrEqual(tree.Left, key)
	  if err != nil {
		return Element{}, err
	  }
	  current = left
	}
	return current, nil
  }
  
func JustLarger(tree *TreeNode, key string) (Element, error) {
	if tree == nil {
	  return Element{}, fmt.Errorf("key %s is larger than any key in the tree", key)
	}
	current := tree.Elem
	if current.Key > key {
	  left, err := JustLarger(tree.Left, key)
	  if err == nil && current.Key > left.Key {
		current = left
	  }
	} else {
	  right, err := JustLarger(tree.Right, key)
	  if err != nil {
		return Element{}, err
	  }
	  current = right
	}
	return current, nil
  }
//Second, disk file implementation
const (
	maxFileLen = 1024
	indexSparseRatio = 3
  )
  
  type DiskFile struct {
	index *TreeNode
	data io.ReadSeeker
	size int
	buf bytes.Buffer
  }
  
  func (d DiskFile) Empty() bool {
	return d.size == 0
  }
  
  func NewDiskFile(elems []Element) DiskFile {
	d := DiskFile{size: len(elems)}
	var indexElems []Element
	var enc *gob.Encoder
	for i, e := range elems {
	  if i % indexSparseRatio == 0 {
		// Create sparse index.
		idx := Element{Key: e.Key, Value: fmt.Sprintf("%d", d.buf.Len())}
		log.Printf("created sparse index element %v", idx)
		indexElems = append(indexElems, idx)
		enc = gob.NewEncoder(&d.buf)
	  }
	  enc.Encode(e)
	}
	d.index = NewTree(indexElems)
	return d
  }
  
  func (d DiskFile) Search(key string) (Element, error) {
	canErr := fmt.Errorf("key %s not found in disk file", key)
	if d.Empty() {
	  return Element{}, canErr
	}
	var si, ei int
	start, err := JustSmallerOrEqual(d.index, key)
	if err != nil {
	  // Key smaller than all.
	  return Element{}, canErr
	}
	si, _ = strconv.Atoi(start.Value)
	end, err := JustLarger(d.index, key)
	if err != nil {
	  // Key larger than all or equal to the last one.
	  ei = d.buf.Len()
	} else {
	  ei, _ = strconv.Atoi(end.Value)
	}
	log.Printf("searching in range [%d,%d)]", si, ei)
	buf := bytes.NewBuffer(d.buf.Bytes()[si:ei])
	dec := gob.NewDecoder(buf)
	for {
	  var e Element
	  if err := dec.Decode(&e); err != nil {
		log.Printf("got err: %v", err)
		break
	  }
	  if e.Key == key {
		return e, nil
	  }
	}
	return Element{}, canErr
  }
  
  func (d DiskFile) AllElements() []Element {
	indexElems := Traverse(d.index)
	var elems []Element
	var dec *gob.Decoder
	for i, idx := range indexElems {
	  start, _ := strconv.Atoi(idx.Value)
	  end := d.buf.Len()
	  if i < len(indexElems)-1 {
		end, _ = strconv.Atoi(indexElems[i+1].Value)
	  }
	  dec = gob.NewDecoder(bytes.NewBuffer(d.buf.Bytes()[start:end]))
	  var e Element
	  for dec.Decode(&e)==nil {
		elems = append(elems, e)
	  }
	}
	return elems
  }
//Third, lsm tree using the previous two implementations
type Element struct {
  Key, Value string
}

type LSMTree struct {
  // Read write lock to control access to the in-memory tree.
  rwm sync.RWMutex
  tree *TreeNode
  treeInFlush *TreeNode
  flushThreshold int
  // Read write lock to control access to the disk files.
  drwm sync.RWMutex
  diskFiles []DiskFile
}

func NewLSMTree(flushThreshold int) *LSMTree {
  t := &LSMTree{flushThreshold: flushThreshold}
  go t.compactService()
  return t
}

func (t *LSMTree) Put(key, value string) {
  t.rwm.Lock()
  defer t.rwm.Unlock()
  Upsert(&(t.tree), Element{Key: key, Value: value})
  if t.tree.Size >= t.flushThreshold && t.treeInFlush == nil {
    // Trigger flush.
    log.Printf("triggering flush %v", Traverse(t.tree))
    t.treeInFlush = t.tree
    t.tree = nil
    go t.flush()
  }
}

func (t *LSMTree) Get(key string) (string, error) {
  t.rwm.RLock()
  if e, err := Find(t.tree, key); err == nil {
    t.rwm.RUnlock()
    return e.Value, nil
  }
  if e, err := Find(t.treeInFlush, key); err == nil {
    t.rwm.RUnlock()
    return e.Value, nil
  }
  t.rwm.RUnlock()
  // The key is not in memory. Search in disk files.
  t.drwm.RLock()
  defer t.drwm.RUnlock()
  for _, d := range t.diskFiles {
    e, err := d.Search(key)
    if err == nil {
      // Found in disk
      return e.Value, nil
    }
  }
  return "", fmt.Errorf("key %s not found", key)
}

func (t *LSMTree) flush() {
  // Create a new disk file.
  d := []DiskFile{NewDiskFile(Traverse(t.treeInFlush))}
  // Put the disk file in the list.
  t.drwm.Lock()
  t.diskFiles = append(d, t.diskFiles...)
  t.drwm.Unlock()
  // Remove the tree in flush.
  t.rwm.Lock()
  t.treeInFlush = nil
  t.rwm.Unlock()
}

func (t *LSMTree) compactService() {
  for {
    time.Sleep(time.Second)
    var d1, d2 DiskFile
    t.drwm.RLock()
    if len(t.diskFiles) >= 2 {
      d1 = t.diskFiles[len(t.diskFiles)-1]
      d2 = t.diskFiles[len(t.diskFiles)-2]
    }
    t.drwm.RUnlock()
    if d1.Empty() || d2.Empty() {
      continue
    }
    // Create a new compacted disk file.
    d := compact(d1, d2)
    // Replace the two old files.
    t.drwm.Lock()
    t.diskFiles = t.diskFiles[0:len(t.diskFiles)-2]
    t.diskFiles = append(t.diskFiles, d)
    t.drwm.Unlock()
  }
}

func compact(d1, d2 DiskFile) DiskFile {
  elems1 := d1.AllElements()
  elems2 := d2.AllElements()
  log.Printf("compacting d1: %v; d2: %v", elems1, elems2)
  size := min(len(elems1), len(elems2))
  var newElems []Element
  var i1, i2 int
  for i1 < size && i2 < size {
    e1 := elems1[i1]
    e2 := elems2[i2]
    if e1.Key < e2.Key {
      newElems = append(newElems, e1)
      i1++
    } else if e1.Key > e2.Key {
      newElems = append(newElems, e2)
      i2++
    } else {
      // d1 is assumed to be older than d2.
      newElems = append(newElems, e2)
      i1++
      i2++
    }
  }
  newElems = append(newElems, elems1[i1:len(elems1)]...)
  newElems = append(newElems, elems2[i2:len(elems2)]...)
  return NewDiskFile(newElems)
}

func min(i, j int) int {
  if i < j {
    return i
  }
  return j
}