package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	PageSize     int
	SlotNum      int
	UsedSlotsNum int
	Tuples       []*Tuple
	Desc         *TupleDesc
	HeapFile     *HeapFile
	PageNo       int
	IsDirty      bool
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	// TODO: some code goes here
	// return &heapPage{}, fmt.Errorf("newHeapPage is not implemented") //replace me
	size := 0
	for i := 0; i < len(desc.Fields); i++ {
		switch desc.Fields[i].Ftype {
		case IntType:
			size += int(unsafe.Sizeof(int64(0)))
		case StringType:
			size += StringLength
		}
	}
	slotNum := (PageSize - 8) / size
	hp := &heapPage{
		PageSize:     PageSize,
		SlotNum:      slotNum,
		UsedSlotsNum: 0,
		Tuples:       make([]*Tuple, 0, slotNum),
		Desc:         desc,
		HeapFile:     f,
		PageNo:       pageNo,
		IsDirty:      false,
	}
	err := f.bufPool.insertPage(hp,f, pageNo, NewTID(), WritePerm)
	if err != nil {
		return nil, err
	}
	return hp, nil
}

func (h *heapPage) getNumSlots() int {
	return h.SlotNum
}

type RID struct {
	PageNo int
	SlotNo int
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if h.UsedSlotsNum >= h.SlotNum {
		return 0, fmt.Errorf("no free slots available")
	}
	h.Tuples = append(h.Tuples, t)
	h.UsedSlotsNum++
	rid := RID{
		PageNo: h.PageNo,
		SlotNo: h.UsedSlotsNum - 1,
	}
	t.Rid = rid
	return rid, nil
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	_rid, ok := rid.(RID)
	if !ok {
		return fmt.Errorf("invalid record ID type")
	}
	if _rid.SlotNo < 0 || _rid.SlotNo >= h.UsedSlotsNum {
		return fmt.Errorf("invalid record ID")
	}
	if h.Tuples[_rid.SlotNo] == nil {
		return fmt.Errorf("tuple at record ID already deleted")
	}
	h.Tuples[_rid.SlotNo] = nil
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.IsDirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.IsDirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	// TODO: some code goes here
	return p.HeapFile
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)

	// Write number of slots
	err := binary.Write(buf, binary.LittleEndian, int32(h.SlotNum))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, int32(h.UsedSlotsNum))
	if err != nil {
		return nil, err
	}

	// Write tuples
	for i := 0; i < h.SlotNum; i++ {
		if i < h.UsedSlotsNum && h.Tuples[i] != nil {
			err = h.Tuples[i].writeTo(buf)
			if err != nil {
				return nil, err
			}
		} else {
			// Write empty tuple with zero-initialized fields
			emptyTuple := Tuple{
				Desc:   *h.Desc,
				Fields: make([]DBValue, len(h.Desc.Fields)),
			}
			// Initialize each field with zero values
			for j, field := range h.Desc.Fields {
				switch field.Ftype {
				case IntType:
					emptyTuple.Fields[j] = IntField{Value: 0}
				case StringType:
					emptyTuple.Fields[j] = StringField{Value: ""}
				}
			}
			err = emptyTuple.writeTo(buf)
			if err != nil {
				return nil, err
			}
		}
	}
	for buf.Len() != PageSize {
		binary.Write(buf, binary.LittleEndian, false)
	}
	return buf, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// Read number of slots
	var slotNum, usedSlotsNum int32
	err := binary.Read(buf, binary.LittleEndian, &slotNum)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &usedSlotsNum)
	if err != nil {
		return err
	}

	// Initialize the heap page
	h.SlotNum = int(slotNum)
	h.UsedSlotsNum = int(usedSlotsNum)
	h.Tuples = make([]*Tuple, h.SlotNum)

	// Read tuples
	for i := 0; i < h.UsedSlotsNum; i++ {
		tuple, err := readTupleFrom(buf, h.Desc)
		if err != nil {
			return err
		}
		h.Tuples[i] = tuple
		tuple.Rid = RID{
			PageNo: h.PageNo,
			SlotNo: i,
		}
	}
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	i := 0
	return func() (*Tuple, error) {
		for {
			if i >= p.UsedSlotsNum {
				return nil, nil
			}
			tuple := p.Tuples[i]
			if tuple == nil {
				i++
				continue
			}
			i++
			return tuple, nil
		}
	}
}
