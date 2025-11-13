package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool *BufferPool
	Desc    *TupleDesc
	HeapPages map[int]*heapPage
	FileName  string
	PageCount int
	file *os.File
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	if fromFile == "" {
		// Create a new temporary file
		tmpFile, err := os.CreateTemp("", "godb_heapfile_*.dat")
		if err != nil {
			return nil, err
		}
		fromFile = tmpFile.Name()
		tmpFile.Close()
	}
	hp :=  &HeapFile{
		Desc:    td,
		bufPool: bp,
		HeapPages: make(map[int]*heapPage),
		FileName: fromFile,
		file: func() *os.File {
			f, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				panic(fmt.Sprintf("failed to open file: %v", err))
			}
			return f
		}(),
	}
	// Load existing pages from file
	fileInfo, err := hp.file.Stat()
	if err != nil {
		return nil, err
	}
	numPages := int(fileInfo.Size() / int64(PageSize))
	for i := 0; i < numPages; i++ {
		page, err := newHeapPage(td, i, hp)
		if err != nil {
			return nil, err
		}
		offset := int64(i) * int64(PageSize)
		_, err = hp.file.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		data := make([]byte, PageSize)
		_, err = hp.file.Read(data)
		if err != nil {
			return nil, err
		}
		err = page.initFromBuffer(bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		hp.HeapPages[i] = page
	}
	hp.PageCount = numPages
	return hp, nil
}


// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	return f.FileName
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	return len(f.HeapPages)
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
    // 1. 检查 page 是否存在
    if _, ok := f.HeapPages[pageNo]; !ok {
        return nil, fmt.Errorf("page %d not found", pageNo)
    }

    // 2. 计算页偏移
    offset := int64(pageNo) * int64(PageSize)

    // 3. 跳转到文件 offset
    _, err := f.file.Seek(offset, io.SeekStart)
    if err != nil {
        return nil, fmt.Errorf("failed to seek to page %d: %w", pageNo, err)
    }

    // 4. 只读 PageSize 大小的数据
    data := make([]byte, PageSize)
    io.ReadFull(f.file, data)


    // 5. 初始化 HeapPage
    hp := f.HeapPages[pageNo]
    err = hp.initFromBuffer(bytes.NewBuffer(data))
    if err != nil {
        return nil, fmt.Errorf("failed to parse page %d: %w", pageNo, err)
    }

    return hp, nil
}


// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	for _, page := range f.HeapPages {
		if page.UsedSlotsNum < page.SlotNum {
			_, err := page.insertTuple(t)
			if err != nil {
				return err
			}
			page.setDirty(tid, true)
			return nil
		}
	}
	newPage, err := newHeapPage(f.Descriptor(), f.PageCount, f)
	if err != nil {
		return err
	}
	f.HeapPages[f.PageCount] = newPage
	f.PageCount++
	hash := heapHash{
		File:   f,
		PageNo: newPage.PageNo,
	}
	f.bufPool.pages[hash] = newPage
	_, err = newPage.insertTuple(t)
	if err != nil {
		return err
	}
	newPage.setDirty(tid, true)
	return nil
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	rid := t.Rid
	_rid, ok := rid.(RID)
	if !ok {
		return fmt.Errorf("invalid RID")
	}
	page := f.HeapPages[_rid.PageNo]
	if page == nil {
		return fmt.Errorf("page %d not found", _rid.PageNo)
	}
	if !ok {
		return fmt.Errorf("invalid page type")
	}
	page.deleteTuple(_rid)
	page.setDirty(tid, true)
	return nil
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	hp := p.(*heapPage)
	if hp == nil {
		return fmt.Errorf("flushPage: page is nil")
	}

	// 序列化页面为字节数组
	data, err := hp.toBuffer()
	if err != nil {
		return fmt.Errorf("flushPage: serialize page failed: %w", err)
	}

	// 转换 Buffer 为字节数组
	dataBytes := data.Bytes()

    pageId := hp.PageNo
    // 确定写回磁盘位置: 页号 * 页大小
    offset := int64(pageId) * int64(PageSize)

	// 将文件光标移动到对应位置
	_, err = f.file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("flushPage: seek failed: %w", err)
	}

	// 写入数据
	_, err = f.file.Write(dataBytes)
	if err != nil {
		return fmt.Errorf("flushPage: write failed: %w", err)
	}

    // 刷到磁盘（非常重要，否则可能只写入了内核缓冲）
    err = f.file.Sync()
    if err != nil {
        return fmt.Errorf("flushPage: fsync failed: %w", err)
    }

    // 清除脏标记
	tid := TransactionID(-1) // 使用一个无效的事务ID来清除脏标记
    hp.setDirty(tid, false)

    return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// return nil //replace me
	return f.Desc

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	var currentPageNo int = 0
	var currentSlotNo int = 0

	return func() (*Tuple, error) {
		for {
			if currentPageNo >= f.NumPages() {
				return nil, nil
			}
			hp := f.HeapPages[currentPageNo]
			if hp == nil {
				continue
			}
			if currentSlotNo < hp.UsedSlotsNum {
				tuple := hp.Tuples[currentSlotNo]
				currentSlotNo++
				return tuple, nil
			}
			currentPageNo++
			currentSlotNo = 0
		}
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	File     DBFile
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{
		File:   f,
		PageNo: pgNo,
	}
}
