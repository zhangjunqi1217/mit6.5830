package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
	"sync"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type pageLock struct {
	mu      sync.Mutex
	readers map[TransactionID]bool
	writer  TransactionID
}

// BufferPool now includes a mutex and a map to track page locks
type BufferPool struct {
	pages     map[heapHash]Page
	DBFiles   map[heapHash]*DBFile
	NumPages  int
	UsedPages int
	lock      sync.Mutex
	pageLocks map[heapHash]*pageLock
	running   map[TransactionID]bool
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		pages:     make(map[heapHash]Page),
		DBFiles:   make(map[heapHash]*DBFile),
		NumPages:  numPages,
		UsedPages: 0,
		pageLocks: make(map[heapHash]*pageLock),
		running:   make(map[TransactionID]bool),
	}, nil
}

func (bp *BufferPool) insertPage(hp *heapPage, file DBFile, pageNo int, tid TransactionID, perm RWPerm) error {
	hash := heapHash{
		File:   file,
		PageNo: pageNo,
	}

	if bp.UsedPages >= bp.NumPages {
		// Evict a page
		for h, p := range bp.pages {
			if !p.isDirty() {
				// Evict this page
				err := file.flushPage(p)
				if err != nil {
					return err
				}
				delete(bp.pages, h)
				bp.pages[hash] = hp
				return nil
			}
		}
		return fmt.Errorf("buffer pool is full of dirty pages; cannot evict")

	} else {
		bp.UsedPages++
		bp.pages[hash] = hp
		return nil
	}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	for _, page := range bp.pages {
		hp, ok := page.(*heapPage)
		if !ok {
			continue
		}
		hp.HeapFile.flushPage(page)
		page.setDirty(-1, false) // 使用一个无效的事务ID来清除脏标记
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.lock.Lock()
	defer bp.lock.Unlock()
	for hash, page := range bp.pages {
		if page.isDirty() {
			// 从磁盘中读取原始页面
			file, ok := hash.File.(*HeapFile)
			if ok {
				originalPage, err := file.readPage(hash.PageNo)
				if err == nil {
					// 替换缓冲池中的页面
					bp.pages[hash] = originalPage
				}
			}
			// 清除脏标记
			page.setDirty(-1, false)
		}
	}

	// 释放该事务持有的所有页面锁
	for hash, pl := range bp.pageLocks {
		pl.mu.Lock()
		delete(pl.readers, tid)
		if pl.writer == tid {
			pl.writer = 0
		}
		if len(pl.readers) == 0 && pl.writer == 0 {
			delete(bp.pageLocks, hash)
		}
		pl.mu.Unlock()
	}

	// 释放事务锁
	delete(bp.running, tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.lock.Lock()
	defer bp.lock.Unlock()

	for hash, page := range bp.pages {
		if page.isDirty() {
			// 写回磁盘
			file, ok := hash.File.(*HeapFile)
			if ok {
				file.flushPage(page)
			}
			// 清除脏标记
			page.setDirty(-1, false)
		}
	}

	// 释放该事务持有的所有页面锁
	for hash, pl := range bp.pageLocks {
		pl.mu.Lock()
		delete(pl.readers, tid)
		if pl.writer == tid {
			pl.writer = 0
		}
		if len(pl.readers) == 0 && pl.writer == 0 {
			delete(bp.pageLocks, hash)
		}
		pl.mu.Unlock()
	}

	// 释放事务锁
	delete(bp.running, tid)
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	// return nil
	bp.lock.Lock()
	defer bp.lock.Unlock()

	if _, exists := bp.running[tid]; exists {
		return fmt.Errorf("transaction %d is already running", tid)
	}
	bp.running[tid] = true
	return nil
}

// Helper function to acquire a lock on a page
func (bp *BufferPool) acquireLock(hash heapHash, tid TransactionID, perm RWPerm) error {
	// fmt.Printf("acquireLock: File=%p, PageNo=%d, tid=%v\n", hash.File, hash.PageNo, tid)

	bp.lock.Lock()
	pl, exists := bp.pageLocks[hash]
	if !exists {
		pl = &pageLock{
			readers: make(map[TransactionID]bool),
		}
		bp.pageLocks[hash] = pl
	}
	bp.lock.Unlock()

	pl.mu.Lock()
	defer pl.mu.Unlock()
	switch perm {
	case ReadPerm:
		// 可以获取读锁的条件：没有其他事务持有写锁
		if pl.writer == 0 || pl.writer == tid {
			pl.readers[tid] = true
			return nil
		} else {
			return fmt.Errorf("cannot acquire read lock: page is locked for writing by another transaction")
		}
	case WritePerm:
		// 可以获取写锁的条件：
		// 1. 当前事务已经持有写锁（重入）
		// 2. 没有写锁，且没有读者
		// 3. 没有写锁，且当前事务是唯一的读者（锁升级）
		if pl.writer == tid {
			// 已经持有写锁，重入
			return nil
		}
		if pl.writer != 0 {
			// 其他事务持有写锁
			return fmt.Errorf("cannot acquire write lock: page is locked for writing by another transaction")
		}
		// pl.writer == 0，检查读者
		if len(pl.readers) == 0 {
			// 没有读者，可以获取写锁
			pl.writer = tid
			return nil
		}
		if len(pl.readers) == 1 && pl.readers[tid] {
			// 当前事务是唯一的读者，可以升级为写锁
			pl.writer = tid
			return nil
		}
		// 有其他事务持有读锁
		return fmt.Errorf("cannot acquire write lock: page is locked for reading by other transactions")
	}
	return fmt.Errorf("unknown permission type")
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	hash := heapHash{
		File:   file,
		PageNo: pageNo,
	}

	if err := bp.acquireLock(hash, tid, perm); err != nil {
		return nil, err
	}
	// 不要在这里释放锁，锁应该在事务提交/中止时释放

	bp.lock.Lock()
	defer bp.lock.Unlock()

	if page, ok := bp.pages[hash]; ok {
		return page, nil
	} else {
		// If not found in cache, read from disk
		page, err := file.readPage(pageNo)
		if err != nil {
			return nil, err
		}
		if bp.UsedPages >= bp.NumPages {
			// Evict a page
			for h, p := range bp.pages {
				if !p.isDirty() {
					// Evict this page
					err := file.flushPage(p)
					if err != nil {
						return nil, err
					}
					delete(bp.pages, h)
					bp.pages[hash] = page
					return page, nil
				}
			}
			return nil, fmt.Errorf("buffer pool is full of dirty pages; cannot evict")
		} else {
			bp.UsedPages++
			bp.pages[hash] = page
			return page, nil
		}
	}
}
