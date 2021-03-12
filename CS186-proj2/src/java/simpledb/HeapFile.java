package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f = null;
    private TupleDesc td = null;
    private int numPages = 0;
    private ArrayList<ReentrantReadWriteLock> rwLocks = null;
    private ReentrantReadWriteLock fLock = null;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.numPages = (int)Math.ceil(1.0 * f.length() / BufferPool.PAGE_SIZE);
        this.rwLocks = new ArrayList<ReentrantReadWriteLock>();
        for (int i = 0; i < this.numPages; i++) {
            this.rwLocks.add(new ReentrantReadWriteLock(true));
        }
        this.fLock = new ReentrantReadWriteLock(true);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        Page page = null;
        try {
            RandomAccessFile rafile = new RandomAccessFile(this.f, "r");
            int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
            rafile.seek(offset);
            rafile.read(data, 0, BufferPool.PAGE_SIZE);
            page = new HeapPage((HeapPageId) pid, data);
            rafile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try {
            RandomAccessFile rafile = new RandomAccessFile(this.f, "rw");
            int offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
            rafile.seek(offset);
            rafile.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
            rafile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        BufferPool bufferPool = Database.getBufferPool();
        ArrayList<Page> pages = new ArrayList<Page>();
        for (int i = 0; i < numPages; i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            Lock wLock = rwLocks.get(i).writeLock();
            try {
                wLock.lock();
                HeapPage page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
                if (page.getNumEmptySlots() > 0) {
                    page.insertTuple(t);
                    page.markDirty(true, tid);
                    pages.add(page);
                    return pages;
                }
            } finally {
                wLock.unlock();
            }
        }
        int numPage = numPages;
        Lock wLock = fLock.writeLock();
        try {
            wLock.lock();
            HeapPageId pid = new HeapPageId(getId(), numPage);
            HeapPage emptyPage = new HeapPage(pid, HeapPage.createEmptyPageData());
            numPages += 1;
            writePage(emptyPage);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            page.markDirty(true, tid);
            rwLocks.add(new ReentrantReadWriteLock(true));
            pages.add(page);
            return pages;
        } finally {
            wLock.unlock();
        }
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        int pageNumber = pid.pageNumber();
        Lock wLock = rwLocks.get(pageNumber).writeLock();
        try {
            wLock.lock();
            BufferPool bufferPool = Database.getBufferPool();
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            page.deleteTuple(t);
            page.markDirty(true, tid);
            return page;
        } finally {
            wLock.unlock();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

