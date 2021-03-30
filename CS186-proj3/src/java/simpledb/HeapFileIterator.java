package simpledb;

import java.util.*;

/**
 * HeapFileIterator is an implementation of DbFileIterator that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class HeapFileIterator implements DbFileIterator {

    private TransactionId tid = null;
    private HeapFile file = null;
    private int pagePos = 0;
    private Iterator<Tuple> tuplesInPage = null;

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        this.tid = tid;
        this.file = file;
        this.pagePos = 0;
        this.tuplesInPage = null;
    }

    public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        return page.iterator();
    }

    /**
     * Opens the iterator. This must be called before any of the other methods.
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        pagePos = 0;
        HeapPageId pid = new HeapPageId(file.getId(), pagePos);
        tuplesInPage = getTuplesInPage(pid);
    }

    /**
     * Returns true if the iterator has more tuples.
     *
     * @return true f the iterator has more tuples.
     * @throws IllegalStateException If the iterator has not been opened
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tuplesInPage == null) {
            return false;
        }
        if (tuplesInPage.hasNext()) {
            return true;
        }
        if (pagePos < file.numPages() - 1) {
            pagePos += 1;
            HeapPageId pid = new HeapPageId(file.getId(), pagePos);
            tuplesInPage = getTuplesInPage(pid);
            return tuplesInPage.hasNext();
        }
        return false;
    }

    /**
     * Returns the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return the next tuple in the iteration.
     * @throws NoSuchElementException if there are no more tuples.
     * @throws IllegalStateException  If the iterator has not been opened
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("not opened or no tuple remained");
        }
        return tuplesInPage.next();
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException           when rewind is unsupported.
     * @throws IllegalStateException If the iterator has not been opened
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    /**
     * Closes the iterator. When the iterator is closed, calling next(), hasNext(),
     * or rewind() should fail by throwing IllegalStateException.
     */
    @Override
    public void close() {
        tuplesInPage = null;
        pagePos = 0;
    }

}
