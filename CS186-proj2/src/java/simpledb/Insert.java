package simpledb;

import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid = null;
    private DbIterator child = null;
    private TupleDesc td = null;
    private int tableId = 0;
    private int insertedNum = 0;
    private TupleDesc intTd = null;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId tid, DbIterator child, int tableId)
            throws DbException {
        this.tid = tid;
        this.child = child;
        this.td = child.getTupleDesc();
        this.tableId = tableId;
        this.insertedNum = 0;
        Type[] typeAr = new Type[]{Type.INT_TYPE};
        this.intTd = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.insertedNum = 0;
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
        this.insertedNum = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        this.insertedNum = 0;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, t);
                this.insertedNum += 1;
                Tuple result = new Tuple(this.intTd);
                result.setField(0, new IntField(this.insertedNum));
                return result;
            } catch (IOException e) {
                throw new DbException(e.toString());
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
