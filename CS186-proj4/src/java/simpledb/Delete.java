package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid = null;
    private DbIterator child = null;
    private TupleDesc td = null;
    private int count = 0;
    private boolean hasAccessed = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param tid
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId tid, DbIterator child) {
        this.tid = tid;
        this.child = child;
        Type[] typeAr = new Type[]{Type.INT_TYPE};
        this.td = new TupleDesc(typeAr);
        this.count = 0;
        this.hasAccessed = false;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
        count = 0;
        hasAccessed = false;
        while (child.hasNext()) {
            Tuple t = child.next();
            Database.getBufferPool().deleteTuple(this.tid, t);
            count += 1;
        }
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasAccessed) {
            return null;
        }
        hasAccessed = true;
        Tuple result = new Tuple(this.td);
        result.setField(0, new IntField(count));
        return result;
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