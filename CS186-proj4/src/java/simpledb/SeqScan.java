package simpledb;

import java.util.*;

import javax.xml.crypto.Data;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid = null;
    private int tableid = 0;
    private String tableAlias = null;
    private DbFileIterator dbFileIterator = null;
    private TupleDesc td = null;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        reset(tableid, tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.dbFileIterator = Database.getCatalog().getDbFile(tableid).iterator(tid);
        TupleDesc oldTd = Database.getCatalog().getTupleDesc(tableid);
        int numFields = oldTd.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];
        if (tableAlias == null) {
            tableAlias = "null";
        }
        for (int i = 0; i < numFields; i++) {
            typeAr[i] = oldTd.getFieldType(i);
            String field = oldTd.getFieldName(i);
            if (field == null) {
                field = "null";
            }
            fieldAr[i] = tableAlias + "." + field;
        }
        this.td = new TupleDesc(typeAr, fieldAr);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple oldTuple = dbFileIterator.next();
        if (oldTuple == null) {
            return null;
        }
        Tuple result = new Tuple(this.td);
        result.setRecordId(oldTuple.getRecordId());
        for (int i = 0; i < oldTuple.getTupleDesc().numFields(); i++) {
            result.setField(i, oldTuple.getField(i));
        }
        return result;
    }

    public void close() {
        dbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbFileIterator.rewind();
    }
}
