package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The schema of this tuple.
     */
    private TupleDesc tupleDesc = null;

    /**
     * Array list of TDItem.
     * */
    private ArrayList<Field> fieldList = null;

    /**
     * The RecordId information for this tuple.
     * */
    private RecordId recordId = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) throws IllegalArgumentException {
        if (td == null || td.numFields() < 1) {
            throw new IllegalArgumentException("td is null or empty");
        }
        tupleDesc = td;
        fieldList = new ArrayList<Field>();
        for (int i = 0; i < td.numFields(); i++) {
            fieldList.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fieldList.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldList.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String str = "";
        for (int i = 0; i < fieldList.size(); i++) {
            Field field = fieldList.get(i);
            if (field != null) {
                str += field.toString();
            } else {
                str += "null";
            }
            if (i + 1 == fieldList.size()) {
                str += "\n";
            } else {
                str += "\t";
            }
        }
        return str;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return fieldList.iterator();
    }
}
