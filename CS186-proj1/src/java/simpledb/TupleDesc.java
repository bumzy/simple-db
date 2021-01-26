package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * Array list of TDItem
     * */
    private ArrayList<TDItem> tdItemList;

    /**
     * Hash Map of fieldName to index
     * */
    private HashMap<String, Integer> fieldNameMap;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;

        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TDItem)) {
                return false;
            }
            TDItem other = (TDItem)o;
            if (this.fieldType.equals(other.fieldType) && this.fieldName.equals(other.fieldName)) {
                return true;
            }
            return false;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) throws IllegalArgumentException {
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr is null or empty");
        }
        if (fieldAr != null && fieldAr.length != typeAr.length) {
            throw new IllegalArgumentException("fieldAr.length != typeAr.length");
        }
        tdItemList = new ArrayList<TDItem>();
        fieldNameMap = new HashMap<String, Integer>();
        for (int i = 0; i < typeAr.length; i++) {
            Type type = typeAr[i];
            if (type == null) {
                throw new IllegalArgumentException("typeAr constains null");
            }
            String field = null;
            if (fieldAr != null) {
                field = fieldAr[i];
                if (field != null) {
                    if (fieldNameMap.containsKey(field)) {
                        throw new IllegalArgumentException("fieldAr constains duplicate field, field=" + field);
                    } else {
                        fieldNameMap.put(field, i);
                    }
                }
            }
            tdItemList.add(new TDItem(type, field));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) throws IllegalArgumentException {
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= tdItemList.size()) {
            throw new NoSuchElementException();
        }
        return tdItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= tdItemList.size()) {
            throw new NoSuchElementException();
        }
        return tdItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null || name.isEmpty()) {
            throw new NoSuchElementException("field name is null or empty");
        }
        if (!fieldNameMap.containsKey(name)) {
            throw new NoSuchElementException("no matching name, field name=" + name);
        }
        return fieldNameMap.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return tdItemList.size();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int size = td1.getSize() + td2.getSize();
        Type[] typeAr = new Type[size];
        String[] fieldAr = new String[size];
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc)o;
        if (other.tdItemList.size() != this.tdItemList.size()) {
            return false;
        }
        for (int i = 0; i < other.tdItemList.size(); i++) {
            if (!other.tdItemList.get(i).equals(this.tdItemList.get(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String str = "";
        for (int i = 0; i < tdItemList.size(); i++) {
            Type fieldType = tdItemList.get(i).fieldType;
            String fieldName = tdItemList.get(i).fieldName;
            str = String.format("%s[%d]", fieldType.name(), i);
            if (fieldName != null) {
                str += String.format("(%s[%d])", fieldName, i);
            } else {
                str += String.format("(null[%d])", i);
            }
            if (i + 1 == tdItemList.size()) {
                str += ",";
            }
        }
        return str;
    }
}
