package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield = Aggregator.NO_GROUPING;
    private int afield = Aggregator.NO_GROUPING;
    private Op op = null;
    private TupleDesc td = null;
    private HashMap<Field, Integer> countMap = null;
    private int allCount = 0;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("what is not Op.COUNT");
        }
        this.gbfield = gbfield;
        this.afield = afield;
        this.op = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] typeAr = new Type[]{Type.INT_TYPE};
            this.td = new TupleDesc(typeAr, null);
            this.allCount = 0;
        } else {
            Type[] typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            this.td = new TupleDesc(typeAr, null);
            this.countMap = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == Aggregator.NO_GROUPING) {
            this.allCount++;
        } else {
            Field gbf = tup.getField(gbfield);
            if (countMap.containsKey(gbf)) {
                int count = countMap.get(gbf);
                countMap.put(gbf, count + 1);
            } else {
                countMap.put(gbf, 1);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        if (gbfield == Aggregator.NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(allCount));
            tuples.add(tuple);
        } else {
            for (Field field : countMap.keySet()) {
                Tuple tuple = new Tuple(td);
                int count = countMap.get(field);
                tuple.setField(0, field);
                tuple.setField(1, new IntField(count));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
