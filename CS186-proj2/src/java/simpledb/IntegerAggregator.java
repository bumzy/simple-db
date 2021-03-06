package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield = Aggregator.NO_GROUPING;
    private int afield = Aggregator.NO_GROUPING;
    private Op op = null;
    private TupleDesc td = null;
    private HashMap<Field, HashMap<Op, Integer>> gbMap = null;
    HashMap<Op, Integer> opMap = null;
    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.op = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] typeAr = new Type[]{Type.INT_TYPE};
            this.td = new TupleDesc(typeAr, null);
            this.opMap = newOpMap();
        } else {
            Type[] typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            this.td = new TupleDesc(typeAr, null);
            this.gbMap = new HashMap<Field, HashMap<Op, Integer>>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField af = (IntField) tup.getField(afield);
        int value = af.getValue();
        if (gbfield == Aggregator.NO_GROUPING) {
            mergeOpMap(this.opMap, value);
        } else {
            Field gbf = tup.getField(gbfield);
            if (gbMap.containsKey(gbf)) {
                HashMap<Op, Integer> opMap = gbMap.get(gbf);
                mergeOpMap(opMap, value);
            } else {
                HashMap<Op, Integer> opMap = newOpMap();
                mergeOpMap(opMap, value);
                gbMap.put(gbf, opMap);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        if (gbfield == Aggregator.NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            if (op.equals(Op.AVG)) {
                int count = opMap.get(Op.COUNT);
                int sum = opMap.get(Op.SUM);
                int avg = count > 0 ? sum / count : 0;
                tuple.setField(0, new IntField(avg));
            } else {
                tuple.setField(0, new IntField(opMap.get(op)));
            }
            tuples.add(tuple);
        } else {
            for (Field field : gbMap.keySet()) {
                Tuple tuple = new Tuple(td);
                HashMap<Op, Integer> opMap = gbMap.get(field);
                tuple.setField(0, field);
                if (op.equals(Op.AVG)) {
                    int count = opMap.get(Op.COUNT);
                    int sum = opMap.get(Op.SUM);
                    int avg = count > 0 ? sum / count : 0;
                    tuple.setField(1, new IntField(avg));
                } else {
                    tuple.setField(1, new IntField(opMap.get(op)));
                }
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

    private HashMap<Op, Integer> newOpMap() {
        HashMap<Op, Integer> opMap = new HashMap<Op, Integer>();
        opMap.put(Op.COUNT, 0);
        opMap.put(Op.MIN, Integer.MAX_VALUE);
        opMap.put(Op.MAX, Integer.MIN_VALUE);
        opMap.put(Op.SUM, 0);
        return opMap;
    }

    private void mergeOpMap(HashMap<Op, Integer> opMap, int value) {
        int count = opMap.get(Op.COUNT);
        int min = opMap.get(Op.MIN);
        int max = opMap.get(Op.MAX);
        int sum = opMap.get(Op.SUM);
        opMap.put(Op.COUNT, count + 1);
        opMap.put(Op.MIN, value < min ? value : min);
        opMap.put(Op.MAX, value > max ? value : max);
        opMap.put(Op.SUM, sum + value);
    }

}
