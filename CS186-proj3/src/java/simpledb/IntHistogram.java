package simpledb;

import java.lang.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets = 0;
    private long histogram[] = null;
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private int width = 0;
    private long ntups = 0;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
    	this.histogram = new long[this.buckets];
        this.min = min;
        this.max = max;
        this.width = (int) Math.ceil((double)(max - min + 1) / this.buckets);
    }

    private int valueToIndex(int v) {
        if (v == this.max) {
            return this.buckets - 1;
        } else {
            return (v - this.min) / this.width;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int i = this.valueToIndex(v);
        this.histogram[i] += 1;
        this.ntups += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int i = this.valueToIndex(v);
        int left = i * width + min;
        int right = left + width - 1;
        switch (op) {
            case EQUALS:
                if (v > this.max || v < this.min) {
                    return 0.0;
                }
                return 1.0 * this.histogram[i] / this.width / this.ntups;
            case GREATER_THAN:
                if (v > this.max) {
                    return 0.0;
                }
                if (v < this.min) {
                    return 1.0;
                }
                long height = this.histogram[i];
                double p1 = ((right - v) * 1.0 / width) * (height * 1.0 / ntups);
                int allInRight = 0;
                for (int j = i + 1; j < buckets; j++) {
                    allInRight += histogram[j];
                }
                double p2 = allInRight * 1.0 / ntups;
                return p1 + p2;
            case NOT_EQUALS:
                return 1.0 - this.estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return this.estimateSelectivity(Predicate.Op.EQUALS, v) + this.estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case LESS_THAN:
                return 1.0 - this.estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
            case LESS_THAN_OR_EQ:
                return 1.0 - this.estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case LIKE:
                return this.avgSelectivity();
            default:
                throw new RuntimeException("Should not reach hear");
        }
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        double sum = 0.0;
        for (int i = 0; i < this.histogram.length; i++) {
            sum = this.histogram[i];
        }
        return sum / this.buckets;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
