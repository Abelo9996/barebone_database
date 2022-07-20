package edu.berkeley.cs186.database.query.join;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
              prepareRight(transaction, rightSource, rightColumnName),
              leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source isn't sorted, wraps the right source in a sort
     * operator. Otherwise, if it isn't materialized, wraps the right source in
     * a materialize operator. Otherwise, simply returns the right source. Note
     * that the right source must be materialized since we may need to backtrack
     * over it, unlike the left source.
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator implements Iterator<Record> {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();

            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            this.marked = false;
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            // Quick initial paranoia check of leftrecord not being null, else return null to end
            if (this.leftRecord == null || this.rightRecord == null){
                // The left source was empty, nothing to fetch
                return null;
            }
            // Iterate through values
            while(true) {
                // Quick initial paranoia check of leftrecord not being null, else return null to end
                if (this.leftRecord == null || this.rightRecord == null){
                    return null;
                }
                // If it is currently not marked = true (marked when we find a value to join)
                if (marked == false){
                    int i = 0;
                    // Set marked to true since we will find a value to join (or it just won't pass anymore if it doesn't)
                    marked = true;
                    // Follows pseudocode from notes (page 9?)
                    // Check that records on left are larger than right, and vice-versa
                    for (;(compare(this.leftRecord, rightRecord) > 0 && rightIterator.hasNext())||(compare(this.leftRecord, rightRecord) < 0 && leftIterator.hasNext());++i){
                        // Set rightrecord to the next value of rightiterator if leftrecord larger
                        if (compare(this.leftRecord, rightRecord) > 0){
                            // Set rightiterator.next
                            rightRecord = rightIterator.next();
                        // Set leftrecord to the next value of leftiterator if rightrecord larger
                        }else{
                            // set leftiterator.next
                            leftRecord = leftIterator.next();
                        }
                    }
                    // Assign the current checkpoint to rightiterator for next reset call
                    rightIterator.markPrev();
                }
                // Now that we have a equal pair
                if (compare(this.leftRecord, rightRecord) == 0){
                    // Assign the current record value by joining them
                    Record ret = this.leftRecord.concat(rightRecord);
                    // Set rightrecord if it has a next value
                    if (rightIterator.hasNext()){rightRecord = rightIterator.next();}else{
                    // Set marked to false, call reset, and setup leftrecord and right record for another check
                        // Set marked false
                        marked = false;
                        // Reset rightiterator
                        rightIterator.reset();
                        // Assign leftrecord to iterator.next if exists, otherwise null (don't forget to test null!)
                        if (leftIterator.hasNext()){this.leftRecord = leftIterator.next();}else{this.leftRecord = null;}
                        // Assign rightrecord to iterator.next (after being reset)
                        rightRecord = rightIterator.next();
                    }
                    // Return the joined record
                    return ret;
                }
                // If value was not equal, then repeat process by setting marked false
                // Set marked false
                marked = false;
                // Reset rightiterator
                rightIterator.reset();
                // Setup leftrecord to iterator.next if exists, otherwise null (don't forget to test null!)
                if (leftIterator.hasNext()){this.leftRecord = leftIterator.next();}else{this.leftRecord = null;}
                // Assign rightrecord to iterator.next (after being reset)
                rightRecord = rightIterator.next();
        }
    }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
