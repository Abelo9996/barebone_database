package edu.berkeley.cs186.database.query.join;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record on the left page
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            // Fetched next block of record from left source (B-2 pages)
            this.leftBlockIterator = getBlockIterator(this.leftSourceIterator,getLeftSource().getSchema(),numBuffers-2);
            // Check if the blockiterator returned something with content inside
            if (this.leftBlockIterator.hasNext()){
                // Set the next checkpoint for next reset
                this.leftBlockIterator.markNext();
                // Assign value to leftrecord
                this.leftRecord = this.leftBlockIterator.next();
            }
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            // Check if the sourceiterator has content inside
            if (this.rightSourceIterator.hasNext()){
            // fetched the next page of records from right source
                this.rightPageIterator = getBlockIterator(this.rightSourceIterator,getRightSource().getSchema(),1);
                // Check if rightpageiterator has content inside
                if (this.rightPageIterator.hasNext()){}else{fetchNextRightPage();}
                // Set checkpoint for rest of pageiterator.
                this.rightPageIterator.markNext();
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            if (rightPageIterator == null || leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }
            while(true) {
                if (this.rightPageIterator.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightPageIterator.next();
                    // If leftrecord == rightrecord at some value
                    if (compare(leftRecord, rightRecord) == 0) {
                        // return join
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftBlockIterator.hasNext()){
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    this.rightPageIterator.reset();
                    // Assign leftrecord value from the left blockiterator
                    this.leftRecord = leftBlockIterator.next();
                } else if (rightSourceIterator.hasNext()){
                    // Neither the right page nor left block iterators have values 
                    // to yield, but there's more right pages
                    fetchNextRightPage();
                    // Reset blockiterator
                    this.leftBlockIterator.reset();
                    // Assign leftrecord value from the first value of leftblockiterator
                    this.leftRecord = leftBlockIterator.next();
                } else if (leftSourceIterator.hasNext()){
                    // Neither right page nor left block iterators have values 
                    // nor are there more right pages, but there are still left blocks
                    this.rightSourceIterator.reset();
                    // Setup new rightpage
                    fetchNextRightPage();
                    // Setup new leftblock
                    fetchNextLeftBlock();
                } else {
                    // if you're here then there are no more records to fetch
                    return null;
                }
            }
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
    }
}
