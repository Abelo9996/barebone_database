package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses);
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        // Create new arraylist to store records
        List<Record> our_recs = new ArrayList<>();
        int i = 0;
        // Iterate through all records
        for (;records.hasNext();++i){
            // Add records to arraylist
            our_recs.add(records.next());
        }
        // Sort arraylist using the given comparator
        our_recs.sort(comparator);
        // Create new run to store arraylist
        Run ret = makeRun();
        // Add all records
        ret.addAll(our_recs);
        return ret;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        
        // TODO(proj3_part1): implement
        int i = 0;
        // Create arraylist of iterators
        ArrayList<Iterator<Record>> iterval = new ArrayList<>();
        // Add the iterators of the given runs to the arraylist above
        for (Run our_run:runs){iterval.add(our_run.iterator());}
        // Create new run to store runs
        Run ret = makeRun();
        // ArrayList of records to store
        List<Record> our_record = new ArrayList<>();
        // Create PriorityQueue of Pair<Record,Integer> objects as suggested in instructions above
        // Uses RecordPairComparator to sort pairs in PriorityQueue
        PriorityQueue<Pair<Record, Integer>> our_queue = new PriorityQueue(new RecordPairComparator());
        // Iterate until we reach the end of the created iterator from above.
        while(i < iterval.size()) {
            // If the iterator at the certain index has a value attached to it
            if (iterval.get(i).hasNext()) {
                // Then add the pair of record and integer to the given priorityqueue above
                our_queue.add(new Pair<Record, Integer>(iterval.get(i).next(), i));
            }
            ++i;
        }
        for (i=0;!our_queue.isEmpty();++i){
            // Get the first Pair of the PriorityQueue
            Pair<Record, Integer> firstval = our_queue.poll();
            // Add the record to our arraylist of records 
            our_record.add(firstval.getFirst());
            // If the iterator for that certain index has another value
            if (iterval.get(firstval.getSecond()).hasNext()) {
                // Add the new given pair of record and integer to the priority heap to sort
                our_queue.add(new Pair<Record, Integer>(iterval.get(firstval.getSecond()).next(), firstval.getSecond()));
            }
        }
        // Add all records to our create run
        ret.addAll(our_record);
        return ret;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        int i = 0;
        // Create new arraylist to store sorted runs
        List<Run> retrun = new ArrayList<>();
        // Iterate through all runs until size is less than or equal to B-1
        for (;numBuffers - 1 < runs.size();++i){
            // Add Run[0,B-1] to arraylist (by also merging the sorted runs)
            retrun.add(mergeSortedRuns(runs.subList(0, numBuffers-1)));
            // Setup new arraylist, runs = runs[B-1, size()]
            runs = runs.subList(numBuffers-1, runs.size());
        }
        // Add all the sorted runs to the arraylist
        retrun.add(mergeSortedRuns(runs));
        return retrun;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryPlan class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        // Create arraylist to store all runs
        List<Run> ret = new ArrayList<Run>();
        // Create the iterator to iterate through given records
        Iterator<Record> sourceIterator = getSource().iterator();
        int i;
        // Iterate through every record within sourceiterator
        for (i = 0;sourceIterator.hasNext();++i){
            // Add the sorted run of B pages of records, and add to the arraylist
            ret.add(sortRun(getBlockIterator(sourceIterator, getSchema(), numBuffers)));
        }
        // TODO(proj3_part1): implement
        // Merge all until we get just 1 final result of a run
        for (i = 0;ret.size()>1;++i){ret = mergePass(ret);}
        // Pick up the run from arraylist at index 0
        Run retrun = ret.get(0);
        return retrun; // TODO(proj3_part1): replace this!
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

