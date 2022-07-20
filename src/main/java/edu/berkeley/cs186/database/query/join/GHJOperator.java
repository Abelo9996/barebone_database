package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.query.*;
import edu.berkeley.cs186.database.query.disk.Partition;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class GHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    public GHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.GHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // Since this has a chance of failing on certain inputs we give it the
        // maximum possible cost to encourage the optimizer to avoid it
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // Executing GHJ on-the-fly is arduous without coroutines, so
            // instead we'll accumulate all of our joined records in this run
            // and return an iterator over it once the algorithm completes
            this.joinedRecords = new Run(getTransaction(), getSchema());
            this.run(getLeftSource(), getRightSource(), 1);
        };
        return joinedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * For every record in the given iterator, hashes the value
     * at the column we're joining on and adds it to the correct partition in
     * partitions.
     *
     * @param partitions an array of partitions to split the records into
     * @param records iterable of records we want to partition
     * @param left true if records are from the left relation, otherwise false
     * @param pass the current pass (used to pick a hash function)
     */
    private void partition(Partition[] partitions, Iterable<Record> records, boolean left, int pass) {
        // TODO(proj3_part1): implement the partitioning logic
        // You may find the implementation in SimpleHashJoin.java to be a good
        // starting point. You can use the static method HashFunc.hashDataBox 
        // to get a hash value.
        for (Record record: records) {
            // Partition left records on the chosen column
            DataBox columnValue;
            // If left is not the chosen column value
            if (left == false){
                // Set columnvalue
                columnValue = record.getValue(getRightColumnIndex());
            // If left is the chosen column value
            }else{
                // Set columnvalue
                columnValue = record.getValue(getLeftColumnIndex());
            }
            // apply hash to column value with current pass
            int hash = HashFunc.hashDataBox(columnValue, pass);
            // modulo to get which partition to use
            int partitionNum = hash % partitions.length;
            if (partitionNum < 0)  // hash might be negative
                partitionNum += partitions.length;
            // add the record to the partition iterator.
            partitions[partitionNum].add(record);
        }
    }

    /**
     * Runs the buildAndProbe stage on a given partition. Should add any
     * matching records found during the probing stage to this.joinedRecords.
     */
    private void buildAndProbe(Partition leftPartition, Partition rightPartition) {
        // true if the probe records come from the left partition, false otherwise
        boolean probeFirst;
        // We'll build our in memory hash table with these records
        Iterable<Record> buildRecords;
        // We'll probe the table with these records
        Iterable<Record> probeRecords;
        // The index of the join column for the build records
        int buildColumnIndex;
        // The index of the join column for the probe records
        int probeColumnIndex;

        if (leftPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = leftPartition;
            buildColumnIndex = getLeftColumnIndex();
            probeRecords = rightPartition;
            probeColumnIndex = getRightColumnIndex();
            probeFirst = false;
        } else if (rightPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = rightPartition;
            buildColumnIndex = getRightColumnIndex();
            probeRecords = leftPartition;
            probeColumnIndex = getLeftColumnIndex();
            probeFirst = true;
        } else {
            throw new IllegalArgumentException(
                "Neither the left nor the right records in this partition " +
                "fit in B-2 pages of memory."
            );}

        // Our hash table to build on. The list contains all the records in the
        // left records that hash to the same key
        Map<DataBox, List<Record>> hashTable = new HashMap<>();  
        
        // Building stage
        // Iterate through buildrecords
        for (Record leftRecord: buildRecords) {
            // find the record join value at the given index
            DataBox leftJoinValue = leftRecord.getValue(buildColumnIndex);
            // Check if hashtable contains that value above
            if (!hashTable.containsKey(leftJoinValue)) {
                // If not, add nothing
                hashTable.put(leftJoinValue, new ArrayList<>());
            }
            // otherwise, add it
            hashTable.get(leftJoinValue).add(leftRecord);
        }

        // Probing stage
        // Iterate through proberecords
        for (Record rightRecord: probeRecords) {
            // Find the record join value at the given probe index
            DataBox rightJoinValue = rightRecord.getValue(probeColumnIndex);
            // Check if hashtable contains that value above
            if (!hashTable.containsKey(rightJoinValue)) continue;
            // We have to join the right record with each left record with
            // a matching key
            for (Record lRecord : hashTable.get(rightJoinValue)) {
                // Create joined record
                Record joinedRecord = lRecord.concat(rightRecord);
                // Accumulate joined records in this.joinedRecords
                this.joinedRecords.add(joinedRecord);
            }
        }
        // TODO(proj3_part1): implement the building and probing stage
        // You shouldn't refer to any variable starting with "left" or "right"
        // here, use the "build" and "probe" variables we set up for you.
        // Check out how SHJOperator implements this function if you feel stuck.
    }

    /**
     * Runs the grace hash join algorithm. Each pass starts by partitioning
     * leftRecords and rightRecords. If we can run build and probe on a
     * partition we should immediately do so, otherwise we should apply the
     * grace hash join algorithm recursively to break up the partitions further.
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("Reached the max number of passes");

        // Create empty partitions
        Partition[] leftPartitions = createPartitions(true);
        Partition[] rightPartitions = createPartitions(false);

        // Partition records into left and right
        this.partition(leftPartitions, leftRecords, true, pass);
        this.partition(rightPartitions, rightRecords, false, pass);

        // Iterate through partitions (leftPartitions.length == rightPartitions.length)
        for (int i = 0; i < leftPartitions.length; i++) {
            // TODO(proj3_part1): implement the rest of grace hash join
            // If you meet the conditions to run the build and probe you should
            // do so immediately. Otherwise you should make a recursive call.
            // Check if both partitions are above buffer memory allocated (Notes Page 6)
            if (rightPartitions[i].getNumPages() > numBuffers -2 && leftPartitions[i].getNumPages() > numBuffers -2){
                // Keep partitioning to make smaller in size to fit memory
                run(leftPartitions[i],rightPartitions[i],pass+1);
            } else{
                // Otherwise, begin build and probe
                buildAndProbe(leftPartitions[i], rightPartitions[i]);
            }
    }
}

    // Provided Helpers ////////////////////////////////////////////////////////

    /**
     * Create an appropriate number of partitions relative to the number of
     * available buffers we have.
     *
     * @return an array of partitions
     */
    private Partition[] createPartitions(boolean left) {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition(left);
        }
        return partitions;
    }

    /**
     * Creates either a regular partition or a smart partition depending on the
     * value of this.useSmartPartition.
     * @param left true if this partition will store records from the left
     *             relation, false otherwise
     * @return a partition to store records from the specified partition
     */
    private Partition createPartition(boolean left) {
        Schema schema = getRightSource().getSchema();
        if (left) schema = getLeftSource().getSchema();
        return new Partition(getTransaction(), schema);
    }

    // Student Input Methods ///////////////////////////////////////////////////

    /**
     * Creates a record using val as the value for a single column of type int.
     * An extra column containing a 500 byte string is appended so that each
     * page will hold exactly 8 records.
     *
     * @param val value the field will take
     * @return a record
     */
    private static Record createRecord(int val) {
        String s = new String(new char[500]);
        return new Record(val, s);
    }

    /**
     * This method is called in testBreakSHJButPassGHJ.
     *
     * Come up with two lists of records for leftRecords and rightRecords such
     * that SHJ will error when given those relations, but GHJ will successfully
     * run. createRecord(int val) takes in an integer value and returns a record
     * with that value in the column being joined on.
     *
     * Hints: Both joins will have access to B=6 buffers and each page can fit
     * exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakSHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();
        int i = 0;
        // TODO(proj3_part1): populate leftRecords and rightRecords such that
        // SHJ breaks when trying to join them but not GHJ
        // SHJ breaks when it is over memory
        while (i < 136){
            // Add records with same index to left
            leftRecords.add(createRecord(i));
            // Add records with same index to right
            rightRecords.add(createRecord(i));
            ++i;
        }
        return new Pair<>(leftRecords, rightRecords);
    }

    /**
     * This method is called in testGHJBreak.
     *
     * Come up with two lists of records for leftRecords and rightRecords such
     * that GHJ will error (in our case hit the maximum number of passes).
     * createRecord(int val) takes in an integer value and returns a record
     * with that value in the column being joined on.
     *
     * Hints: Both joins will have access to B=6 buffers and each page can fit
     * exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakGHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();
        int i = 0;
        // TODO(proj3_part1): populate leftRecords and rightRecords such that GHJ breaks
        // GHJ breaks when Key skew occurs, hence added 69420101 (69 420 lol) to each record by a large amount of times
        while(i < 1000) {
            // Add records with same index to left
            leftRecords.add(createRecord(69420101));
            // Add records with same index to right
            rightRecords.add(createRecord(69420101));
            ++i;
        }
        return new Pair<>(leftRecords, rightRecords);
    }
}

