package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // Commit record is appended
        long ret = logManager.appendToLog(new CommitTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        // Log is flushed
        logManager.flushToLSN(ret);
        // Transaction table updated
        transactionTable.get(transNum).lastLSN = ret;
        // Transaction status updated
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        // Return LSN of commit record
        return ret;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // Abort record is appended and transaction table is updated
        transactionTable.get(transNum).lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        // Transaction status is updated
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);
        // Returns the LSN of the abort record
        return transactionTable.get(transNum).lastLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // If transaction is aborting
        if (transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.ABORTING){
            // Roll back changes
            rollbackToLSN(transNum, 0);
        }
        // EndTransactionLogRecord appended to log
        long ret = logManager.appendToLog(new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        // Transaction LSN updated
        transactionTable.get(transNum).lastLSN = ret;
        // Transaction status updated
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
        // Transaction removed from transaction table
        transactionTable.remove(transNum);
        //  Return LSN of end record
        return ret;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Emit the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // While current LSN > LSN rolling back to
        while (currentLSN > LSN){
            // If record is undoable
            if (logManager.fetchLogRecord(currentLSN).isUndoable()){
                // Get CLR by undoing record
                LogRecord clr = logManager.fetchLogRecord(currentLSN).undo(lastRecordLSN);
                // Append clr to log
                long retlsn = logManager.appendToLog(clr);
                // Update LSN
                transactionEntry.lastLSN = lastRecordLSN = retlsn;
                // Redo on CLR
                clr.redo(this,diskSpaceManager, bufferManager);
            }
            // If record does indeed exist
            if (logManager.fetchLogRecord(currentLSN).getPrevLSN().isPresent()){
                // Update current LSN to next record
                currentLSN = logManager.fetchLogRecord(currentLSN).getPrevLSN().get();
            }
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        // UpdatePageLogRecord appended to log
        long lsn = logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, transactionTable.get(transNum).lastLSN, pageOffset, before, after));
        // Transaction table updated
        transactionTable.get(transNum).lastLSN = lsn;
        // Dirty page table updated
        dirtyPageTable.putIfAbsent(pageNum, lsn);
        // Return LSN of last record written to log
        return lsn;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        // Rollback to LSN appropriately
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        // Iterate through the entry set of the dirty Page table
        for (Map.Entry<Long,Long> ourmap:dirtyPageTable.entrySet()){
            // If it does not fit in one record
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size()+1, 0)){
                // Append the EndCheckPointLogRecord to log
                logManager.appendToLog(new EndCheckpointLogRecord(chkptDPT, chkptTxnTable));
                // Clear transactiontable
                chkptTxnTable.clear();
                // Clear dirtypagetable
                chkptDPT.clear();
            }
            // If it still fits in one record, place recLSNs from DPT to our chkptDPT
            chkptDPT.put(ourmap.getKey(), ourmap.getValue());
        }
        // Iterate through the entry set of the transaction table
        for (Map.Entry<Long,TransactionTableEntry> ourmap:transactionTable.entrySet()){
            // If it does not fit in one record
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size()+1)){
                // Append the EndCheckPointLogRecord to log
                logManager.appendToLog(new EndCheckpointLogRecord(chkptDPT, chkptTxnTable));
                // Clear transactiontable
                chkptTxnTable.clear();
                // Clear dirtypagetable
                chkptDPT.clear();
            }
            // If it still fits in one record, place status/lastLSNs from the transactions table to our chkptTxnTable
            chkptTxnTable.put(ourmap.getKey(), new Pair<Transaction.Status, Long>(ourmap.getValue().transaction.getStatus(), ourmap.getValue().lastLSN));
        }
        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related, update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records are processed, cleanup and end transactions that are in
     * the COMMITING state, and move all transactions in the RUNNING state to
     * RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        // Scan log records
        Iterator<LogRecord> ouriter = logManager.scanFrom(LSN);
        // While our iterator has a logrecord
        while (ouriter.hasNext()){
            // Get next logrecord
            LogRecord ourrecord = ouriter.next();
            // If the log record is for a transaction operation (getTransNum is present)
            if (ourrecord.getTransNum().isPresent()){
                // If transaction doesn't exist
                if (!transactionTable.containsKey(ourrecord.getTransNum().get())) {
                    // Start transaction
                    startTransaction(newTransaction.apply(ourrecord.getTransNum().get()));
                }
                // Otherwise, continue and update the transaction table
                transactionTable.get(ourrecord.getTransNum().get()).lastLSN = ourrecord.getLSN();
            }
            // If the log record is page-related
            if (ourrecord.getPageNum().isPresent()){
                // If Update page or unodo update page
                if (ourrecord.type.equals(LogType.UPDATE_PAGE) || ourrecord.type.equals(LogType.UNDO_UPDATE_PAGE)){
                    // Dirty pages
                    dirtyPageTable.putIfAbsent(ourrecord.getPageNum().get(), ourrecord.getLSN());
                // If free/undoalloc page
                }else if (ourrecord.type.equals(LogType.FREE_PAGE) || ourrecord.type.equals(LogType.UNDO_ALLOC_PAGE)){
                    // flush changes to disk
                    logManager.flushToLSN(ourrecord.getLSN());
                    // Remove from dirty pages
                    dirtyPageTable.remove(ourrecord.getPageNum().get());
                }
                // no action needed for alloc/undofree page
            }
            // if END_TRANSACTION
            if (ourrecord.type.equals(LogType.END_TRANSACTION)){
                // Clean up  transaction
                transactionTable.get(ourrecord.getTransNum().get()).transaction.cleanup();
                // Set transaction status to be complete
                transactionTable.get(ourrecord.getTransNum().get()).transaction.setStatus(Transaction.Status.COMPLETE);
                // remove from txn table
                transactionTable.remove(ourrecord.getTransNum().get());
                // Add to endedTransactions
                endedTransactions.add(ourrecord.getLSN());
            // If status is commit_transaction
            }else if (ourrecord.type.equals(LogType.COMMIT_TRANSACTION)){
                // Update to committing
                transactionTable.get(ourrecord.getTransNum().get()).transaction.setStatus(Transaction.Status.COMMITTING);
            // If status is abort_transaction
            }else if (ourrecord.type.equals(LogType.ABORT_TRANSACTION)){
                // Update to recovery_aborting
                transactionTable.get(ourrecord.getTransNum().get()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            // If the log record is an end_checkpoint record
            }else if (ourrecord.type.equals(LogType.END_CHECKPOINT)){
                // Copy all entries of checkpoint DPT
                // Iterate through every entry of the checkpoint dirtypagetable entry set
                for (Map.Entry<Long, Long> val: ourrecord.getDirtyPageTable().entrySet()){
                    // Copy value to dpt
                    dirtyPageTable.put(val.getKey(), val.getValue());
                }
                // Iterate through the transaction table entry set
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entryval: ourrecord.getTransactionTable().entrySet()) {
                    // If our transaction table entry set is not in endedTransaction
                    if (!endedTransactions.contains(entryval.getKey())) {
                        // and if transaction table does not contain the given entry
                        if (!transactionTable.containsKey(entryval.getKey())) {
                            // Apply the new transaction
                            Transaction ourxact = newTransaction.apply(entryval.getKey());
                            // Begin the transaction
                            startTransaction(ourxact);
                            // Update transaction table
                            transactionTable.get(ourxact.getTransNum()).lastLSN = entryval.getValue().getSecond();
                        }
                        // Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's
                        if (ourrecord.getLSN() <= entryval.getValue().getSecond()) {
                            // Update LSN value
                            transactionTable.get(entryval.getKey()).lastLSN = entryval.getValue().getSecond();
                        }
                        // The status's in the transaction table should be updated if it is possible
                        // If entry status is complete
                        if (entryval.getValue().getFirst() == Transaction.Status.COMPLETE) {
                            // Update transaction table status to complete
                            transactionTable.get(entryval.getKey()).transaction.setStatus(Transaction.Status.COMPLETE);
                        // If transaction table status is running and entry status is aborting
                        }else if (transactionTable.get(entryval.getKey()).transaction.getStatus() == Transaction.Status.RUNNING && entryval.getValue().getFirst() == Transaction.Status.ABORTING) {
                            // Set transaction table status to be recovery_aborting
                            transactionTable.get(entryval.getKey()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        // If transaction table status is running and entry status is Committing
                        } else if (entryval.getValue().getFirst() == Transaction.Status.COMMITTING && transactionTable.get(entryval.getKey()).transaction.getStatus() == Transaction.Status.RUNNING) {
                            // Set transaction table status to be committing
                            transactionTable.get(entryval.getKey()).transaction.setStatus(Transaction.Status.COMMITTING);
                        }
                    }
                }
            }
        }
        // Iterate through the transactiontable entryset
        for (Map.Entry<Long,TransactionTableEntry> val:transactionTable.entrySet()){
            // If transaction status is complete
            if (val.getValue().transaction.getStatus() == Transaction.Status.COMPLETE) {
                // Remove transaction from transactiontable
                transactionTable.remove(val.getValue().transaction.getTransNum());
            // If transaction status is committing
            }else if (val.getValue().transaction.getStatus() == Transaction.Status.COMMITTING){
                // Cleanup transaction
                val.getValue().transaction.cleanup();
                // End transaction
                end(val.getKey());
            // If transaction status is running
            }else if (val.getValue().transaction.getStatus() == Transaction.Status.RUNNING){
                // Move transaction status to recovery_aborting
                val.getValue().transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                // Append an AbortTransactionLogRecord to log
                val.getValue().lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(val.getKey(),val.getValue().lastLSN));                
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a partition (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // Do nothing if dpt is empty
        if (dirtyPageTable.values().size() == 0 ){
        }else{
        // Set starting point iterator for redo from the dpt
        Iterator<LogRecord> ouriter = logManager.scanFrom(Collections.min(dirtyPageTable.values()));
        // While a LogRecord exists
        while (ouriter.hasNext()){
            // Get LogRecord
            LogRecord ourrecord = ouriter.next();
            // If record is redoable and about a partition
            if (ourrecord.isRedoable() && ourrecord.getPartNum().isPresent()){
                // Redo it
                ourrecord.redo(this,diskSpaceManager,bufferManager);
            }
            // If record is redoable and allocates a page
            if (ourrecord.isRedoable() && (ourrecord.getType() ==LogType.ALLOC_PAGE || ourrecord.getType() == LogType.UNDO_FREE_PAGE)){
                // Always Redo it
                ourrecord.redo(this,diskSpaceManager,bufferManager);
            // If record is redoable and modifies a page
            }else if (ourrecord.isRedoable() && ourrecord.getPageNum().isPresent()){
                // Page fetched from disk
                Page page = bufferManager.fetchPage(new DummyLockContext(), ourrecord.getPageNum().get());
                try{
                    // If dirty page table with LSN >= recLSN, modifies a page in dpt, and page LSN < record LSN
                    if (dirtyPageTable.containsKey(ourrecord.getPageNum().get()) && page.getPageLSN() < ourrecord.getLSN() && dirtyPageTable.get(ourrecord.getPageNum().get()) <= ourrecord.getLSN()){
                        // Record is redone
                        ourrecord.redo(this,diskSpaceManager,bufferManager);
                    }    
                }finally{
                    // Finally, unpin the page
                    page.unpin();
                }
            }
        }
    }
}

    /**
     * This method performs the undo pass of restart recovery.
     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and emit the appropriate CLR
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if not available) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        // Priority queue created
        PriorityQueue<Pair<Long, Long>> ourqueue = new PriorityQueue<>(new PairFirstReverseComparator<>());
        // Fill up queue with values sorted on lastLSN of all aborting transactions
        // Iterate through transaction values
        for (TransactionTableEntry val : transactionTable.values()) {
            // Add lastLSN to queue if transaction's status is recovery_aborting
            if (val.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {ourqueue.add(new Pair<>(val.lastLSN,val.transaction.getTransNum()));}
        }
        // While the queue is not empty
        while (!ourqueue.isEmpty()){
            // Remove pair value
            Pair<Long,Long> ourval = ourqueue.remove();
            // Get LogRecord from the given LSN
            LogRecord ourrecord = logManager.fetchLogRecord(ourval.getFirst());
            // If record is undoable
            if (ourrecord.isUndoable()){
                // Undo it and get CLR
                LogRecord ourclr = ourrecord.undo(transactionTable.get(ourrecord.getTransNum().get()).lastLSN);
                // Update TransactionTable
                transactionTable.get(ourrecord.getTransNum().get()).lastLSN = logManager.appendToLog(ourclr);
                // Redo clr
                ourclr.redo(this,diskSpaceManager, bufferManager);
            }
            // Set newLSN variable
            Long newval;
            // If undoNextLSN is available, set newlsn value to that
            // Otherwise, use prevLSN to assign newlSN value
            if (ourrecord.getUndoNextLSN().isPresent()){newval=ourrecord.getUndoNextLSN().get();}else{newval = ourrecord.getPrevLSN().get();}
            // If new LSN is 0
            if (newval==0){
                // Cleanup transaction
                transactionTable.get(ourrecord.getTransNum().get()).transaction.cleanup();
                // End transaction
                end(ourval.getSecond());
                // Remove from transaction table (removed already from queue)
                transactionTable.remove(ourval.getSecond());
            }else{
                // If not 0, return the pair into the queue again
                ourqueue.add(new Pair<>(newval,ourval.getSecond()));
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
