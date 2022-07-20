package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /* Helper function to see current lock */
        public Lock lockreturn(long transaction){
            // Iterate through all locks from locks, if transactions match, return
            for (Lock ourlock:locks){if(transaction == ourlock.transactionNum){return ourlock;}}
            // Else, nothing returned
            return null;
        }

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            // Sync
            synchronized (this){
                // Iterate through pre-existing locks
                for (Lock ourlock : locks){
                    // Checks if locktype is not compatible with pre-existing locks and if the transaction numbers are not the same
                    if (ourlock.transactionNum != except && !LockType.compatible(ourlock.lockType, lockType)){
                        // return false
                        return false;
                    }
                }
            }
            // Otherwise, true
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
	        // TODO(proj4_part1): implement	
            // Sync
            synchronized (this){
                // Iterate through pre-existing locks
                for (Lock ourlock: locks){
                    // If transaction already has a lock
                    if (ourlock.transactionNum.equals(lock.transactionNum)){
                        // Update lock
                        locks.set(locks.indexOf(ourlock),lock);
                        // End call
                        return;
                    }
                }
                // Give transaction the lock
                locks.add(lock);
            }
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
	        // TODO(proj4_part1): implement
            // Sync
            synchronized(this){
                // Release the lock
                locks.remove(lock);
                // Remove lock from transactionLocks
                transactionLocks.get(lock.transactionNum).remove(lock);
                // Process the queue
                processQueue();
            }
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
	        // TODO(proj4_part1): implement
            // Sync
            synchronized(this){
                // If addFront is false
                if (!addFront){
                    // Add to end of queue
                    waitingQueue.addLast(request);
                }else{
                    // Otherwise, add to front of queue
                    waitingQueue.addFirst(request);
                }
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement
            // Sync
            synchronized(this){
                // While iterator has a next element
                while(!waitingQueue.isEmpty()){
                    // Check if lock can be granted
                    if (checkCompatible(waitingQueue.getFirst().lock.lockType,waitingQueue.getFirst().lock.transactionNum)){
                        // Grant lock
                        grantOrUpdateLock(waitingQueue.getFirst().lock);
                        // Add lock to the respective transaction number
                        if (transactionLocks.get(waitingQueue.getFirst().lock.transactionNum) == null){
                            // Put a new ArrayList if transactionlock empty
                            transactionLocks.put(waitingQueue.getFirst().lock.transactionNum,new ArrayList<>());
                        }
                        // Add lock to transactionLocks
                        transactionLocks.get(waitingQueue.getFirst().transaction.getTransNum()).add(waitingQueue.getFirst().lock);
                        // Remove transaction lock
                        for (int i=0; i < waitingQueue.getFirst().releasedLocks.size();++i){transactionLocks.get(waitingQueue.getFirst().lock.transactionNum).remove(waitingQueue.getFirst().releasedLocks.get(i));}
                        // Transaction unblocked.
                        waitingQueue.getFirst().transaction.unblock();
                        // Remove request from queue
                        waitingQueue.removeFirst();
                    }else{
                        // Stop when lock can't be granted
                        break;
                    }
                }
            }
        }

	    /**
         * Gets the type of lock `transaction` has on this resource.
         * Helper function that returns 1 if lock exists, else -1.
         */
        public int getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            // Sync
            synchronized (this){
                // Iterate through pre-existing locks
                for (Lock ourlock:locks){
                    // If transaction number matches
                    if (ourlock.transactionNum == transaction){
                        // return 1
                        return 1;
                    }
                }
            }
            // else -1
            return -1;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType1(long transaction) {
            // TODO(proj4_part1): implement
            // Sync
            synchronized (this){
                // Iterate through pre-existing locks
                for (Lock ourlock:locks){
                    // If transaction number matches
                    if (ourlock.transactionNum == transaction){
                        // return its locktype
                        return ourlock.lockType;
                    }
                }
            }
            // otherwise, No locks
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        // Set to null if transaction is not there
        transactionLocks.putIfAbsent(transaction.getTransNum(), new ArrayList<>());
        // Create new lock to use for later
        Lock ourlock = new Lock(name, lockType, transaction.getTransNum());
        // Set boolean if it should be blocked
        boolean shouldBlock = false;
        // Set array to store locks to avoid ConcurrentModificationException
        List<Lock> ourarr = new ArrayList<>();
        // Sync
        synchronized (this) {
            // System.out.println(releaseNames);
            // Iterate through all the releaseNames
            for (ResourceName ourres: releaseNames){
                // If no lock in transaction of respective resource name
                if(getResourceEntry(ourres).getTransactionLockType(transaction.getTransNum()) == -1){
                    // Throw error
                    throw new NoLockHeldException("Transaction doesn't hold on every name in releaseNames");
                }
            }
            // Iterate through all the transaction locks
            for (Lock ourlock1 : transactionLocks.get(transaction.getTransNum())) {
                // If locks are the same
                if (ourlock.equals(ourlock1)) {
                    // Return error since name is held by transaction already
                    throw new DuplicateLockRequestException("Lock on name is already held by transaction");
                }
            }
            if (getResourceEntry(name).checkCompatible(lockType, transaction.getTransNum())){
                // Add lock to the respective transaction number
                transactionLocks.get(transaction.getTransNum()).add(ourlock);
                // Grant/Update lock for the resource name
                getResourceEntry(name).grantOrUpdateLock(ourlock);
                // Iterate through all the resource names of releaseNames
                for (ResourceName ourres: releaseNames){
                    // Iterate through all the locks for the respective resource name
                    for (Lock ourlock1:resourceEntries.get(ourres).locks){
                        // If lock isn't the same to the value we are iterating over
                        if (ourlock1 != ourlock && transaction.getTransNum() == ourlock1.transactionNum){
                            // Then add lock to the array for later use
                            ourarr.add(ourlock1);
                        }
                    }
                    // Now iterate through the created array
                    for (Lock ourlock1: ourarr){
                        // Release the lock of each value within our created array
                        // System.out.println(ourlock1);
                        resourceEntries.get(ourres).releaseLock(ourlock1);
                    }
                    // Reset the array for next iteration.
                    ourarr = new ArrayList<>();
                }
            }else{
                // Should block
                shouldBlock = true;
                // Add to queue at the back
                getResourceEntry(name).addToQueue(new LockRequest(transaction, ourlock), true);
                // Prepare block
                transaction.prepareBlock();
            }
        }
        // If it should be blocked
        if (shouldBlock) {
            // Block the transaction
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
	    // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        // Create the lock with the given information
        Lock ourlock = new Lock(name, lockType, transaction.getTransNum());
        boolean shouldBlock = false;
        // Sync
        synchronized (this) {
            // Put an empty array if no transaction exists
            transactionLocks.putIfAbsent(transaction.getTransNum(), new ArrayList<>());
            // init iterator
            int i = 0;
            // iterate through all transaction locks
            while (i < transactionLocks.get(transaction.getTransNum()).size()){
                // If there is indeed a lock on name held by the transaction, return an error
                if (transactionLocks.get(transaction.getTransNum()).get(i).equals(ourlock)){
                    // Throw DuplicateLockRequestException error
                    throw new DuplicateLockRequestException("Lock on 'name' is  held by 'transaction'");
                }
                // increment
                ++i;
            }
            // System.out.println("YES1234");
            // If it is not compatible or does have a waiting Queue
            if (!getResourceEntry(name).checkCompatible(lockType, transaction.getTransNum()) || !getResourceEntry(name).waitingQueue.isEmpty()){
                // Should block
                shouldBlock = true;
                // Add to queue at the back
                getResourceEntry(name).addToQueue(new LockRequest(transaction, ourlock), false);
                // Prepare block
                transaction.prepareBlock();
            }else{
                // Otherwise, grant lock
                getResourceEntry(name).grantOrUpdateLock(ourlock);
                // Add lock to the respective transaction number
                transactionLocks.get(transaction.getTransNum()).add(ourlock);
            }
        }
        // System.out.println("YES1234567");
        // If shouldBlock is true
        if (shouldBlock) {
            // System.out.println("testing123");
            // Then block transaction out
            transaction.block();
            // System.out.println("testing12345");
        }
        // System.out.println("testing");
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
	    // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            // If no lock on name is held by the given transaction, return error
            if (getResourceEntry(name).getTransactionLockType(transaction.getTransNum()) == -1) {
                // Throw NoLockHeldException error
                throw new NoLockHeldException("No lock on 'name' is  held by 'transaction'");
            }
            // Get all the locks for that transaction
            List<Lock> ourlocks = getLocks(transaction);
            // Iterate through all the locks
            for (Lock ourlock:ourlocks){
                // If the names match (note to self: use .equals instead of == for 2PL tests)
                if (name.equals(ourlock.name)){
                    // Remove lock from existing locks
                    ourlocks.remove(ourlock);
                    // Update the locks for the given transaction
                    transactionLocks.put(transaction.getTransNum(),ourlocks);
                    // Release lock
                    getResourceEntry(name).releaseLock(ourlock);
                    // End call
                    break;
                }
            }
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        // Boolean for blocking
        boolean shouldBlock = false;
        // Array for items to queue
        List<Lock> ourarr = new ArrayList<>();
        // Add the current lock
        ourarr.add(getResourceEntry(name).lockreturn(transaction.getTransNum()));
        // New lock for implementation
        Lock ourlock = new Lock(name, newLockType, transaction.getTransNum());
        // Our current lock type
        LockType currlocktype = getResourceEntry(name).getTransactionLockType1(transaction.getTransNum());
        synchronized (this) {
            // Check if transaction has a newlocktype on name
            if (getResourceEntry(name).getTransactionLockType1(transaction.getTransNum()) ==  newLockType) {
                // Throw error if not
                throw new DuplicateLockRequestException("Transaction already has a LockType lock on name");
            }
            // Check if promotion is valid or not
            if (!LockType.substitutable(newLockType,currlocktype) || newLockType == currlocktype){
                // Throw error if not
                throw new InvalidLockException("Not a valid promotion");
            }
            // Check if transaction has a lock on name
            if (getResourceEntry(name).getTransactionLockType(transaction.getTransNum()) == -1) {
                // Throw error if not
                throw new NoLockHeldException("No lock on 'name' is  held by 'transaction'");
            }
            // If not compatible
            if (!getResourceEntry(name).checkCompatible(newLockType, transaction.getTransNum())){
                // Prepare the block
                transaction.prepareBlock();
                // Set to block the transaction
                shouldBlock = true;
                // Add request to queue
                getResourceEntry(name).addToQueue(new LockRequest(transaction, ourlock, ourarr), true);
            }else{
                // Grant/Update our new lock
                getResourceEntry(name).grantOrUpdateLock(ourlock);
                // Remove the current lock from transactionLocks
                transactionLocks.get(transaction.getTransNum()).remove(getResourceEntry(name).lockreturn(transaction.getTransNum()));
                // Now add the new lock to transactionLocks
                transactionLocks.get(transaction.getTransNum()).add(ourlock);
            }
        }
        // If it is blockable
        if (shouldBlock) {
            // Block the transaction
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
	    // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        // get the lock type from the resourceEntry
        LockType result = resourceEntry.getTransactionLockType1(transaction.getTransNum());
        return result;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
