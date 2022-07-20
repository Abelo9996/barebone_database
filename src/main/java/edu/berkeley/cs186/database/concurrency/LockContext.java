package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException, UnsupportedOperationException {
        // TODO(proj4_part2): implement
            // If context is readonly
            if (readonly){
                // Throw error
                throw new UnsupportedOperationException("Context is readonly");
            }
            // If lock is already held by the transaction
            if (getEffectiveLockType(transaction) == lockType){
                // Throw error
                throw new DuplicateLockRequestException("lock is already held by the transaction");
            }
            // If parent is null or can be a parent
            if (parent == null || LockType.canBeParentLock(parentContext().getEffectiveLockType(transaction), lockType)){
                // If no childlocks assocatied with transaction
                if (!numChildLocks.containsKey(transaction.getTransNum())){
                    // Set the number to 0
                    numChildLocks.put(transaction.getTransNum(), 0);
                }
                // Otherwise, if it exists and the parent is not null
                if (parent != null){
                    // Get the new size of child locks, get the number of children of parent + 1
                    int newsize = parent.numChildLocks.get(transaction.getTransNum())+1;
                    // Set the new amount
                    parent.numChildLocks.put(transaction.getTransNum(), newsize);
                }
            }else{
                // If request is invalid, throw error
                throw new InvalidLockException("Request is invalid");
            }
            // Lastly, call acquire to the new transaction
            lockman.acquire(transaction, getResourceName(), lockType);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // If context is readonly
        if (readonly){
            // Throw Error
            throw new UnsupportedOperationException("Context is readonly");
        }
        // If no lock on name is held by transaction
        if (getEffectiveLockType(transaction) == null){
            // Throw Error
            throw new NoLockHeldException("no lock on name is held by transaction");
        }
        // Create iterator for children's values
        Iterator<LockContext> ouriter = children.values().iterator();
        // While the iterator has a new value to check
        while (ouriter.hasNext()){
            // Get the first LockContext
            LockContext ourlcx = ouriter.next();
            // System.out.println(getExplicitLockType(transaction));
            // System.out.println(LockType.parentLock(ourlcx.getExplicitLockType(transaction)));
            // Check if multigranularity is violated
            if (getExplicitLockType(transaction) == LockType.parentLock(ourlcx.getExplicitLockType(transaction)) && getExplicitLockType(transaction) != ourlcx.getExplicitLockType(transaction)){
                // Throw error
                throw new InvalidLockException("Violates multigranularity constraints");
            }
        }

        // If parent is not null
        if (parent != null){
            // Find new size by subtracting parent's number of children by 1
            int newsize = parent.numChildLocks.get(transaction.getTransNum())-1;
            // Set new amount of children locks
            parent.numChildLocks.put(transaction.getTransNum(), newsize);
        }
        // System.out.println("IMIN");
        // Finally, release the transaction lock
        // System.out.println(transaction);
        lockman.release(transaction, name);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // Store initial lock type of current level for use later after promoting lock
        LockType initlocktype = getExplicitLockType(transaction);
        // If context is readonly
        if (readonly){
            // Throw error
            throw new UnsupportedOperationException("context is readonly");
        }
        // If transaction does not have a lock
        if (lockman.getLocks(transaction).isEmpty()){
            // Throw error
            throw new NoLockHeldException("transaction has no lock");
        }
        // If transaction already has a newlocktype lock
        if (getEffectiveLockType(transaction) ==  newLockType) {
            // Throw error
            throw new DuplicateLockRequestException("Transaction already has a LockType lock");
        }
        // if the requested lock type is not a promotion or promoting would cause the lock manager to enter an invalid state
        if (parent!=null && ((newLockType == LockType.SIX && hasSIXAncestor(transaction)) || (!LockType.substitutable(newLockType,getExplicitLockType(transaction)) || newLockType == getExplicitLockType(transaction)))){
            // Throw error
            throw new InvalidLockException("Invalid promotion");
        }
        // Promote new lock of given transaction
        lockman.promote(transaction, name, newLockType);
        // Check if B is SIX and A is IS/IX/S
        if ((initlocktype == LockType.IS || initlocktype == LockType.IX || initlocktype == LockType.S) && newLockType == LockType.SIX){
            // Iterate through all the sisDescendants using our helper function
            // System.out.println(sisDescendants(transaction));
            for (ResourceName ourres:sisDescendants(transaction)){
                // Release the lock using the given resource
                lockman.release(transaction, ourres);
                // System.out.println(ourres);
                if (ourres.toString().contains("page")){
                    String ourstr = ourres.toString().split("/")[ourres.toString().split("/").length-2];
                    lockman.databaseContext().childContext(ourstr).numChildLocks.put(transaction.getTransNum(), lockman.databaseContext().childContext(ourstr).numChildLocks.get(transaction.getTransNum())-1);
                }else if (ourres.toString().contains("/table")){
                    lockman.databaseContext().numChildLocks.put(transaction.getTransNum(), lockman.databaseContext().numChildLocks.get(transaction.getTransNum())-1);
                }
                // System.out.println(lockman.databaseContext().numChildLocks);
            }
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException,UnsupportedOperationException {
        // TODO(proj4_part2): implement
        // Set array to iterate through children values and place lock names
        List<ResourceName> ourarr = new ArrayList<>();
        // Add current name to defined array above
        ourarr.add(name);
        // If transaction has no lock at this level
        if (lockman.getLocks(transaction).isEmpty()){
            // Throw error
            throw new NoLockHeldException("transaction has no lock at this level");
        }
        // If context is readonly
        if (readonly){
            // Throw error
            throw new UnsupportedOperationException("Context is readonly");
        }
        // As long as we have identified an S/X lock at bottom level
        if (RetEssentialLock(transaction) != 0){
            // Iterate through all the children values
            for (LockContext ourlock: children.values()){
                // If the current lock type is No Lock Type
                if (ourlock.getExplicitLockType(transaction) != LockType.NL){
                    // Find the new number of childlocks by subtracting 1
                    int oursize = numChildLocks.get(transaction.getTransNum()) -1;
                    // Set the new size to the number of children locks
                    numChildLocks.put(transaction.getTransNum(),oursize);
                    // Add the lock name to the defined array from above
                    ourarr.add(ourlock.name);
                }
            }
        // Set the default jump value to NoLock Type
        LockType jumpval = LockType.NL;
        // If returned value from helper is 1 (indicating S lock type)
        if (RetEssentialLock(transaction) == 1){
            // Set jump to S lock type
            jumpval = LockType.S;
        // If returned value from helper is -1 (indicating X lock type)
        }else if (RetEssentialLock(transaction) == -1) {
            // Set jump to X lock type
            jumpval = LockType.X;
        }
        // System.out.println(ouarr);
            // Finally, call acquireandrelease to the new jump lock type we got from helper
            lockman.acquireAndRelease(transaction, name, jumpval, ourarr);    
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        // If transaction is null, then just return No Lock Type
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        // Return the lock type using our lock manager function
        return lockman.getLockType(transaction, name);
    }

    /* Helper function to check if given table's root is an X or S lock type */
    public int RetEssentialLock(TransactionContext transaction){
        // Set our integer value to 0
        int retS = 0;
        // If the lock types are SIX or IX
        if(getExplicitLockType(transaction) == LockType.SIX || getExplicitLockType(transaction) == LockType.IX){
            // Set retS to -1, indicating that it is an X lock
            retS = -1;
        }
        // Otherwise, if it is IS
        else if (getExplicitLockType(transaction) == LockType.IS){
            // Set to 1, indicating that it is an S lock
            retS = 1;
        }
        // Otherwise, iterate through all the children values
        for (LockContext ourcont: children.values()){
            // If bottom one is S
            if (ourcont.getExplicitLockType(transaction) == LockType.S && retS != -1){
                // Set retS to 1, indicating that it is an S lock
                retS = 1;
            // Otherwise, if bottom one is X
            }else if (ourcont.getExplicitLockType(transaction) == LockType.X){
                // Set retS to -1, indicating that it is an X lock
                retS = -1;
            }
        }
        // Finally, return retS for use in our escalate function
        return retS;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        // Return NL Type if transaction is null
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        // If parent is null
        if (parent == null){
            // Return the current lock type
            return lockman.getLockType(transaction, name);
        }else{
            // If the parent's lock is X
            if (parent.getEffectiveLockType(transaction) == LockType.X){
                // Return X
                return LockType.X;
            }
            // If parent's lock is SIX or X
            if (parent.getEffectiveLockType(transaction) == LockType.S || parent.getEffectiveLockType(transaction) == LockType.SIX){
                // Return S
                return LockType.S;
            }
        }
        // Return the current lock type if nothing found
        return lockman.getLockType(transaction, name);
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // If does not hold a SIX at an ancestor or is null
        if ( parent == null || (!parent.hasSIXAncestor(transaction) && parent.getExplicitLockType(transaction) != LockType.SIX)){
            // Return false
            return false;
        }else{
            // Else, return true
            return true;
        }
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // Set iterator value
        int i = 0;
        // Create an iterator of all the locks' values
        Iterator<Lock> ouriter = lockman.getLocks(transaction).iterator();
        // Create array of resource names to return
        List<ResourceName> x = new ArrayList<>();
        // Iterate through all locks' values
        while (i < lockman.getLocks(transaction).size()){
            // Get the lock context
            Lock ourlock = ouriter.next();
            // System.out.println(ourlock);
            // System.out.println("YES");
            // Get its lock type
            LockType ourlocktype = ourlock.lockType;
            // Get the resource name
            ResourceName ourname = ourlock.name;
            // Check if condition satisfied
            if (ourlocktype == LockType.IS || ourlocktype == LockType.S){
                // Add to array
                x.add(ourname);
            }
            // Increment iterator value
            ++i;
        }
        // Return array
        return x;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}
