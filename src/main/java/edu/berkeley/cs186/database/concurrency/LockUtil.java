package edu.berkeley.cs186.database.concurrency;
import java.util.*;
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);
        // Set up array for sisDescendants later on
        List<ResourceName> ourarr = new ArrayList<>();
        // Add our name to the defined array above
        ourarr.add(lockContext.name);
        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        // TODO(proj4_part2): implement
        // If the locktype is not substitutable and not the same as the current lock
        if (!LockType.substitutable(effectiveLockType, requestType) && effectiveLockType != requestType){
            // If we want to add an X lock
            if (requestType.equals(LockType.X)){
                // We acquire all locks for parents
                if(AcquireAllLocks(transaction, lockContext, LockType.IX) == 1){
                }else{
                // If our current lock is not NL
                if (!explicitLockType.equals(LockType.NL)){
                    // Escalate the lock
                    lockContext.escalate(transaction);
                    // Promote it to the new lock type
                    lockContext.promote(transaction, requestType);
                }else{
                    // Otherwise, if there is an NL, just acquire the lock
                    lockContext.acquire(transaction, requestType);
                }
            // If we want to add an S lock
            }}else if (requestType.equals(LockType.S)){
                // We acquire all locks for parents
                if(AcquireAllLocks(transaction, lockContext, LockType.IS) == 1){
                }else{
                // If our current lock is not NL
                if (!explicitLockType.equals(LockType.NL)){
                    // If it is not IX
                    if (!explicitLockType.equals(LockType.IX)){
                        // We escalate it
                        lockContext.escalate(transaction);}else{
                            // Otherwise, iterate through all the sisDescendants of the transaction
                            for (Lock ourlock:lockContext.lockman.getLocks(transaction)){
                                // If condition for LockManager's sisDescendants are correct, add the name of the lock to the array for acquireandrelease
                                if(ourlock.name.isDescendantOf(lockContext.name) && (ourlock.lockType.equals(LockType.S) || ourlock.lockType.equals(LockType.IS))){ourarr.add(ourlock.name);}
                            }
                            // Now, acquire and release a lock of SIX with our defined array of names
                            lockContext.lockman.acquireAndRelease(transaction, lockContext.name, LockType.SIX, ourarr);    
                        }
                }else{
                    // If our current lock is NL, just acquire the lock
                    lockContext.acquire(transaction, requestType);
                }
            }
        }
    }
}

    public static int AcquireAllLocks(TransactionContext transaction,LockContext lockContext, LockType requestType) {
        // If parent is null, no work is done, so return 0
        if (lockContext.parent == null){return 0;}
        // If substitutable but the parent's locks are not IS or IX, end call by returning 1 to satisfy the total traversal of ancestors.
        if(LockType.substitutable(lockContext.parent.lockman.getLockType(transaction, lockContext.parent.name), requestType) && !lockContext.parent.lockman.getLockType(transaction, lockContext.parent.name).equals(LockType.IS) && !lockContext.parent.lockman.getLockType(transaction, lockContext.parent.name).equals(LockType.IX)){return 1;}
        // If the given locktype is not substitutable
        else if(!LockType.substitutable(lockContext.parent.lockman.getLockType(transaction, lockContext.parent.name), requestType)){
            // We acquire the parent's parent's locks, which will finish if returned by 1, since 1 indicates full traversal through ancestors
            if (AcquireAllLocks(transaction, lockContext.parent, requestType) == 1){return 1;}
            // If the parent's locktype is not NL
            if(!lockContext.parent.lockman.getLockType(transaction, lockContext.parent.name).equals(LockType.NL)){
                // If parent is IX and child is S, promote parent to SIX
                if (lockContext.parent.lockman.getLockType(transaction, lockContext.parent.name).equals(LockType.IX) && requestType.equals(LockType.S)){lockContext.parent.promote(transaction, LockType.SIX);return 0;}
                // If parent is S and child is IX, promote parent to SIX
                if (requestType.equals(LockType.IX) && lockContext.parent.lockman.getLockType(transaction, lockContext.parent.name).equals(LockType.S)){lockContext.parent.promote(transaction, LockType.SIX);return 0;}
                // Otherwise, if SIX case is not satisfied, just promote whatever lock it is to be promoted to
                else{lockContext.parent.promote(transaction, requestType);}
            }else{
                // Otherwise, if the parent's locktype is NL, just acquire the lock on the parent
                lockContext.parent.acquire(transaction, requestType);
            }
        }
        // Return 0 if nothing indicated the full traversal of ancestors
        return 0;
    }
}
