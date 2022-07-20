package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        // Anything with NL is compatible
        }else if ( a == NL || b == NL){
            return true;
        // Anything with IS (other than IS) is compatible
        }else if ((a!=X && b!=X) && (a==IS || b==IS)){
            return true;
        // If a and b are equal, anything is compatible unless they are SIX or X
        }else if ((a!=X && a!=SIX) && (a==b)){
            return true;
        }
        // TODO(proj4_part1): implement
        // Otherwise, return false
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        // If the child is NL or parent is IX, then anything can be its parent.
        }else if (childLockType == NL || parentLockType == IX){
            return true;
        // If parent is IS with either IS or S as its child, then it can be its parent
        }else if (parentLockType == IS && (childLockType == IS || childLockType == S)){
            return true;
        // If parent is SIX with either IX or X as its child, then it can be its parent
        }else if (parentLockType == SIX && (childLockType == X || childLockType == IX)){
            return true;
        }
        // TODO(proj4_part1): implement
        // Otherwise, return false
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // If either the original value is the same as its substituted one (or NL), then it can be substituted
        if (required == NL || required == substitute){
            return true;
        // Handle certain test cases (using Sanity tests on Gradescope)
        } else if ((substitute == SIX && (required == IX || required == IS || required == S)) || (required == S && (substitute == X || substitute ==SIX)) || (required == IS && substitute == IX )){
            return true;
        }
        // Otherwise, return false.
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}
