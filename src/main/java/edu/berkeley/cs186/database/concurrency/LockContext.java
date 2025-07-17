package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        if(this.readonly){
            throw new UnsupportedOperationException("Read only locks are not supported");
        }
        
        // 检查是否尝试获取 NL 锁
        if (lockType == LockType.NL) {
            throw new InvalidLockException("Cannot acquire NL lock, use release instead");
        }
        
        long transNum = transaction.getTransNum();
        if(!lockman.getLockType(transaction,name).equals(LockType.NL)){
            throw new DuplicateLockRequestException("Transaction " + transNum + " already holds a lock on " + name);
        }

        if (parent != null) {
            LockType parentLockType = lockman.getLockType(transaction, parent.getResourceName());
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("Parent lock " + parentLockType + 
                    " does not allow child lock " + lockType);
            }
        }

        lockman.acquire(transaction, name, lockType);

        if (parent != null) {
            parent.numChildLocks.put(transNum,
                    parent.numChildLocks.getOrDefault(transNum, 0) + 1);
        }


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
        if(this.readonly){
            throw new UnsupportedOperationException("Read only locks are not supported");
        }
        long transNum = transaction.getTransNum();
        LockType lockType = lockman.getLockType(transaction, name);
        if(lockType.equals(LockType.NL)) {
            throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);
        }

        // 检查是否有子锁阻止释放
        int childLockCount = numChildLocks.getOrDefault(transNum, 0);
        if (childLockCount > 0) {
            throw new InvalidLockException("Cannot release lock when child locks are held");
        }

        lockman.release(transaction, name);
        if(parent != null){
            int currentCount = parent.numChildLocks.getOrDefault(transNum, 0);
            if (currentCount > 0) {
                parent.numChildLocks.put(transNum, currentCount - 1);
            }
        }

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
        if(this.readonly){
            throw new UnsupportedOperationException("Read only locks are not supported");
        }
        long transNum = transaction.getTransNum();
        LockType currentLockType = lockman.getLockType(transaction, name);
        
        if(currentLockType.equals(LockType.NL)){
            throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);
        }

        if(currentLockType.equals(newLockType)){
            throw new DuplicateLockRequestException("Transaction " + transNum + " already holds " + newLockType + " lock");
        }

        if (!LockType.substitutable(newLockType, currentLockType)) {
            throw new InvalidLockException("Cannot promote " + currentLockType + " to " + newLockType);
        }

        if (newLockType == LockType.SIX && hasSIXAncestor(transaction)) {
            throw new InvalidLockException("Cannot promote to SIX with SIX ancestor");
        }
        
        // 如果升级到 SIX，需要释放所有 S/IS 后代锁
        if (newLockType == LockType.SIX && (currentLockType == LockType.IS || currentLockType == LockType.IX)) {
            List<ResourceName> descendants = sisDescendants(transaction);
            // 创建要释放的锁列表，包括当前锁和所有 S/IS 后代锁
            List<ResourceName> locksToRelease = new ArrayList<>(descendants);
            locksToRelease.add(name);
            
            lockman.acquireAndRelease(transaction, name, newLockType, locksToRelease);

            int releasedCount = descendants.size();
            int currentCount = numChildLocks.getOrDefault(transNum, 0);
            numChildLocks.put(transNum, Math.max(0, currentCount - releasedCount));
        } else {
            // 普通升级，不需要释放后代锁
            lockman.promote(transaction, name, newLockType);
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
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement

        if (readonly) {
            throw new UnsupportedOperationException("Read only locks are not supported");
        }
        long transNum = transaction.getTransNum();
        LockType currentLockType = lockman.getLockType(transaction, name);
        if (currentLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);
        }

        List<ResourceName> descendantLocks = getAllDescendantLocks(transaction);

        // 决定新锁类型
        LockType newLockType;
        
        if (descendantLocks.isEmpty()) {
            // 如果没有后代锁，根据当前锁类型决定升级
            if (currentLockType == LockType.IS) {
                newLockType = LockType.S;
            } else if (currentLockType == LockType.IX) {
                newLockType = LockType.X;
            } else {
                // 如果当前是 S 或 X 锁，不需要升级
                return;
            }
        } else {
            // 如果有后代锁，检查是否需要 X 锁
            boolean needX = false;
            for(ResourceName childLock : descendantLocks){
                LockType childLockType = lockman.getLockType(transaction, childLock);
                if(childLockType.equals(LockType.X) || childLockType.equals(LockType.IX) || childLockType.equals(LockType.SIX)) {
                    needX = true;
                    break;
                }
            }
            newLockType = needX ? LockType.X : LockType.S;
        }

        // 如果新锁类型和当前锁类型相同，不需要做任何改变
        if (currentLockType.equals(newLockType)) {
            return;
        }

        // 创建要释放的锁列表，包括当前锁和所有后代锁
        List<ResourceName> locksToRelease = new ArrayList<>(descendantLocks);
        locksToRelease.add(name);  // 包含当前锁，以便进行替换

        lockman.acquireAndRelease(transaction, name, newLockType, locksToRelease);

        // 清理子锁计数：需要遍历所有被释放的后代锁，并更新它们的父上下文
        for (ResourceName descendantName : descendantLocks) {
            LockContext descendantContext = LockContext.fromResourceName(lockman, descendantName);
            LockContext descendantParent = descendantContext.parentContext();
            
            // 从父上下文中减去这个子锁
            while (descendantParent != null && !descendantParent.name.equals(name)) {
                int currentCount = descendantParent.numChildLocks.getOrDefault(transNum, 0);
                if (currentCount > 0) {
                    descendantParent.numChildLocks.put(transNum, currentCount - 1);
                }
                descendantParent = descendantParent.parentContext();
            }
        }
        
        // 重置当前上下文的子锁计数（因为所有后代锁都被释放了）
        numChildLocks.put(transNum, 0);

    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement

        return lockman.getLockType(transaction, name);
    }

    private List<ResourceName> getAllDescendantLocks(TransactionContext transaction) {
        List<ResourceName> result = new ArrayList<>();
        for (Map.Entry<String, LockContext> entry : children.entrySet()) {
            LockContext child = entry.getValue();
            LockType lockType = child.lockman.getLockType(transaction, child.getResourceName());
            if (!lockType.equals(LockType.NL)) {
                result.add(child.getResourceName());
            }
            // 递归
            result.addAll(child.getAllDescendantLocks(transaction));
        }
        return result;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        
        // 首先检查显式锁
        LockType explicitLock = lockman.getLockType(transaction, name);
        if (!explicitLock.equals(LockType.NL)) {
            return explicitLock;
        }
        
        // 如果没有显式锁，检查祖先的隐式锁
        LockContext current = this.parent;
        while (current != null) {
            LockType ancestorLock = current.lockman.getLockType(transaction, current.getResourceName());
            if (ancestorLock == LockType.S || ancestorLock == LockType.X) {
                return ancestorLock;  // 祖先的 S/X 锁隐式授予当前级别相同的锁
            } else if (ancestorLock == LockType.SIX) {
                return LockType.S;  // SIX 锁隐式授予当前级别 S 锁
            }
            current = current.parent;
        }
        
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext current = this.parent;
        while (current != null) {
            LockType lockType = current.lockman.getLockType(transaction, current.getResourceName());
            if (lockType == LockType.SIX) {
                return true;
            }
            current = current.parent;
        }
        return false;
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

        List<ResourceName> result = new ArrayList<>();
        for (Map.Entry<String, LockContext> entry : children.entrySet()) {
            LockContext child = entry.getValue();
            LockType lockType = child.lockman.getLockType(transaction, child.getResourceName());
            if (lockType == LockType.S || lockType == LockType.IS) {
                result.add(child.getResourceName());
            }
            result.addAll(child.sisDescendants(transaction));
        }
        return result;
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

