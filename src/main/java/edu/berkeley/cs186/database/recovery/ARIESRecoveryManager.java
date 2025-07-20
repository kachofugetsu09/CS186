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
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        if (transactionEntry == null) {
            throw new IllegalArgumentException("Transaction " + transNum + " not found.");
        }

        long prevLSN = transactionEntry.lastLSN;

        LogRecord commitRecord = new CommitTransactionLogRecord(transNum, prevLSN);
        long commitLSN = logManager.appendToLog(commitRecord);

        flushToLSN(commitLSN);

        // Update transaction table entry
        transactionEntry.lastLSN = commitLSN;
        Transaction transaction = transactionEntry.transaction;
        transaction.setStatus(Transaction.Status.COMMITTING);


        return commitLSN;
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
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        if (transactionEntry == null) {
            throw new IllegalArgumentException("Transaction " + transNum + " not found.");
        }

        long prevLSN = transactionEntry.lastLSN;
        LogRecord abortRecord = new AbortTransactionLogRecord(transNum, prevLSN);
        long abortLSN = logManager.appendToLog(abortRecord);

        transactionEntry.lastLSN = abortLSN;
        Transaction transaction = transactionEntry.transaction;
        transaction.setStatus(Transaction.Status.ABORTING);
        return abortLSN;
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
        // TODO(proj5): implement

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        if (transactionEntry == null) {
            throw new IllegalArgumentException("Transaction " + transNum + " not found.");
        }
        Transaction transaction = transactionEntry.transaction;
        if (transaction.getStatus() == Transaction.Status.ABORTING) {
            rollbackToLSN(transNum, 0);
        }

        EndTransactionLogRecord endRecord = new EndTransactionLogRecord(transNum, transactionEntry.lastLSN);
        long endLSN = logManager.appendToLog(endRecord);
        transaction.cleanup();
        transaction.setStatus(Transaction.Status.COMPLETE);


        transactionTable.remove(transNum);
        return endLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
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
        // TODO(proj5) implement the rollback logic described above

        while(currentLSN > LSN){
            LogRecord logRecord = logManager.fetchLogRecord(currentLSN);
            if(logRecord.isUndoable()){
                // Get the compensation log record (CLR)
                LogRecord clr = logRecord.undo(transactionEntry.lastLSN);
                // Append the CLR to the log
                long clrLSN = logManager.appendToLog(clr);

                // Call redo on the CLR to perform the undo
                clr.redo(this, diskSpaceManager, bufferManager);

                // Update the lastLSN of the transaction
                transactionEntry.lastLSN = clrLSN;
                // Update the currentLSN to the next record to undo
                currentLSN = logRecord.getPrevLSN().orElse(0L);
            }
            else{
                // If the record is not undoable, we can just move to the previous record
                currentLSN = logRecord.getPrevLSN().orElse(0L);
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
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        if (transactionEntry == null) {
            throw new IllegalArgumentException("Transaction " + transNum + " not found.");
        }

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        
        long LSN =  logManager.appendToLog(record);
        transactionEntry.lastLSN = LSN;
        transactionTable.get(transNum).lastLSN = LSN;
        dirtyPage(pageNum, LSN);
        return LSN;
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


        rollbackToLSN(transNum, savepointLSN);

        return;
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

        //记录哪些页面是脏的
        Map<Long, Long> chkptDPT = new HashMap<>();
        //记录哪些事务是活跃的
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table

        //iterate through the dirtyPageTable and copy the entries.
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            // 检查是否还能添加更多条目
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, chkptTxnTable.size())) {
                //If at any point, copying the current record would cause the end checkpoint record to be too large,
                // an end checkpoint record with the copied DPT entries should be appended to the log.
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);

                // 清空DPT，继续下一批
                chkptDPT.clear();
            }
            // 添加当前条目
            chkptDPT.put(entry.getKey(), entry.getValue());
        }

        //iterate through the transaction table,
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            // and copy the status/lastLSN,
            Long transNum = entry.getKey();
            Transaction.Status status = entry.getValue().transaction.getStatus();
            Long lastLSN = entry.getValue().lastLSN;

            // outputting end checkpoint records only as needed.
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {

                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);

                // 清空所有临时表
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            // 添加当前事务信息
            chkptTxnTable.put(transNum, new Pair<>(status, lastLSN));
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
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
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
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
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
        Iterator<LogRecord> logIterator = logManager.scanFrom(LSN);

        while(logIterator.hasNext()){
            LogRecord logRecord = logIterator.next();

            //1.如果是事务操作的记录，如果不在事务表当中添加到事务表当中，更新事务的lastLSN
            if(logRecord.getTransNum().isPresent()){
                long transNum = logRecord.getTransNum().get();
                if (!transactionTable.containsKey(transNum)) {
                    Transaction transaction = newTransaction.apply(transNum);
                    startTransaction(transaction);
                }

                transactionTable.get(transNum).lastLSN = logRecord.getLSN();
            }

            //2.如果是页面操作相关的record，更新脏页表
            if(logRecord.getPageNum().isPresent()) {
                long pageNum = logRecord.getPageNum().get();
                LogType type = logRecord.getType();
                
                if (type == LogType.UPDATE_PAGE || type == LogType.UNDO_UPDATE_PAGE) {
                    dirtyPage(pageNum, logRecord.getLSN());
                } else if (type == LogType.FREE_PAGE || type == LogType.UNDO_ALLOC_PAGE) {
                    dirtyPageTable.remove(pageNum);
                }
                // ALLOC_PAGE 和 UNDO_FREE_PAGE 不需要操作
            }

            //3.事务状态变化的日志记录
            LogType type = logRecord.getType();
            if(type == LogType.COMMIT_TRANSACTION){
                long transNum = logRecord.getTransNum().get();
                TransactionTableEntry transactionEntry = transactionTable.get(transNum);
                transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
            }
            else if(type == LogType.ABORT_TRANSACTION){
                long transNum = logRecord.getTransNum().get();
                TransactionTableEntry transactionEntry = transactionTable.get(transNum);
                transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
            else if(type == LogType.END_TRANSACTION){
                long transNum = logRecord.getTransNum().get();
                TransactionTableEntry transactionEntry = transactionTable.get(transNum);
                transactionEntry.transaction.cleanup();
                transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                endedTransactions.add(transNum);
                transactionTable.remove(transNum);
            }
            else if(type == LogType.END_CHECKPOINT){
                EndCheckpointLogRecord checkpointRecord = (EndCheckpointLogRecord) logRecord;

                // 处理 DPT
                for(Map.Entry<Long, Long> entry : checkpointRecord.getDirtyPageTable().entrySet()) {
                    dirtyPageTable.put(entry.getKey(), entry.getValue());
                }

                // 处理事务表
                for(Map.Entry<Long, Pair<Transaction.Status, Long>> entry : checkpointRecord.getTransactionTable().entrySet()) {
                    long transNum = entry.getKey();
                    Transaction.Status checkpointStatus = entry.getValue().getFirst();
                    long checkpointLastLSN = entry.getValue().getSecond();

                    // 跳过已结束的事务
                    if (endedTransactions.contains(transNum)) {
                        continue;
                    }

                    // 如果事务表中没有此事务，创建并添加
                    if (!transactionTable.containsKey(transNum)) {
                        Transaction transaction = newTransaction.apply(transNum);
                        startTransaction(transaction);
                    }

                    TransactionTableEntry tableEntry = transactionTable.get(transNum);

                    // 更新 lastLSN（使用较大的值）
                    if (checkpointLastLSN >= tableEntry.lastLSN) {
                        tableEntry.lastLSN = checkpointLastLSN;
                    }

                    // 更新状态（检查状态转换是否合法）
                    Transaction.Status currentStatus = tableEntry.transaction.getStatus();
                    if (canChange(currentStatus, checkpointStatus)) {
                        // 如果检查点显示事务正在中止，而我们的内存表显示其正在运行，
                        // 我们应该将内存状态更新为恢复中止*，因为有可能从运行状态过渡到中止状态。* 如果检查点显示中止，请确保设置为恢复中止而不是中止
                        if (checkpointStatus == Transaction.Status.ABORTING) {
                            tableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        } else {
                            tableEntry.transaction.setStatus(checkpointStatus);
                        }
                    }
                }
            }
        }

        // 最后处理事务表中剩余的事务（在循环外部）
        Iterator<Map.Entry<Long, TransactionTableEntry>> iterator = transactionTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, TransactionTableEntry> entry = iterator.next();
            long transNum = entry.getKey();
            TransactionTableEntry tableEntry = entry.getValue();
            Transaction.Status status = tableEntry.transaction.getStatus();

            if (status == Transaction.Status.COMMITTING) {
                //状态为 COMMITTING 的所有事务应被终止（ cleanup() ，状态设置为 COMPLETE ，写入结束事务记录，并从事务表中移除）。
                tableEntry.transaction.cleanup();
                tableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                long endLSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, tableEntry.lastLSN));
                iterator.remove();
            } else if (status == Transaction.Status.RUNNING) {
                //状态为 RUNNING 的所有事务应移入 RECOVERY_ABORTING 状态，并写入中止事务记录。
                tableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                long abortLSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, tableEntry.lastLSN));
                tableEntry.lastLSN = abortLSN;
            }
            // 对于处于 RECOVERY_ABORTING 状态的事务，无需执行任何操作。
        }
        return;
    }

    private boolean canChange(Transaction.Status currentStatus, Transaction.Status checkpointStatus) {
        if (currentStatus == checkpointStatus) {
            return false;
        }

        switch (currentStatus) {
            case RUNNING:
                return checkpointStatus == Transaction.Status.COMMITTING ||
                        checkpointStatus == Transaction.Status.ABORTING;

            case COMMITTING:
                return checkpointStatus == Transaction.Status.COMPLETE;

            case ABORTING:
            case RECOVERY_ABORTING:
                return checkpointStatus == Transaction.Status.COMPLETE;

            case COMPLETE:
                return false;

            default:
                return false;
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {

        if (dirtyPageTable.isEmpty()) {
            return;
        }
        // TODO(proj5): implement
        long startLSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> logIterator = logManager.scanFrom(startLSN);
        while(logIterator.hasNext()){
            LogRecord record = logIterator.next();
            if(record.isRedoable()){
                if(record.getType() == LogType.ALLOC_PART|| record.getType() == LogType.FREE_PART ||
                        record.getType() == LogType.UNDO_ALLOC_PART || record.getType() == LogType.UNDO_FREE_PART) {

                    record.redo(this, diskSpaceManager, bufferManager);
                } else if (record.getType() == LogType.ALLOC_PAGE || record.getType() == LogType.UNDO_FREE_PAGE) {

                    record.redo(this, diskSpaceManager, bufferManager);
                }
                else if(record.getType() == LogType.UPDATE_PAGE|| record.getType() == LogType.UNDO_UPDATE_PAGE ||
                        record.getType() == LogType.UNDO_ALLOC_PAGE || record.getType() == LogType.FREE_PAGE ){
                    if(record.getPageNum().isPresent() && dirtyPageTable.containsKey(record.getPageNum().get())) {
                        if(record.getLSN() >= dirtyPageTable.get(record.getPageNum().get())) {
                            long pageNum =record.getPageNum().get();

                            Page page = bufferManager.fetchPage(new DummyLockContext(),pageNum);
                            try{
                                if(page.getPageLSN() < record.getLSN()){
                                    record.redo(this, diskSpaceManager, bufferManager);
                                }
                            }finally {
                                page.unpin();
                            }
                        }
                    }
                }
            }
        }
        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        //First, a priority queue is created sorted on lastLSN of all aborting transactions.
        PriorityQueue<Pair<Long, TransactionTableEntry>> undoQueue = new PriorityQueue<>(
                new PairFirstReverseComparator<>());
        for (TransactionTableEntry entry : transactionTable.values()) {
            if (entry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                undoQueue.add(new Pair<>(entry.lastLSN, entry));
            }
        }
        // Then, always working on the largest LSN in the priority queue until we are done,
        while(!undoQueue.isEmpty()){
            Pair<Long, TransactionTableEntry> pair = undoQueue.poll();
            long currentLSN = pair.getFirst();
            TransactionTableEntry entry = pair.getSecond();
            LogRecord record = logManager.fetchLogRecord(currentLSN);
            
            if(record.isUndoable()){
                //如果记录可撤销，我们写出 CLR 并撤销它.创建 CLR 时，prevLSN 应该是事务当前的 lastLSN
                LogRecord clr = record.undo(entry.lastLSN);
                long clrLSN = logManager.appendToLog(clr);
                
                // 立即更新事务表中的 lastLSN
                entry.lastLSN = clrLSN;
                
                // 执行 undo 操作
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            
            //如果记录有 undoNextLSN，就用它的 undoNextLSN 替换集合中的 LSN；否则用 prevLSN 替换。
            long nextLSN = record.getUndoNextLSN().orElse(record.getPrevLSN().orElse(0L));
            //如果前一步的 LSN 为 0，则结束事务，将其从集合和事务表中移除。
            if(nextLSN == 0){
                // 则结束事务，将其从集合和事务表中移除。
                entry.transaction.cleanup();
                entry.transaction.setStatus(Transaction.Status.COMPLETE);
                long endLSN = logManager.appendToLog(new EndTransactionLogRecord(entry.transaction.getTransNum(), entry.lastLSN));
                transactionTable.remove(entry.transaction.getTransNum());
            } else {
                undoQueue.add(new Pair<>(nextLSN, entry));
            }
        }
        return;
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
