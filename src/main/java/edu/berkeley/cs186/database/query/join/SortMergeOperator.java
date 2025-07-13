package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
              prepareRight(transaction, rightSource, rightColumnName),
              leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source isn't sorted, wraps the right source in a sort
     * operator. Otherwise, if it isn't materialized, wraps the right source in
     * a materialize operator. Otherwise, simply returns the right source. Note
     * that the right source must be materialized since we may need to backtrack
     * over it, unlike the left source.
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator implements Iterator<Record> {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();

            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            this.marked = false;
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

        /**
         * Sort-Merge Join核心算法：返回下一个匹配的记录组合
         * 算法原理：双指针遍历两个已排序的表，生成所有键值相等的记录组合
         */
        private Record fetchNextRecord() {
            if (leftRecord == null || rightRecord == null) {
                return null;
            }

            while (true) {
                int cmp = compare(leftRecord, rightRecord);
                
                if (cmp == 0) {
                    // 键值匹配：生成连接结果
                    if (!marked) {
                        // 首次匹配时标记右表位置，用于后续回溯
                        rightIterator.markPrev();
                        marked = true;
                    }
                    
                    Record result = leftRecord.concat(rightRecord);
                    
                    // 右表前进，继续寻找更多匹配
                    if (rightIterator.hasNext()) {
                        rightRecord = rightIterator.next();
                    } else {
                        // 右表遍历完，左表前进并重置右表到标记位置
                        // 这样新的左记录可以与所有匹配的右记录组合
                        if (leftIterator.hasNext()) {
                            leftRecord = leftIterator.next();
                            rightIterator.reset();
                            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                        } else {
                            leftRecord = null;
                        }
                    }
                    
                    return result;
                    
                } else if (cmp < 0) {
                    // 左值 < 右值：左表前进
                    if (leftIterator.hasNext()) {
                        leftRecord = leftIterator.next();
                        if (marked) {
                            // 重置右表到标记位置，让新左记录从头匹配
                            // 例如：左表[1,1,2] 右表[1,1,2] 
                            // 当左表第2个1来到时，右表已前进到2
                            // 必须重置让第2个1与右表的1匹配，避免丢失组合
                            rightIterator.reset();
                            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                            marked = false;
                        }
                    } else {
                        return null;
                    }
                    
                } else {
                    // 左值 > 右值：右表前进
                    if (rightIterator.hasNext()) {
                        rightRecord = rightIterator.next();
                    } else {
                        return null;
                    }
                    marked = false; // 清除标记，因为跳过了匹配区域
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
