package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextLeftBlock() {
            if(!this.leftSourceIterator.hasNext()){
                this.leftBlockIterator = null;
                this.leftRecord = null;
                return;
            }
            this.leftBlockIterator = QueryOperator.getBlockIterator(
                    this.leftSourceIterator, getLeftSource().getSchema(), numBuffers-2
            );

            if(this.leftBlockIterator.hasNext()){
                // 当处理下一个右页时，重置回到左块开始，重新扫描整个左块
                this.leftBlockIterator.markNext();
                this.leftRecord = this.leftBlockIterator.next();
            } else {
                this.leftRecord = null;
            }
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextRightPage() {
            if(!this.rightSourceIterator.hasNext()){
                return;
            }
            this.rightPageIterator = QueryOperator.getBlockIterator(
                    this.rightSourceIterator, getRightSource().getSchema(), 1
            );
            
            // 标记右页的起始位置，以便重置，在同一个左块内，处理不同的左记录时需要重新扫描当前右页
            if(this.rightPageIterator != null) {
                this.rightPageIterator.markNext();
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * ppt中的讲解：
         * for each rpage in R:
         *      for each spage in S:
         *          for each rtuple in rpage:
         *              for each stuple in spage:
         *                  if join_condition(rtuple, stuple):
         *                      add <rtuple, stuple> to result buffer
         */
        private Record fetchNextRecord() {
            while(true) {
                // Case 1: 右侧页面迭代器有一个值要生成
                if(this.rightPageIterator != null && this.rightPageIterator.hasNext()){
                    Record rightRecord = this.rightPageIterator.next();
                    if(this.leftRecord != null && compare(this.leftRecord, rightRecord) == 0){
                        return this.leftRecord.concat(rightRecord);
                    }
                }

                // Case 2:右侧页面迭代器没有值可以生成，但左侧块迭代器有
                else if(this.leftBlockIterator != null && this.leftBlockIterator.hasNext()){
                    this.leftRecord = this.leftBlockIterator.next();
                    //因为左块迭代器做了移动，每一个迭代器的行为应该是匹配右侧每一个值，所以这里要重置右迭代器。
                    if(this.rightPageIterator != null) {
                        this.rightPageIterator.reset();
                    }
                }
                // Case 3: 右侧页面和左侧块迭代器都没有值可以生成，但右侧页面还有更多
                else {
                    //首先获取下一个右侧页面
                    this.fetchNextRightPage();
                    if(this.rightPageIterator != null && this.rightPageIterator.hasNext()){
                        // 重置左块到开始位置，重新扫描左块
                        if(this.leftBlockIterator != null) {
                            this.leftBlockIterator.reset();
                            if(this.leftBlockIterator.hasNext()) {
                                this.leftRecord = this.leftBlockIterator.next();
                            } else {
                                this.leftRecord = null;
                            }
                        }
                    }
                    // Case 4: 右页和左块迭代器都没有值，也没有更多的右页，但仍有左块
                    else {
                        //获取新的左块
                        this.fetchNextLeftBlock();
                        if(this.leftBlockIterator == null || !this.leftBlockIterator.hasNext()){
                            return null;
                        }
                        // 重置右表到开始位置，重新做某一个左块对一整个右表的匹配
                        this.rightSourceIterator.reset();
                        this.fetchNextRightPage();
                    }
                }
            }
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
    }
}
