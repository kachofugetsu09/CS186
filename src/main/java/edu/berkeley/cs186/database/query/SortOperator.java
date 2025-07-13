package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        Run run = makeRun();
        List<Record> recordList = new ArrayList<>();
        while(records.hasNext()){
            Record record = records.next();
            if (record == null) {
                continue; // Skip null records
            }
            recordList.add(record);

        }
        recordList.sort(comparator);
        run.addAll(recordList);
        return run;

    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement

        Run result = makeRun();

        PriorityQueue<Pair<Record,Integer>> pq = new PriorityQueue<>(runs.size(), new RecordPairComparator());

        List<BacktrackingIterator<Record>> iterators = new ArrayList<>();
        for(int i = 0; i < runs.size(); i++) {
            // 为第i个Run创建迭代器
            BacktrackingIterator<Record> it = runs.get(i).iterator();
            iterators.add(it);
            
            // 如果这个Run不为空，就取出第一个记录，连同Run的索引一起放入优先队列
            if (it.hasNext()) {
                Record record = it.next();
                pq.add(new Pair<>(record, i));
            }
        }
        
        // 第二步：不断从优先队列中取出最小的记录，直到所有记录都被处理完
        while(!pq.isEmpty()){
            // 从优先队列中取出当前最小的记录及其来源Run的索引
            Pair<Record, Integer> pair = pq.poll();
            Record record = pair.getFirst();    // 取出记录
            int runIndex = pair.getSecond();    // 取出该记录来自第几个Run
            
            // 将这个最小记录添加到结果中
            result.add(record);

            //因为我们取出了这个run中的记录，堆里面这个run的位置需要这个run的后继者来补充
            //所以我们需要检查这个Run的迭代器是否还有下一个记录，如果有就放进去
            BacktrackingIterator<Record> iterator = iterators.get(runIndex);
            if (iterator.hasNext()) {
                Record nextRecord = iterator.next();
                pq.add(new Pair<>(nextRecord, runIndex));
            }
        }
        
        return result;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        List<Run> result = new ArrayList<>();
        for(int i = 0; i < runs.size(); i += this.numBuffers - 1) {
            List<Run> batch = runs.subList(i, Math.min(i + this.numBuffers - 1, runs.size()));
            Run mergedRun = mergeSortedRuns(batch);
            result.add(mergedRun);
        }
        return result;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

        //PASS 0 - 创建初始的已排序runs
        List<Run> runs = new ArrayList<>();
        while(sourceIterator.hasNext()){
            // Pass 0阶段可以使用所有buffer来排序
            BacktrackingIterator<Record> blockIterator = getBlockIterator(sourceIterator, getSchema(), this.numBuffers);
            Run sortedRun = sortRun(blockIterator);
            runs.add(sortedRun);
        }

        // 后续轮次 - 不断合并直到只剩1个run
        while(runs.size() > 1){
            runs = mergePass(runs);  //更新排序过后的自己
        }

        return runs.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

