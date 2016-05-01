# Assignment 5
In this assignment, you are asked to optimize recovery modules of VanillaCore.

## Steps
To complete this assignment, you need to

1. Fork the Assignment 5 project
2. Trace the code in the `vanilladb-core` project yourself
3. Modify the current implementation to reach better performance
4. Test the correctness using `RecoveryBasicTest`
5. Run a few experiments using the micro-benchmark project
6. Write an experiment analysis report
7. Push your repository to Gitlab and open a merge request to the assigned branch

## Optimization

We implemented Algorithms for Recovery and Isolation Exploiting Semantics, ARIES for short, in our recovery module. However, our implementation only focuses on correctness. The recovery process and checkpointing might be slow.

For example, VanillaCore flushes all buffers at each time of checkpointing, which causes that the transactions modifying those buffers are blocked until flushing is finished. Although it can speed up the recovery process, it may make a big impact when processing normal transactions. To solve this problem, you can maintain a **Dirty Page Table** to keep tracking dirty pages, and flush those pages a little at a time by a background thread. Then, instead of flushing all buffers, checkpoint process only flushes the **Dirty Page Table** to a stable storage. The recovery process will first find the minimum LSN in the **Dirty Page Table** and start REDO phase from this LSN. Also during the REDO procedure, it can only REDO the pages that are marked as DIRTY in the **Dirty Page Table** which is recorded in the most recent checkpoint log.

Moreover, you can also add a **Transaction Table** containing the numbers of active transactions and the LSNs of their last appended logs to the checkpoint log. In order to undo the actions made by unfinished transactions, this table can be used to find the last log record appended by those transactions without scanning whole log files.

A dirty page table and a transaction table might be very large. Saving these tables takes a lot of time and we do not want to block the transactions to save them when checkpointing. Therefore, we can do **fuzzy checkpoint** here. Fuzzy checkpoint appends a start checkpoint log when it starts, and appends a end checkpiont log when it finishes saving the tables. This makes whole saving process could be done in the background without blocking all active transacions.

Note that dirty page tables and transaction tables are designed for eliminating unncessary REDO and scanning. Without them, ARIES could still recovery a database to a consistent state.

## Reference

- [Overview of ARIES](http://netdb-db.appspot.com/slides/tas/XI_Overview_of_ARIES.pdf)
- [Optimization of ARIES](http://netdb-db.appspot.com/slides/tas/XII_Optimization_of_ARIES.pdf)
- [ARIES paper](https://www.cs.berkeley.edu/~brewer/cs262/Aries.pdf)

## Experiments

In this assignment, you can do any experiment to prove your optimization works.

For example, to prove that you eliminate the impact of checkpointing, you can run the micro-benchmark while enabling checkpointing process of VanillaCore and draw a plot demonstrating the throughput per second with the x-axis of elapsed time. Or, you can measure the time needed by the recovery process before and after the optimization to prove that your optimization makes recovery faster.

We have enabled the checkpointing in the properties file of benchmarks. The default setting makes the checkpointing happended every `60` seconds. You can also manually adjust those settings in `vanilladb.properties`:

```properties
# The flag to control doing periodical checkpointing or not.
org.vanilladb.core.server.VanillaDb.DO_CHECKPOINT=true
# Method: PERIODIC = 0, MONITOR = 1
# PERIODIC: Create checkpoint every X millisecond
# MONITOR: Create checkpoint every Y transactions committed
org.vanilladb.core.storage.tx.recovery.CheckpointTask.MY_METHOD=0
# in millisecond
org.vanilladb.core.storage.tx.recovery.CheckpointTask.PERIOD=60000
org.vanilladb.core.storage.tx.recovery.CheckpointTask.TX_COUNT_TO_CHECKPOINT=1000
```

## The report

- Explain what you exactly do for optimization
- Run a few experiments with the micro-benchmark
  - Show the result of your experiments
  - Explain the result of the experiments
- **Discuss why your optimization works**

	Note: There is no strict limitation to the length of your report. Generally, a 2~3 pages report with some figures and tables is fine. **Remember to include all the group members' student IDs in your report.**

## Submission

The procedure of submission is as following:

1. Fork our [Assignment 5](http://shwu10.cs.nthu.edu.tw/2016-cloud-database/CloudDB16-Assignment-5) on GitLab
2. Clone the repository you forked
3. Finish your work and write a report
4. Commit your work, push to GitLab and then open a merge request to the assigned branch. The repository should contain
	- *[Project directory]*
	- *[Team Member 1 ID]_[Team Member 2 ID]*_assignment3_report.pdf (e.g. 102062563_103062528_assignment3_reprot.pdf)

    Note: Each team only needs one submission.

## Demo

Due to the complexity of this assignment, we hope you can come to explain your work face to face. We will announce a demo table after submission. Don't forget to choose the demo time for your team.

## Deadline
Sumbit your work before **2016/05/18 (Wed.) 23:59:59**.
