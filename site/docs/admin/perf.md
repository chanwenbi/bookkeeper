---
title: Performance tuning
---

Here are a few points:

- Disk layouts
  - Journals: dedicated disks. Either SSD or HDD is okay because it is for sequential writes.
  - Ledgers: don't use journal disks for journals. It can be raided to achieve high IOPS.
  - Mount Options:
    - If there are power-loss data protection on the disks, it is preferred to use nobarrier mount option on mouting filesyste like ext3 or ext4

- Throughput and Latency
  - Threading: numJournalCallbackThreads numAddWorkerThreads numReadWorkerThreads => Configuring the number of threads to be around or twice of the number of cpu cores.
  - Journals: journalMaxGroupWaitMSec=1 => The maximum wait time that bookie journal thread waits befor fsyncing the data to disks. Configuring it to be around 1 or 2ms will allow a good balance between latency and throughput.

- Naming

By default, the bookie uses IP address for identifying themselves. It is important to keep IP address unchanged during
the lifecycle of bookies. If the bookkeeper cluster is deployed in an environment that IP address will be changed. It
might be good to consider configuring 'useHostNameAsBookieID=true' to use hostname as the identifiers.

- Listening Ports

If there are mutliple nics available on the bookie host, you can configure which nic to use by specifying 'listeningInterface'.

- Compaction

[TBD]

