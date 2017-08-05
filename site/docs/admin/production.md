---
title: Production Deployment
subtitle: Considerations, Best Practices and Import Configurations for production deployment
next: ../post-deployment
prev: ../index.html
---

This document covers the key things to consider before putting your cluster live in production.
It is not meant to be a comprehensive guide to running your BookKeeper cluster in production.

It covers following three main areas:

- Logistical considerations, such as hardware recommendations and deployment strategies.
- Configuration settings that are more suitable for a production environment.
- Actions after deployment, such as `autorecovery` and production monitoring.

## Hardware

If you have been following the [deployment](../../deployment/manual) guide, you've probably played with BookKeeper on your laptop locally or on a small cluster of machines laying around.
But when it comes to deploying BookKeeper to production, there are a few recommendations that you should consider. These recommendations will provide good starting points based on
the experiences with production clusters.

### CPU

Apache BookKeeper is a storage system, which is most likely bound by I/O and network. As such, the exact processor setup matters less than other resources (e.g. disks and network).
However if [TLS](../../security/tls) is enabled, the CPU requirements can be significantly higher. The exact details depend on the CPU type and JVM implementation on TLS.

It is recommended to choose a modern processor with multiple cpu cores. Common deployments utilize 24 core machines. If you need to choose between faster CPUs or more cores, choose
*more cores*. The extra concurrency that multiple cores offers will far outweigh a slightly faster clock speed.

### Memory

The memory usage on Bookies comes from different places.

- `JVM Heap`: Most of the object allocations happen on jvm heap. However they are not long-lived objects and will quickly be garbage collected. Bookies use heap space very carefuly
    and does not require setting a large heap size. However if you are using `SortedLedgerStorage` as the ledger storage implementation, the latest written data will be buffered in
    a on-heap skiplist map before flushing to disks. You need to adjust the heap size to accommondate the space for the skiplist map.
- `JVM Direct Memory`: Bookies use direct memory extensively in order to reduce the objects allocated on heap and prevent long pauses during JVM garbage collection. The direct memory
    usage is mainly comes from `index page cache` and `netty io`.
- `Kernel Pagecache`: Although bookies don't rely directly on filesystem for caching latest data. The data flushed from memory table will still be written to filesystem and cached in pagecache
    for random reads or `catch-up` reads. Reserve certain amount of memory for filesystem pagecache will help `random` and `catch-up` reads.

A simple rule as below is helpful for the memory capacity planning. This assumes that you are using `SortedLedgerStorage` as the ledger store.

- `direct memory` is mainly used by `ledger cache` and `netty io`. The memory in `netty io` is used for receiving data and sending data. You typically don't need memory higher than
    your nic's bandwidth; while the memory in `ledger cache` depends on the number of active ledgers and the corresponding index page size, you can compute the memory need as `active-ledgers * index-page-size`,
    and allocate 2 or 3 times of the calcuated memory.
- once you figure out the `direct memory` size, you can configure your jvm heap size to be same as the `direct memory` size.
- In order to have better performance for fanout reads, you need sufficient memory for filesystem to buffer data for active readers. You can do a back-of-the-envelope estimate of memory
    needs by assuming you want to be able to buffer for 30 seconds and compute your filesystem page cache need as `write_throughput * 30`.

A machine with 64GB of RAM is a decent choice to cover a lot of common use cases. However if your applications don't have a lot of read activities, you can use machines wit smaller memory.

See more details in [Development](../../development) to better understand BookKeeper's I/O architecture.

### <a name="disks"></a> Disks

In order for bookies to provide optimal performance, it's essential that bookies have suitable disk configuration. There are two key dimensions to bookie's disk configuration:

- Disk I/O bandwidth
- Storage capacity

Data written to bookies are always synced to disk before returning an acknowledgement to the clients. To achieve I/O isolation and ensure low write latency, bookies are designed to leverage multiple
devices for different purposes.

- A `journal directory` is used for ensuring durability. It is critical to have a dedicated device for `journal`, in order to have fast [fsync](https://linux.die.net/man/2/fsync) operations.
    Typically, small and fast [solid-state drives](https://en.wikipedia.org/wiki/Solid-state_drive) (SSDs) should suffice, or [hard disk drives](https://en.wikipedia.org/wiki/Hard_disk_drive)
    (HDDs) with a [RAID](https://en.wikipedia.org/wiki/RAID)s controller and a battery-backed write cache. Both solutions can ensure fsync latency of half millisecond.
    (NOTE: multiple journal devices are supported since 4.5.0 release.)
- Multiple `ledger directories` are used for storing data. The data written into `ledger directories` in background, so write I/O in general is not a big concern. Reads will happen sequentially most
    of the time. To store large amounts of data, a typical configuration will involve multiple HDDs with a RAID controller. If your applications will have extensive random rands to bookkeeper cluster,
    SSDs are preferred over HDDs.
- It is optionally to configure multiple `index directories` for storing the index data. The data written into `ledger directories` are huge and mostly sequentially, while the data written to
    `index` are small and mostly randomly. If you are using HDDs for `ledger directories`, it is recommended to have a small dedicated device as `index directory`. This ensures writing flushing index
    files won't impact your normal writes.

You can either RAID the devices together into a single volume or format and mount each drive as its owner directory.

For example, if you have 2 SSDs (`/dev/ssd1` and `/dev/ssd2`) and 6 HDDs (`/dev/hdd{0-5}`), you have multiple options to configure them.
The first option, you can raid those 2 ssds into one volume (`/dev/journal`) and use it as the `journal device`, and raid the other 6 HDDs into another volume (`/dev/ledger`) for the `ledger device`;
Or you can mount them individually and configure a list of directories for `journal` and `ledger`.

If you configure multiple journal directories, the data will be assigned by ledger ids to journal directories. Each ledger will only be written in one of the `journal` directories. If throughput is not
well balanced among ledgers this can lead to load imbablance between journal disks.

RAID can potentially do better at balancing load between disks because it balances load at a lower level and also improve the ability to tolerate disk failures, which is good for operating a large
cluster. The primary downside of RAID is that it reduces the disk capacity.

See more details in [Development](../development) to better understand BookKeeper's I/O architecture.

### Network

Although bookies don't have strict requirements on networking, a *fast* and *reliable* network is recommended for performance considerations. Modern data-center networking (e.g. 1GbE, 10GbE) is
sufficient for the vast majority of clusters. `10GbE` is recommended since the software can take full advantages of that.

BookKeeper can be deployed either within one data center or spanning multiple data centers. It also provides multiple placement policies to place the replicas in your cluster, to better utilize
the resources according to your network topologies. See [Placement Policies](#placement-policies) on how to configure the placement polices.

### Filesystem

`XFS` or `Ext4` is recommended to run bookies.

## JVM

It is recommended to run BookKeeper on latest version of JDK 1.8 with the [G1](http://www.oracle.com/technetwork/tutorials/tutorials-1876574.html) collector.

One recommended GC tuning looks like this:

```shell
-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g \
-XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions \
-XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB
```

If you are looking for using [Concurrent Mark Sweep](https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gctuning/cms.html) (CMS) Collector, you can
follow following settings:

```shell
-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g -XX:MaxMetaspaceSize=128M -XX:MetaspaceSize=128M \
-XX:ParallelGCThreads=${JVM_NUM_GC_THREADS} \
-XX:+CMSScavengeBeforeRemark \
-XX:TargetSurvivorRatio=90 \
-XX:+UseConcMarkSweepGC
```

It is also a good practise to include gc log related settings to dump the gc details to a log file for performance troubleshooting.

```shell
-Xloggc:gc.log -XX:+PrintCommandLineFlags -verbosegc \
-XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=64M -XX:+UseGCLogFileRotation \
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC \
-XX:+HeapDumpOnOutOfMemoryError
```

## Configuration

BookKeeper ships with very good defaults, especially when it comes to performance-related settings and options. When in doubt, just leave the settings alone.

With that said, there are some logistical configurations that should be changed for production. These changes are necessary either to make your life easier, or
because their is no way to set a good default (because it depends on your cluster layout).

Here is a list of important steps for you to follow to configure your cluster and clients.

1. Decide where to store your metadata.
2. Configure your bookie storage.
3. Choose your placement policy for clients.

### Decide where to store your metadata

Currently ZooKeeper is the default metadata storage for BookKeeper. You are recommended to use either `LongHierarchicalLedgerManagerFactory` or `HierarchicalLedgerManagerFactory`
as the `ledger manager`. `HierarchicalLedgerManagerFactory` was introduced since `4.2.0` and most widely used, however it has a limitation on the number of ledgers
that it can be allocated (currently it is bound at 2^32); while `LongHierarchicalLedgerManagerFactory` was introduced in `4.5.0` to overcome this limitation. If you use `LedgerHandleAdv`
API to generate ledger id by yourself, it is strongly recommended to use `LongHierarchicalLedgerManagerFactory`, so you will not encounter the limitation in `HierarchicalLedgerManagerFactory`.

Below are some important settings you need to configure for both your bookies and clients.

`ledgerManagerFactoryClass`

The ledger manager factory implementation that the cluster is using.

- Type: String
- Required: `true`
- Values:
    - `org.apache.bookkeeper.meta.FlatLedgerManagerFactory`
    - `org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory`
    - `org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory`
    - `org.apache.bookkeeper.meta.MSLedgerManagerFactory`

`zkLedgersRootPath`

Root zookeeper path to store the ledger metadata. This parameter is used by zookeeper-based ledger managers as a root znode to store all the ledger metadata.

- Type: String
- Required: `true` if `ledgerManagerFactoryClass` is zookeeper based.
- Default: `/ledgers`

`metastoreImplClass`

The metastore implementation used by `MSLedgerManagerFactory`. This parameter is used by metastore interface based ledger managers.

- Type: String
- Required: `true` if `ledgerManagerFactoryClass` is `MSLedgerManagerFactory`.

You can checkout [ledger metadata management](../../developement) to learn more details.

### Configure your bookie storage

As described in [disks](#disks) section, you have to configure `ledgerDirectories` and `journalDirectories` in order to run a bookie. This is important as
they are used for storing the data written to bookie. It is also recommended to have separate devices for journals and ledger storage.

`journalDirectories`

The directories to which Bookies output its write-ahead-log (WAL). You can define multiple directories to store write ahead logs separated by a comma.

- Type: String
- Required: `true`
- Format: a list of directories separated by a comma, for example, `/tmp/dir1,/tmp/dir2`.

`ledgerDirectories`

The directories to which Bookies output ledger snapshots. You can define multiple directories to store snapshots separated by a comma.

- Type: String
- Required: `true`
- Format: a list of directories separated by a comma, for example, `/tmp/dir1,/tmp/dir2`.

`indexDirectories`

The directories to which Bookie output its index files. If not specified, the value of `ledgerDirectories` will be used as index directories.
It is good to configure a separated device for index files if you are using HDDs for `ledgerDirectories`.

- Type: String
- Required: optional. better to configure if `ledgerDirectories` are on HDDs.
- Format: a list of directories separated by a comma, for example, '/tmp/dir1,/tmp/dir2`.

#### Journal Performance Tuning

The default settings are pretty good for running bookies for most of the application and still achieve pretty low latency. However if you want to
customize your journal settings, below are a few settings that you need to know.

`journalMaxGroupWaitMSec`

Maximum latency to impose on a journal write to achieve grouping.

- Type: Integer
- Default: `2`

`journalFlushWhenQueueEmpty`

A journal flush will be triggered when journal queue is empty. If it is `true`, `journalMaxGroupWaitMSec` will be ignored.

- Type: Boolean
- Default: `false`

`journalBufferedWritesThreshold`

Maximum bytes to buffer to achieve grouping. 

- Type: Integer
- Default: `512KB`

It is strongly recommended to configure `journalMaxGroupWaitMSec` and disable `journalFlushWhenQueueEmpty`. This would ensure predictable latency on
fsyncing journals even during traffic spikes. A value of 2 to 4 milliseconds is in general good for `journalMaxGroupWaitMSec`. Enabling `journalFlushWhenQueueEmpty`
can have a slightly better latency when your traffic is low, however your latency will become spiky when the traffic becomes bursty.

### <a name="placement-policies"></a> Choose your placement policy for clients

BookKeeper provides a few built-in placement policies to placement replicas in different racks or regions (aka data centers) according to the cluster's network topology.

The available placement policies are listed as below:

- `default`: `org.apache.bookkeeper.client.DefaultEnsemblePlacementPolicy`
- `rack-aware`: `org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy`
- `region-aware`: `org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy`

If you don't care about the network topology or run the cluster in an environment where network topology is unclear, you can use `default` placement policy (you don't have
anything to worry about); if you are running the cluster in a single data center where network topology can be retrieved, you can configure your clients to use
`rack-aware` placement policy; or if your are running a cluster spanning over multiple data centers, you can configure your clients to use `region-aware` placement policy.

To configure which placement policy, you need specify the placement policy class in following setting:

`ensemblePlacementPolicy`

- Type: String
- Format: Class name of the placement policy implementation.

For `rack-aware` and `region-aware` placement policies, you can either configure a `DNSResolver` in client builder or specify a `DNSResover` class in client configuration.

`reppDnsResolverClass`

- Type: String
- Format: Class name of the `DNSResolver` implementation.

If you don't configure a `DNSResolver`, it will use a `ScriptBasedMapping` dns resolver, which it uses a script to resolve bookies' network locations. The script can be configured
in following settings:

`networkTopologyScriptFileName`

- Type: String.
- Format: File path. The path points to a script file.

`networkTopologyScriptNumberArgs`

- Type: Integer
- Description: the number of arguments used by the script defined in `networkTopologyScriptFileName`.

Please check [Placement Policies](../placement-policy) to learn more details.

## File Descriptors

Currently the large number of files is used by the bookies for storing the index files, when using `InterleavedLedgerStorage` or `SortedLedgerStorage`. At the same
time, Bookies uses a large number of sockets to communicate with the clients. All of this requires a relatively high number of available file descriptors.

Many modern Linux distributions ship with a paltry `1024` file descriptors allowed per process. This is far too low for even a small bookie node, let alone one that
hosts more than thousands of ledgers.

It is strongly recommended to increate the file descriptor count to something very large, such as `100,000`. This process is irritatingly difficult and highly dependent
on your particular OS and distribution. Consult the documentation for your OS to determine how best to change the allowed file descriptor count.

Beside setting a proper allowed file descriptor count, you should also configure `openFileLimit` in bookies to prevent bookie swapping ledger indexes from memory to disk
too often. Too frequent swap will affect performance. You can tune this number to gain performance according to your requirements. Also when you change this value,
please also adjust the jvm memory settings accordingly in order for the JVM to be able to hold the ledger cache in memory.

## ZooKeeper

### Stable version

It is recommended to run zookeeper 3.4.6 or later.

### Operation Notes

- Redundancy in the physical/hardware/network layout: try not to put them all in the same rack, decent (but don’t go nuts) hardware,
    try to keep redundant power and network paths, etc. A typical ZooKeeper ensemble has 5-7 servers, which tolerates 2 and 3 servers down,
    respectively. If you have a small deployment, then using 3 servers is acceptable, but keep in mind that you’ll only be able to tolerate 1 server down in this case.
- I/O segregation: if you do a lot of write type traffic you’ll almost definitely want the transaction logs on a dedicated disk group.
    Writes to the transaction log are synchronous (but batched for performance), and consequently, concurrent writes can significantly affect performance.
    ZooKeeper snapshots can be one such a source of concurrent writes, and ideally should be written on a disk group separate from the transaction log. 
    Snapshots are writtent to disk asynchronously, so it is typically ok to share with the operating system and message log files.
    You can configure a server to use a separate disk group with the `dataLogDir` parameter.
- Application segregation: Unless you really understand the application patterns of other apps that you want to install on the same box,
    it can be a good idea to run ZooKeeper in isolation (though this can be a balancing act with the capabilities of the hardware). If you do end up sharing the ensemble,
    you might want to use the [chroot](https://zookeeper.apache.org/doc/r3.4.6/zookeeperProgrammers.html#ch_zkSessions) feature. With chroot, you give each application its own namespace.
- Use care with virtualization: In general, there is no problem with using ZooKeeper in a virtualized environment. It is best to place servers in different availability
    zones to avoid correlated crashes and to make sure that the storage system available can accommodate the requirements of the transaction logging and snapshotting of ZooKeeper.
    Persistence volumes are strongly recommended to use in a cloud environment. Also, keep in mind that there might be issues in ZooKeeper related to name resolution in the the cloude,
    typically related to JVM DNS caching, you are recommended to add `-Dnetworkaddress.cache.ttl=60 -Dnetworkaddress.cache.negative.ttl=60` as the system properties to run your zookeeper servers.
- JVM: ZooKeeper is an in-memory metadata store. Make sure you give zookeeper `enough` heap space for holding the metadata you need to store for ledgers. We don't have a good formula for
    calculating the memory needed for a certain number of ledgers. But keep in mind that large number of ledgers means that snapshots of zookeeper can become large, and large snapshots affect
    recovery time. If the snapshot becomes too large (a few gigabytes), then you may need to increate the `initLimit` parameter in zookeeper to give enough time for zookeeper servers to recover
    and join the ensemble.
- Observers: It is recommended to add [observers](https://zookeeper.apache.org/doc/r3.4.6/zookeeperObservers.html) when your have large number of readers in the cluster.
- Additional admin documentation: If you need some additional detail about the administration of ZooKeeper, the [admin guide](https://zookeeper.apache.org/doc/r3.4.6/zookeeperAdmin.html) contains more material.
