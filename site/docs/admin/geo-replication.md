---
title: Geo-replication
---

(https://distributedlog.incubator.apache.org/docs/latest/deployment/global-cluster.html)

## Setting up a Global bookkeeper cluster

How to setup a global bookkeeper cluster spanning over 3 regions (A, B, C).

- Setting a global zookeeper spanning over A, B, C
- Setting a global bookkeeper
  - For each region, it is same as setting bookkeeper within same region.
    The difference is to point the bookies to use the global zookeeper setup in first step.
  - Configuring the autorecovery to use region-aware placement policy. (some the content in the link https://distributedlog.incubator.apache.org/docs/latest/deployment/global-cluster.html)
- Once the first two steps are done, configure the clients on how to use the global cluster
  - placement policy
  - connection timeouts
  - quorum size
