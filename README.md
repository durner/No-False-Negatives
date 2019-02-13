No False Negatives: Accepting All Useful Schedules in a Fast Serializable Many-Core System
=======

This is a single node OLTP database benchmarking system.
It is a prototype database that can scale to many cores.
The corresponding paper to that database system is the following:

    No False Negatives: Accepting All Useful Schedules in a Fast Serializable Many-Core System
    Dominik Durner, Thomas Neumann
    35th IEEE International Conference on Data Engineering (ICDE 2019)
    https://db.in.tum.de/~durner/papers/no-false-negatives-icde19.pdf

Build
------------

```
    mkdir build
    cd build
    cmake -DCMAKE_BUILD_TYPE=Release ../
    make -j16
```


Execute
---
Basic config

```
  ./bin/db benchmark cc_protocol db_size tx_per_core oltp_cores [extra infos for workload]
```

For example
```
  ./bin/db svcc_smallbank NoFalseNegatives 1000 100000 10
```

