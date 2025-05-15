# toy lsm from scratch

A minimal, educational implementation of a **Log-Structured Merge Tree** in Python.

# project structure

```
toy-lsm-tree/
├── memtable.py         # in-memory key-value store
├── sstable.py          # immutable sorted disk file
├── lsmtree.py         # coordinates reads, writes, and flushes
├── wal.py              # (optional) write-ahead log for durability
├── test/
│   └── test.py     # basic demo 
└── README.md
```
