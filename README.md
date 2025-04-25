# toy lsm from scratch

A minimal, educational implementation of a **Log-Structured Merge Tree** in Python.

# project structure

tiny-lsm-tree/
├── memtable.py         # in-memory key-value store
├── sstable.py          # immutable sorted disk file
├── lsm_tree.py         # coordinates reads, writes, and flushes
├── utils.py            # helpers for filenames and file I/O
├── wal.py              # (optional) write-ahead log for durability
├── compaction.py       # (optional) merges multiple SSTables
├── test/
│   └── test_lsm.py     # basic demo and validation script
└── README.md

