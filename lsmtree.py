import os
from pathlib import Path
from memtable import Memtable
from sstable import SSTable
from wal import WAL
from constants import TOMBSTONE

class LSMTree: 
    def __init__(self, data_dir='./data', memtable_size=1000):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.memtable = Memtable(memtable_size)
        self.sstables = []
        self.wal = WAL(self.data_dir / 'wal.txt')

        for key, value in self.wal.replay():
            self.memtable.put(key, value)

        self._load_sstables()
        
    def put(self, key, value):
        self.wal.append(key, value)
        self.memtable.put(key, value)
        if self.memtable.is_full(): 
            self._flush_memtable()
    
    def get(self, key):
        val = self.memtable.get(key)
        if val is not None: 
            return val
        
        for sstable in self.sstables:
            val = sstable.get(key)
            if val is not None: 
                return val
        return None

    def delete(self, key):
        self.put(key, TOMBSTONE)

    def range(self, start, end):
        yielded_keys = set()

        for key, value in self.memtable.range(start, end):
            yielded_keys.add(key)
            yield (key, value)
        
        for sstable in self.sstables:
            for key, value in sstable.range(start, end):
                if key not in yielded_keys:
                    yielded_keys.add(key)
                    yield (key, value)

    def _flush_memtable(self):
        old_memtable = self.memtable 
        self.memtable = Memtable(old_memtable.capacity) # Replace it immediately for immutability. 

        items = old_memtable.flush() 
        path = self._new_sstable_path()

        SSTable.write(path, items) 
        self.sstables.insert(0,SSTable(path))
        
        self.wal.reset()
    
    def _load_sstables(self):
        self.sstables.clear()
        for filepath in sorted(self.data_dir.glob('sst_*.txt'), reverse=True):
            self.sstables.append(SSTable(filepath))
    
    # Robust to deletions, merges, or crashes. 
    def _new_sstable_path(self):
        existing_files = list(self.data_dir.glob('sst_*.txt'))
        max_id = 0
        for f in existing_files: 
            try:
                num = int(f.stem.split('_')[1])
                max_id = max(max_id, num)
            except (IndexError, ValueError):
                continue
        next_id = max_id + 1
        filename = f'sst_{next_id:06}.txt'
        return self.data_dir / filename
    
    def _compact_sstables(self):
        if len(self.sstables) < 2:
            return
        
        tables_to_merge = self.sstables[-2:]\
        merged = {}

        for sstable in reversed(tables_to_merge):
            for key, value in sstable.range("", "zzz"): # full scan
                if key not in merged:
                    merged[key] = value
        
        filtered = [(k, v) for k, v in merged.items() if v != TOMBSTONE]
        
        path = self._new_sstable_path()
        SSTable.write(path, sorted(filtered))
    
        for sstable in tables_to_merge:
            os.remove(sstable.filepath)
        
        self._load_sstables()
            
