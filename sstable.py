from bisect import bisect_left, bisect_right
from constants import TOMBSTONE
class SSTable: 
    def __init__(self, filepath): 
        self.filepath = filepath
        self._load_index()
    
    @staticmethod
    def write(filepath, data): 
        with open(filepath, 'wb') as f: 
            for key, value in data: 
                line = f"{key}|{value}\n"
                f.write(line.encode())

    def get(self, key): 
        if key not in self.index: 
            return None
        
        with open(self.filepath, 'rb') as f:
            f.seek(self.index[key])
            line = f.readline()
            decoded = line.decode(errors='ignore').strip()
            if "|" in decoded: 
                found, value = decoded.split("|", 1)
                if found == key:
                    if value == TOMBSTONE:
                        return None
                    return value
        return None

    def range(self, start, end):
        if start > end: 
            raise ValueError("Start key must be less than or equal to end key.")
        
        sorted_keys = sorted(self.index.keys())
        start_idx = bisect_left(sorted_keys, start)
        end_idx = bisect_right(sorted_keys, end)
        key_range = sorted_keys[start_idx:end_idx]
        
        with open(self.filepath, 'rb') as f:
            for key in key_range: 
                f.seek(self.index[key])
                line = f.readline()
                decoded = line.decode(errors='ignore').strip()
                if "|" in decoded: 
                    found, value = decoded.split("|", 1)
                    if found == key and value != TOMBSTONE: 
                        yield(key, value)
    
    def _load_index(self): 
        with open(self.filepath, 'rb') as f: 
            self.index = {}
            offset = 0
            while True:
                line = f.readline()
                if not line: 
                    break
                decoded = line.decode(errors='ignore').strip()
                if "|" in decoded: 
                    key, _ = decoded.split("|", 1)
                    self.index[key] = offset
                offset += len(line)