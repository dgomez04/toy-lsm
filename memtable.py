from constants import TOMBSTONE

class Memtable: 

    def __init__(self, capacity=1000): 
        self.store = dict()
        self.capacity = capacity

    def put(self, key, value): 
        self.store[key] = value

    def get(self, key): 
        val = self.store.get(key)
        if val == TOMBSTONE:
            return None
        return val

    def range(self, start, end):
        for key in sorted(self.store.keys()):
            if key > end: 
                break 
            if start <= key: 
                val = self.store[key]
                if val != TOMBSTONE:
                    yield (key, val)

    def flush(self): 
        items = sorted(self.store.items())
        self.store.clear()
        return items

    def is_full(self):
        return len(self.store) >= self.capacity