class Memtable: 
    def __init__(self, capacity=1000): 
        self.store = dict()
        self.capacity = capacity

    def put(self, key, value): 
        self.store[key] = value

    def get(self, key): 
        return self.store.get(key)

    def range(self, start, end):
        for key in sorted(self.store.keys()):
            if start <= key <= end:
                yield (key, self.store[key])
            elif key > end:
                break

    def flush(self): 
        items = sorted(self.store.items())
        self.store.clear()
        return items

    def is_full(self):
        return len(self.store) >= self.capacity