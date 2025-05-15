class WAL: 
    def __init__(self, path): 
        self.path = path

    def append(self, key, value):
        with open(self.path, 'ab') as f:
            line = f"{key}|{value}\n"
            f.write(line.encode())

    def replay(self):
        entries = []
        if not os.path.exists(self.path):
            return entries
        
        with open(self.path, 'r', encoding='utf-8') as f:
            for line in f: 
                decoded = line.strip()
                if "|" in decoded: 
                    key, value = decoded.split("|", 1)
                    entries.append((key, value))
        return entries

    def reset(self):
        open(self.path, 'w').close()
