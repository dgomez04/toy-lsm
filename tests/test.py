db = LSMTree()

db.put("a", "1")
db.put("b", "2")
db.delete("a")

assert db.get("a") is None
assert db.get("b") == "2"

# Flush and recheck
for i in range(1000): db.put(f"key{i}", str(i))  # trigger flush

assert db.get("a") is None
assert db.get("b") == "2"