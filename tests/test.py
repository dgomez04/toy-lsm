import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import shutil
from lsmtree import LSMTree
from constants import TOMBSTONE

def reset_storage():
    shutil.rmtree("demo_data", ignore_errors=True)

def main():
    print("starting lsm tree demo")
    reset_storage()

    # initialize database
    db = LSMTree(data_dir="demo_data", memtable_size=5)

    print("\ninserting key-value pairs")
    for i in range(10):
        db.put(f"key{i}", f"value{i}")
    print("inserted keys: key0 to key9")

    print("\nreading back values")
    print("key3 =", db.get("key3"))
    print("key7 =", db.get("key7"))
    print("key99 =", db.get("key99"))  # should return none

    print("\ndeleting keys: key2, key4, key6")
    db.delete("key2")
    db.delete("key4")
    db.delete("key6")

    print("\nconfirming deletions")
    print("key2 =", db.get("key2"))  # none
    print("key4 =", db.get("key4"))  # none
    print("key6 =", db.get("key6"))  # none

    print("\nforcing flush by exceeding memtable size")
    db.put("force_flush", "1")  # forces a flush (memtable size = 5)

    print("\nreopening database to test durability")
    db2 = LSMTree(data_dir="demo_data")
    print("key3 =", db2.get("key3"))
    print("key2 =", db2.get("key2"))  # still deleted
    print("force_flush =", db2.get("force_flush"))

    print("\nrange scan before compaction")
    for k, v in db2.range("key0", "key9"):
        print(f"{k} => {v}")

    print("\ncompacting sstables")
    db2._compact_sstables()

    print("\nrange scan after compaction")
    for k, v in db2.range("key0", "key9"):
        print(f"{k} => {v}")

    print("\ndemo complete")

if __name__ == "__main__":
    main()