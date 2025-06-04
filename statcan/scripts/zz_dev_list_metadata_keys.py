import os
import json
from collections import Counter

metadata_dir = "/app/raw/metadata/"  # Adjust if needed
dimension_keys = Counter()
member_keys = Counter()

for filename in os.listdir(metadata_dir):
    if filename.endswith(".json"):
        path = os.path.join(metadata_dir, filename)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Top-level list with one dict
            record = data[0] if isinstance(data, list) else data
            obj = record.get("object", {})
            dimensions = obj.get("dimension", [])

            for dim in dimensions:
                if isinstance(dim, dict):
                    dimension_keys.update(dim.keys())
                    members = dim.get("member", [])
                    for mem in members:
                        if isinstance(mem, dict):
                            member_keys.update(mem.keys())

        except Exception as e:
            print(f"⚠️  Error reading {filename}: {e}")

print("\n✅ Dimension keys:")
for k, v in dimension_keys.most_common():
    print(f"  - {k} ({v})")

print("\n✅ Member keys:")
for k, v in member_keys.most_common():
    print(f"  - {k} ({v})")

