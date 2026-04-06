#!/usr/bin/env python3
import json
import sys

def convert_to_bulk_format():
    for line in sys.stdin:
        try:
            doc = json.loads(line.strip())
            # Create the index action
            index_action = {
                "index": {
                    "_index": doc["_index"],
                    "_id": doc["_id"]
                }
            }
            # Print index action
            print(json.dumps(index_action, separators=(',', ':')))
            # Print document source
            print(json.dumps(doc["_source"], separators=(',', ':')))
        except json.JSONDecodeError as e:
            print(f"Error parsing line: {e}", file=sys.stderr)
            continue
        except KeyError as e:
            print(f"Missing key: {e}", file=sys.stderr)
            continue

if __name__ == "__main__":
    convert_to_bulk_format()
