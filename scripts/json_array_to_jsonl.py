import json
import sys
from pathlib import Path


def convert(input_path, output_path):
    input_path = Path(input_path)
    output_path = Path(output_path)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("El JSON no es una lista (array).")

    with output_path.open("w", encoding="utf-8") as f:
        for i, row in enumerate(data):
            if not isinstance(row, dict):
                print(f"Skip index {i}: no es objeto JSON")
                continue
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"OK -> {len(data)} registros escritos en {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python json_array_to_jsonl.py input.json output.jsonl")
        sys.exit(1)

    convert(sys.argv[1], sys.argv[2])
