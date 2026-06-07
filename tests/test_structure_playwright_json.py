import json
import argparse
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("json_file", help="Ruta al archivo JSON")
parser.add_argument(
    "--ignore-none",
    action="store_true",
    help="Ignora valores null/NoneType al comprobar tipos"
)

args = parser.parse_args()

with open(args.json_file, encoding="utf-8") as f:
    data = json.load(f)

types = defaultdict(set)


def walk(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            walk(v, f"{path}.{k}" if path else k)

    elif isinstance(obj, list):
        types[path].add("array")
        for item in obj:
            walk(item, path + "[]")

    else:
        type_name = type(obj).__name__

        if args.ignore_none and type_name == "NoneType":
            return

        types[path].add(type_name)


for row in data:
    walk(row)

found = False

for path, t in sorted(types.items()):
    if len(t) > 1:
        found = True
        print(f"INCONSISTENTE: {path} -> {t}")

if not found:
    print("No se encontraron inconsistencias de tipos.")
