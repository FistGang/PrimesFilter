import argparse
from pathlib import Path
import random

parser = argparse.ArgumentParser(description="Generate a file with random numbers")
parser.add_argument(
    "--num", type=int, default=1000, help="Number of random numbers to generate"
)
parser.add_argument("--start", type=int, default=0, help="Starting of random numbers")
parser.add_argument("--end", type=int, default=10000, help="Ending of random numbers")
args = parser.parse_args()

data_path = Path("data")
data_path.mkdir(exist_ok=True)
data_file = data_path / "random_numbers.txt"

random_numbers = [random.randint(args.start, args.end) for _ in range(args.num)]

with open(data_file, mode="w") as file:
    for number in random_numbers:
        file.write(f"{number}\n")

print(
    f"Generated {args.num} random numbers from {args.start} to {args.end} and saved to {data_file}"
)
