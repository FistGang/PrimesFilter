from pathlib import Path
import random

data_path = Path("data")
data_path.mkdir(exist_ok=True)
data_file = data_path / "random_numbers.txt"

random_numbers = [random.randint(1, 10000) for _ in range(1000)]

with open(data_file, mode="w") as file:
    for number in random_numbers:
        file.write(f"{number}\n")
