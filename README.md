# PrimesFilter

This project aims to filter all prime numbers in a text file of random numbers

## Generate sample data

```bash
python3 generate_sample_data.py --num 10000000 --start 0 --end 10000000 --output random_numbers
```

All values passed into the script arguments are default, you can change them
on your own.

## Scripts

```bash
python3 find_primes.py --input random_numbers --output prime_numbers
```

This script will filter all prime numbers in the sample data folder, which contain
multiple partitions of random numbers, remove duplicates and sort them in ascending
order by default. Then save them in `.csv` files.
