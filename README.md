# PrimesFilter

This project aims to filter all prime numbers in a text file of random numbers

## Generate sample data

```bash
python3 generate_sample_data.py --num 1000 --start 0 --end 10000
```

All values passed into the script arguments are default, you can change them
on your own

## Scripts

```bash
python3 find_primes.py
```

This script will filter all prime numbers in the sample data file, remove
duplicates and sort them in ascending order by default. Then save them in
a `.csv` file.
