from pyspark.sql import SparkSession
import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find prime numbers in a dataset using PySpark"
    )
    parser.add_argument(
        "--input_dir", type=str, required=True, help="Input data directory"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Output directory for prime numbers",
    )
    return parser.parse_args()


def is_prime(num):
    if num <= 1:
        return False
    if num == 2:
        return True
    if num % 2 == 0:
        return False
    for d in range(3, int(num**0.5) + 1, 2):
        if num % d == 0:
            return False
    return True


if __name__ == "__main__":
    args = parse_args()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PrimeNumberFinder")
        .getOrCreate()
    )
    sc = spark.sparkContext

    data = sc.textFile(args.input_dir)

    primes_rdd = data.map(int).filter(is_prime).distinct()

    primes_df = primes_rdd.map(lambda x: (x,)).toDF(["prime"])

    sorted_primes_df = primes_df.orderBy("prime")

    sorted_primes_df.write.mode("overwrite").csv(args.output_dir)

    print(f"Prime numbers written to {args.output_dir}")

    spark.stop()
