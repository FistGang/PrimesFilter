from pyspark.sql import SparkSession
from pyspark import SparkConf
import random
import argparse
from pathlib import Path
import shutil


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a file with random numbers using PySpark"
    )
    parser.add_argument(
        "--num", type=int, default=10000000, help="Number of random numbers to generate"
    )
    parser.add_argument(
        "--start", type=int, default=0, help="Starting of random numbers"
    )
    parser.add_argument(
        "--end", type=int, default=10000000, help="Ending of random numbers"
    )
    parser.add_argument(
        "--output", type=str, default="random_numbers", help="Output directory name"
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1000,
        help="Number of partitions for parallel processing",
    )
    return parser.parse_args()


def generate_random_numbers(num, start, end):
    return [random.randint(start, end) for _ in range(num)]


if __name__ == "__main__":
    args = parse_args()

    spark_conf = SparkConf().setAppName("RandomNumberGenerator").setMaster("local[*]")
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    partitions = spark.sparkContext.parallelize(
        range(args.num), numSlices=args.partitions
    )

    random_numbers_rdd = partitions.flatMap(
        lambda _: generate_random_numbers(1, args.start, args.end)
    )

    output_path = Path(args.output)
    if output_path.exists() and output_path.is_dir():
        shutil.rmtree(output_path)

    random_numbers_rdd.saveAsTextFile(str(output_path))

    print(
        f"Generated {args.num} random numbers from {args.start} to {args.end} and saved to {output_path}"
    )

    spark.stop()
