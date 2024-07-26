from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]").appName("PrimeNumberFinder").getOrCreate()
)
sc = spark.sparkContext


def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True


data = sc.textFile("./data/random_numbers.txt")

primes = data.map(int).filter(is_prime)

primes.saveAsTextFile("./data/prime_numbers.txt")

spark.stop()
