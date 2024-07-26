from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]").appName("PrimeNumberFinder").getOrCreate()
)
sc = spark.sparkContext


def is_prime(n):
    if n <= 1:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5) + 1, 2):
        if n % i == 0:
            return False
    return True


data = sc.textFile("./data/random_numbers.txt")

primes_rdd = data.map(int).filter(is_prime).distinct()

primes_df = primes_rdd.map(lambda x: (x,)).toDF(["prime"])

sorted_primes_df = primes_df.orderBy("prime")

sorted_primes_df.write.mode("overwrite").csv("./data/prime_numbers.csv")

spark.stop()
