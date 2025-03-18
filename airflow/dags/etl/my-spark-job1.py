from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder.appName("CalculatePiJob1").getOrCreate()

# 定义计算 PI 的函数
def calculate_pi(partitions):
    n = 100000 * partitions
    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x**2 + y**2 <= 1 else 0
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(lambda a, b: a + b)
    return 4.0 * count / n

# 计算 PI
partitions = 10
pi = calculate_pi(partitions)
print(f"Pi is roughly {pi}")

# 停止 SparkSession
spark.stop()