from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as pyspark_sum, lit, desc, lag, lead
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder.master('local[*]').appName('YearCountTask').getOrCreate()

# 创建DataFrame
data = [
    ('AE686(AE)', '7', 'AE686', 2022),
    ('AE686(AE)', '8', 'BH2740', 2021),
    ('AE686(AE)', '9', 'EG999', 2021),
    ('AE686(AE)', '10', 'AE0908', 2023),
    ('AE686(AE)', '11', 'QA402', 2022),
    ('AE686(AE)', '12', 'OA691', 2022),
    ('AE686(AE)', '12', 'OB691', 2022),
    ('AE686(AE)', '12', 'OC691', 2019),
    ('AE686(AE)', '12', 'OD691', 2017)
]

df = spark.createDataFrame(data, ["peer_id", "id_1", "id_2", "year"])

# 定义窗口规范，按peer_id和年份排序
w = Window.partitionBy("peer_id").orderBy(desc("year"))

# 步骤1: 找到每个peer_id中包含id_2的年份
df_with_years = df.where(col("id_2").isin(df.select("id_1").rdd.flatMap(lambda x: x).distinct().collect()))

# 步骤2: 对每个peer_id分组，并统计每年的数量
df_year_count = df_with_years.groupBy("peer_id", "year").count().orderBy("peer_id", "year")


# 步骤3-5: 累加计数直到达到给定大小数
def cumulative_count_until(df, threshold):
    result = []
    for peer_id, group in df.groupBy("peer_id"):
        years = group.orderBy("year").collect()
        cumulative_count = 0
        for year_row in years:
            cumulative_count += year_row.count
            if cumulative_count >= threshold:
                result.append((peer_id, year_row.year))
                break
            elif year_row.year != years[-1].year:
                next_year_row = years[years.index(year_row) + 1]
                cumulative_count += next_year_row.count
                result.append((peer_id, next_year_row.year))
                break
    return spark.createDataFrame(result, ["peer_id", "year"])


# 执行并收集结果
size_threshold = 5
result_df = cumulative_count_until(df_year_count, size_threshold)
result_df.show()

# 停止SparkSession
spark.stop()