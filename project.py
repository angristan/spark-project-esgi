import datetime

from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Project") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option("header", True).csv('full.csv')

# 1. Afficher dans la console les 10 projets Github pour lesquels il y a eu le plus de commit.

df.groupBy('repo') \
    .count() \
    .orderBy('count', ascending=False) \
    .show(n=10)

# Output:
# +--------------------+--------+
# |                repo|   count|
# +--------------------+--------+
# |                null|31631188|
# |         openbsd/src|  103906|
# |      rust-lang/rust|   77696|
# |    microsoft/vscode|   65518|
# | freebsd/freebsd-src|   64103|
# |      python/cpython|   63910|
# |         apple/swift|   45756|
# |kubernetes/kubern...|   41480|
# |     rstudio/rstudio|   29384|
# |       opencv/opencv|   25772|
# +--------------------+--------+

# 2. Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de commit) du projet apache/spark.

df.filter(df.repo == 'apache/spark') \
    .groupBy('author') \
    .count() \
    .orderBy('count', ascending=False) \
    .first()

# Output:
# Row(author='Matei Zaharia <matei@eecs.berkeley.edu>', count=683)

# 3. Afficher dans la console les plus gros contributeurs du projet apache/spark sur les 6 derniers mois.

# Enable use of 'E' in time format
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df_dates = df.withColumn("timestamp", unix_timestamp(col("date"), "EEE MMM dd HH:mm:ss yyyy Z").cast("timestamp"))
six_months_ago = datetime.datetime.now() - datetime.timedelta(days=180)
df_dates.filter(df.repo == 'apache/spark') \
    .filter(df_dates["timestamp"] >= six_months_ago.strftime('%Y-%m-%d %H:%M:%S')) \
    .groupBy('author') \
    .count() \
    .orderBy('count', ascending=False) \
    .show()

# Output:
# nothing because the dataset is outdated.

# 4. Afficher dans la console les 10 mots qui reviennent le plus dans les messages de commit sur l’ensemble des projets.

# remove all rows that have null values on any column
df_no_null = df.na.drop()

# tokenzie the commit message column
tokenizer = Tokenizer(inputCol="message", outputCol="words_token")
tokenized = tokenizer.transform(df_no_null).select('words_token')

# remove stop words from the tokenized column
remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
# remove empty words
data_clean = remover.transform(tokenized) \
    .select('words_clean') \
    .where(size(col("words_clean")) > 0)

result = data_clean.withColumn('word', explode(col('words_clean'))) \
    .groupBy('word') \
    .count() \
    .sort('count', ascending=False)

result.filter(result.word != '').show()

# Output:
# +---------------+-----+
# |           word|count|
# +---------------+-----+
# |            fix|82251|
# |            add|73657|
# |          merge|71816|
# |         branch|40341|
# |         remove|38339|
# |            use|32890|
# |         update|31808|
# |           test|27310|
# |remote-tracking|21173|
# |          tests|19183|
# +---------------+-----+
