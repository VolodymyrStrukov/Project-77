
import pyspark
import pyspark.sql.types as t
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())
#C:\Users\VMC\PycharmProjects\Test 1 pyspark\venv\Lib\site-packages\pyspark\bin

# data = [("Tonja",18), ("Nata",44)]
# schema = t.StructType([
#    t.StructField("name", t.StringType(), True),
#   t.StructField("age", t.IntegerType(), True)])
# df=spark_session.createDataFrame(data,schema)
# df.show()
schema1_acaS = t.StructType([
    t.StructField("titleId", t.StringType(), True),
    t.StructField("ordering", t.IntegerType(), True),
    t.StructField("title", t.StringType(), False),
    t.StructField("region", t.StringType(), False),
    t.StructField("language", t.StringType(), True),
#    t.StructField("types", t.ArrayType(t.StringType(), True), True),
    t.StructField("types", t.StringType(), True),
    t.StructField("attributes", t.StringType(), True),
    t.StructField("isOriginalTitle", t.BooleanType(), True)])

schema2_title_basics = t.StructType([
    t.StructField("tcoNst", t.StringType(), True),
    t.StructField("titleType", t.StringType(), True),
    t.StructField("primaryTitle", t.StringType(), True),
    t.StructField("originalTitle", t.StringType(), True),
    t.StructField("isAdult", t.BooleanType(), True),
    t.StructField("startYear", t.DateType(), True),
    t.StructField("endYear", t.DateType(), True),
    t.StructField("runtimeMinutes", t.IntegerType(), True),
#    t.StructField("genres", t.ArrayType(t.StringType(), True), True)])
    t.StructField("genres", t.StringType(), True)])

schema3_crew = t.StructType([
    t.StructField("tcoNst", t.StringType(), True),
    t.StructField("directors", t.StringType(), True),
    t.StructField("writers", t.StringType(), True),
    ])

schema4_episode = t.StructType([
    t.StructField("tcoNst", t.StringType(), True),
    t.StructField("parentTcoNst", t.StringType(), True),
    t.StructField("seasonNumber", t.IntegerType(), True),
    t.StructField("episodeNumber", t.IntegerType(), True)])

schema5_principals = t.StructType([
    t.StructField("tcoNst", t.StringType(), True),
    t.StructField("ordering", t.IntegerType(), False),
    t.StructField("ncoNst", t.StringType(), True),
    t.StructField("category", t.StringType(), True),
    t.StructField("job", t.StringType(), True),
    t.StructField("characters", t.StringType(), True)
    ])

schema6_ratings = t.StructType([
    t.StructField("tcoNst", t.StringType(), True),
    t.StructField("averageRating", t.FloatType(), True),
    t.StructField("numVotes", t.IntegerType(), False)])

schema7_name_basics = t.StructType([
    t.StructField("ncoNst", t.StringType(), True),
    t.StructField("primaryName", t.StringType(), True),
    t.StructField("birthYear", t.DateType(), True),
    t.StructField("deathYear", t.DateType(), True),
    t.StructField("primaryProfession", t.StringType(), True),
    t.StructField("knownForTitles", t.StringType(), True)])

path1_acaS = "D:\\DataSets_project\\title.akas.tsv.gz"
path2_title_basics = "D:\\DataSets_project\\title.basics.tsv.gz"
path3_crew = "D:\\DataSets_project\\title.crew.tsv.gz"
path4_episode = "D:\\DataSets_project\\title.episode.tsv.gz"
path5_principals = "D:\\DataSets_project\\title.principals.tsv.gz"
path6_ratings = "D:\\DataSets_project\\title.ratings.tsv.gz"
path7_name_basics = "D:\\DataSets_project\\name.basics.tsv.gz"
# from_csv_df = spark_session.read.csv(path1_acaS)
# from_csv_df.show()
'''
from_csv_df1 = spark_session.read.csv(path1_acaS,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema1_acaS)
# from_csv_df1.show(truncate=False)
from_csv_df1.show()
from_csv_df1.printSchema()


from_csv_df2 = spark_session.read.csv(path2_title_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep="\t",
                                      schema=schema2_title_basics)
from_csv_df2.show()
from_csv_df2.printSchema()


from_csv_df3 = spark_session.read.csv(path3_crew,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema3_crew)
from_csv_df3.show()
from_csv_df3.printSchema()

from_csv_df4 = spark_session.read.csv(path4_episode,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema4_episode)
from_csv_df4.show()
from_csv_df4.printSchema()

from_csv_df5 = spark_session.read.csv(path5_principals,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema5_principals)
from_csv_df5.show()
from_csv_df5.printSchema()

from_csv_df6 = spark_session.read.csv(path6_ratings,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema6_ratings)
from_csv_df6.show()
from_csv_df6.printSchema()
'''
from_csv_df7 = spark_session.read.csv(path7_name_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema7_name_basics)
from_csv_df7.show()
from_csv_df7.printSchema()


print("KKKKKKKKKKKKKKKKKKKKKKK")

