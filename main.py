
import pyspark
import pandas as pd
import pyspark.sql.types as t
import pyspark.sql.functions as f
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
    t.StructField("titleId", t.StringType(), False),
    t.StructField("ordering", t.IntegerType(), False),
    t.StructField("title", t.StringType(), True),
    t.StructField("region", t.StringType(), True),
    t.StructField("language", t.StringType(), False),
#    t.StructField("types", t.ArrayType(t.StringType(), True), True),
    t.StructField("types", t.StringType(), False),
    t.StructField("attributes", t.StringType(), False),
    t.StructField("isOriginalTitle", t.IntegerType(), True)])

schema2_title_basics = t.StructType([
    t.StructField("tcoNst", t.StringType(), True),
    t.StructField("titleType", t.StringType(), True),
    t.StructField("primaryTitle", t.StringType(), True),
    t.StructField("originalTitle", t.StringType(), True),
    t.StructField("isAdult", t.IntegerType(), True),
#    t.StructField("isAdult", t.BooleanType(), True),
    t.StructField("startYear", t.IntegerType(), False),
    t.StructField("endYear", t.IntegerType(), False),

#    t.StructField("startYear", t.DateType(), True),
#    t.StructField("endYear", t.DateType(), True),

    t.StructField("runtimeMinutes", t.IntegerType(), False),
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
    t.StructField("birthYear", t.IntegerType(), True),
    t.StructField("deathYear", t.IntegerType(), True),
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



# *******************  TASK 1 ************************************************************

def func_task1() :
    from_csv_df1 = spark_session.read.csv(path1_acaS,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema1_acaS)
    from_csv_df1.show(100)


    for_ua = from_csv_df1.select("title", "region").filter(f.col("region") == "UA")
    for_ua.show(200)
    path_to_saved_acaS = 'D:\\DataSets_project\\Saved_files\\task1\\saved_acaS'
    for_ua.write.csv(path_to_saved_acaS, header=True, mode="overwrite")
    print("Saved to D:\\DataSets_project\\Saved_files\\task1\\saved_acaS")
    return

# *******************  END OF TASK 1 ************************************************************
'''
# first_100_df.show()

from_csv_df1.printSchema()

'''
'''
from_csv_df2 = spark_session.read.csv(path2_title_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep="\t",
                                      schema=schema2_title_basics)

# my_list = from_csv_df2["startYear"].pd.tolist()
# print(my_list)
from_csv_df2.show()
# from_csv_df2.printSchema()
'''
'''

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
# *******************  TASK 2 ************************************************************
def func_task2() :
    from_csv_df7 = spark_session.read.csv(path7_name_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema7_name_basics)
    # from_csv_df7.show(200)
    for_task2 = from_csv_df7.select("primaryName", "birthYear")\
        .filter((f.col("birthYear") > 1900) & (f.col("birthYear") < 2000))
    # from_csv_df7.printSchema()
    for_task2.show(200)

    path_to_saved_name_basics = 'D:\\DataSets_project\\Saved_files\\task2\\saved_name_basics'
    for_task2.write.csv(path_to_saved_name_basics)
    print("Saved to D:\\DataSets_project\\Saved_files\\task2\\saved_name_basics")
    return
# *******************  END OF TASK 2 ************************************************************


# *******************  TASK 3 ************************************************************
def func_task3() :
    from_csv_df2 = spark_session.read.csv(path2_title_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema2_title_basics)
    # from_csv_df2.show(200)

    for_task3 = from_csv_df2.select("primaryTitle", "originalTitle", "runtimeMinutes")\
        .filter(f.col("runtimeMinutes") > 120)
    # from_csv_df2.printSchema()
    for_task3.show(100)

    path_to_saved_title_basics = 'D:\\DataSets_project\\Saved_files\\task3\\saved_title_basics'
    # for_task3.write.csv(path_to_saved_title_basics)
    print("Saved to D:\\DataSets_project\\Saved_files\\task3\\saved_title_basics")
    return
# ***************************** END OF TASK 3 ***************************************

# *******************  TASK 4 ************************************************************
def func_task4() :
    from_csv_df5 = spark_session.read.csv(path5_principals,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema5_principals)
    # from_csv_df5.show(100)

    for1_task4_df = from_csv_df5.select("tconst", "nconst", "characters").filter((f.col("category") == "actor") |
                                                                                    (f.col("category") == "actress"))

    # for1_task4_df.show()
    # for1_task4_df.count()
    # for1_task4_df.printSchema()
    # ********************************* Getting actor's names **********************

    from_csv_df7 = spark_session.read.csv(path7_name_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema7_name_basics)
    Act_names_df = from_csv_df7.select("nconst", "primaryName")
    # Act_names_df.show(30)
    Act_names_df.count()
    Exp_Act_names_df = for1_task4_df.join(Act_names_df, on=for1_task4_df["nconst"] == Act_names_df["nconst"],
                                          how='left')
    #    Exp_Act_names_df.show()

    # ***************** Join Titles of films *********************************

    from_csv_df2 = spark_session.read.csv(path2_title_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep="\t",
                                      schema=schema2_title_basics)
    Tit_films_df = from_csv_df2.select("tconst", "primaryTitle")
    # Tit_films_df.show()
    # print(Tit_films_df.count())
    Exp_Films_df = Exp_Act_names_df.join(Tit_films_df, on=Exp_Act_names_df["tconst"] == Tit_films_df["tconst"],
                                         how='left')
    # Exp_Films_df.show()
    Res_Task4_df = Exp_Films_df.select("primaryName", "characters", "primaryTitle")
    Res_Task4_df.show()
    # print("Count lines = ", Res_Task4_df.count())
    # Источник: https://pythonstart.ru/list/preobrazovanie-freyma-dannyh-v-spisok-python-4-sposoba

    path_to_saved_Res_Task4_df = 'D:\\DataSets_project\\Saved_files\\task4\\saved_saved_Res_Task4_df'
    # path_to_saved_principals = 'D:\\DataSets_project\\Saved_files\\task4\\saved_principals'
    Res_Task4_df.write.csv(path_to_saved_Res_Task4_df, header=True, mode="overwrite")
    print("Res_Task4_df Saved to D:\\DataSets_project\\Saved_files\\task4\\saved_Res_Task4_df")
    return
# *******************  END OF TASK 4 ******************************************************

# *******************  TASK 5 ************************************************************

# **************************** Read Ratings *********************
def func_task5() :
    from_csv_df2 = spark_session.read.csv(path2_title_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep="\t",
                                      schema=schema2_title_basics)

    # from_csv_df2.show(30)
    # print("Count lines from_csv_df2 = ", from_csv_df2.count())

    # **************************** Read Ratings *********************

    from_csv_df6 = spark_session.read.csv(path6_ratings,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema6_ratings)
    # from_csv_df6.show()
    # from_csv_df6.printSchema()
    # **************************** End Read Ratings *********************

    # All_films_adult_df = from_csv_df2.select("tconst", "primaryTitle", "isAdult").filter(f.col("isAdult") == 1)
    All_films_adult_df = from_csv_df2.select("tconst", "primaryTitle", "isAdult")
    # All_films_adult_df.show()
    # print("Count lines All_films_adult_df = ", All_films_adult_df.count())
    Rating_films = All_films_adult_df.join(from_csv_df6,
        on=All_films_adult_df["tconst"] == from_csv_df6["tconst"], how='left')
    # Rating_films.printSchema()

    Rating_films_res = Rating_films.select("primaryTitle", "averageRating").filter(f.col("averageRating") > 0)
    # Rating_films_res.show(1000, truncate=False)
    Ordered_Rating_films = Rating_films_res.orderBy("averageRating", ascending=False)
    # Ordered_Rating_films.show(10000, truncate=False)
    Best_100_films = Ordered_Rating_films.limit(100)

    Rating_films = Rating_films.orderBy("count", ascending=False)
    from_csv_df1 = spark_session.read.csv(path1_acaS,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema1_acaS)
    Regions_df = from_csv_df1.select("titleId", "ordering", "region")
    # Regions_df.show()
    # print("Count lines Regions_df = ", Regions_df.count())
    # ***************** Adding regions ************************

    Add_adult_df = Regions_df.join(All_films_adult_df, on=Regions_df["titleId"] == All_films_adult_df["tconst"],
                                   how='left')
    # Add_adult_df.show()
    # print("Count lines Add_adult_df = ", Add_adult_df.count())

    Only_adult_df = Add_adult_df.select("titleId", "primaryTitle", "ordering", "region", "isAdult")\
        .filter(f.col("isAdult") == 1)
    # Only_adult_df.show(30)
    # print("Count lines Only_adult_df = ", Only_adult_df.count())
    # *********************** Grooping by region ***********************

    Groop_reg_df = Only_adult_df.groupby("region").count()
    # Groop_reg_df.show()
    # print("Count lines Groop_reg_df = ", Groop_reg_df.count())
    Ordered_df = Groop_reg_df.orderBy("count", ascending=False)
    # Ordered_df.show(117)

    # **************** Write Result 1 to file *****************************

    path_to_saved_Res_Task5_1_df = 'D:\\DataSets_project\\Saved_files\\task5_1\\saved_Res_Task5_1_df'
    # path_to_saved_principals = 'D:\\DataSets_project\\Saved_files\\task4\\saved_principals'
    Ordered_df.write.csv(path_to_saved_Res_Task5_1_df, header=True, mode="overwrite")
    print("Res_Task5_1_df Saved to D:\\DataSets_project\\Saved_files\\task4\\saved_Res_Task5_1_df")

    # **************** End Write Result 1 to file *****************************

    # *********************** Grooping by films ******************************

    Groop_on_films_df = Only_adult_df.groupby("primaryTitle").count()
    # Groop_on_films_df.show()
    Ordered_film = Groop_on_films_df.orderBy("count", ascending=False)
    Ordered_film.show(100, truncate=False)
    print("Count lines Groop_on_films_df = ", Groop_on_films_df.count())
    First100_Ordered_film = Ordered_film.limit(100)
    # First100_Ordered_film.show()
    # print("Count lines First100_Ordered_film = ", First100_Ordered_film.count())

    # **************** Write Result 2 to file *****************************

    path_to_saved_Res_Task5_2_df = 'D:\\DataSets_project\\Saved_files\\task5_1\\saved_Res_Task5_2_df'
    # First100_Ordered_film.write.csv(path_to_saved_Res_Task5_2_df, header=True, mode="overwrite")
    Best_100_films.write.csv(path_to_saved_Res_Task5_2_df, header=True, mode="overwrite")
    print("Res_Task5_2_df Saved to D:\\DataSets_project\\Saved_files\\task4\\saved_Res_Task5_2_df")

    # **************** End Write Result 2 to file *****************************

    from_csv_df5 = spark_session.read.csv(path5_principals,
                                          header=True,
                                          nullValue='null',
                                          dateFormat='yyyy',
                                          sep='\t',
                                          schema=schema5_principals)
    # from_csv_df5.show(100)

    # for_task4 = from_csv_df5.select("primaryTitle", "runtimeMinutes").filter(f.col("runtimeMinutes") > 120)
    # from_csv_df2.printSchema()
    # for_task4.show(100)

    # path_to_saved_principals = 'D:\\DataSets_project\\Saved_files\\task4\\saved_principals'
    # for_task4.write.csv(path_to_saved_principals)
    print("Saved to D:\\DataSets_project\\Saved_files\\task4\\saved_principals")
    return
# *******************  END OF TASK 5 ************************************************************

# *******************  TASK 6 ************************************************************
def func_task6() :
    # ************************* Read title_basics **********************
    from_csv_df2 = spark_session.read.csv(path2_title_basics,\
                                          header=True,
                                          nullValue='null',
                                          dateFormat='yyyy',
                                          sep="\t",
                                          schema=schema2_title_basics)
    # **** Chois need field tconst and originalTitle from title_basics **********************

    Serial_names = from_csv_df2.select("tcoNst", "originalTitle")  # **** Chois need field tconst and originalTitle

    # ************************* Read episode **********************

    from_csv_df4 = spark_session.read.csv(path4_episode,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema4_episode)

    # **************** Виконання Task 6 *******************

    Epizode_serials = from_csv_df4.join(Serial_names, on=from_csv_df4["tcoNst"] == Serial_names["tcoNst"], how='left')
    Epizode_serials.printSchema()
    Res_Epizode = Epizode_serials.select("originalTitle", "episodeNumber")
    # Res_Epizode.show()
    Groop_epizode = Res_Epizode.groupby("originalTitle").count()
    # Groop_epizode.show()
    Ordered_Groop_epizode = Groop_epizode.orderBy("count", ascending=False)
    Res_Task6 = Ordered_Groop_epizode.limit(50)

    # **************** Write Res Task 6 *******************
    path_to_saved_epizodes = 'D:\\DataSets_project\\Saved_files\\task6\\saved_epizodes'
    Res_Task6.write.csv(path_to_saved_epizodes, header=True, mode="overwrite")
    print("Saved to D:\\DataSets_project\\Saved_files\\task6\\saved_epizodes")
    return
# *******************  END OF TASK 6 ************************************************************

# *******************  TASK 7 *******************************************************************
def func_task7() :
    # *************** Read title_basics  *********************************


    from_csv_df2 = spark_session.read.csv(path2_title_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep="\t",
                                      schema=schema2_title_basics)
                                     
    from_csv_df5 = spark_session.read.csv(path5_principals,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema5_principals)
    from_csv_df5.show(100)

    Added_decade = from_csv_df2.withColumn("Decade", f.lit(1))
    Added_decade.printSchema()
    Added_decade.select('startYear', 'Decade').show(100)
    Added_decade2 = Added_decade.withColumn('Decade', f.col('startYear') // 10)


    # for_task4 = from_csv_df5.select("primaryTitle", "runtimeMinutes").filter(f.col("runtimeMinutes") > 120)

    # from_csv_df2.printSchema()
    # for_task4.show(100)

    # path_to_saved_Task7 = 'D:\\DataSets_project\\Saved_files\\task7\\saved_Task7'
    # for_task7.write.csv(path_to_saved_Task7, header=True, mode="overwrite")
    # print("Saved to D:\\DataSets_project\\Saved_files\\task4\\saved_Task7")
    return
# *******************  END OF TASK 7 ************************************************************

# *******************  TASK 8 ************************************************************
def func_task8() :
    # ******************* Read title_basics ********************

    from_csv_df2 = spark_session.read.csv(path2_title_basics,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep="\t",
                                      schema=schema2_title_basics)

    # ******************* Read ratings ********************
    from_csv_df6 = spark_session.read.csv(path6_ratings,
                                      header=True,
                                      nullValue='null',
                                      dateFormat='yyyy',
                                      sep='\t',
                                      schema=schema6_ratings)
    # from_csv_df6.show(100)
    # ******************* End Reading ratings ********************


    # for_task4 = from_csv_df5.select("primaryTitle", "runtimeMinutes").filter(f.col("runtimeMinutes") > 120)


    # path_to_saved_task8 = 'D:\\DataSets_project\\Saved_files\\task8\\saved_task8'
    # for_task4.write.csv(path_to_saved_task8, header=True, mode="overwrite")
    # print("Saved to D:\\DataSets_project\\Saved_files\\task8\\saved_task8")
    return
# *******************  END OF TASK 8 ************************************************************
func_task1()
'''
func_task2()
func_task3()
func_task4()
func_task5()
func_task6()
func_task7()
func_task8()
'''
