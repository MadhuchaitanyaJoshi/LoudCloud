from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, Row,DoubleType
import math

from pyspark.sql.types import DateType
from py4j.java_gateway import java_import

sc = SparkContext()
spark = SparkSession(sc)
spark.conf.set("spark.sql.legacy.utcTimestampFunc.enabled", "true")
java_import(sc._jvm, 'org.apache.hadoop.fs.Path')
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

user = spark.read.csv("F:\\LoudCloud\\Spark_Test\\dataset\\u.user",sep="|").toDF('user id', 'age', 'gender', 'occupation', 'zip code')
print("-------------------------------USER Dataframe----------------------------------")
print(user.show(50))
user.createOrReplaceTempView('usert')
data = spark.read.csv("F:\\LoudCloud\\Spark_Test\\dataset\\u.data",sep="\t").toDF('user id','item id','rating','timestamp')
print("-------------------------------Data Dataframe----------------------------------")
print(data.show(50))

print("-------------------------------GENRE Dataframe----------------------------------")
genre = spark.read.csv("F:\\LoudCloud\\Spark_Test\\dataset\\u.genre",sep="|").toDF('genre','genre_id')
print(genre.show(50))

print("-------------------------------ITEM Dataframe----------------------------------")
item = spark.read.csv("F:\\LoudCloud\\Spark_Test\\dataset\\u.item",sep="|").toDF('movie id','movie title','release date','video release date','IMDb URL','unknown','Action','Adventure','Animation',"Children's",'Comedy','Crime','Documentary','Drama','Fantasy','Film-Noir','Horror','Musical','Mystery','Romance','Sci-Fi','Thriller','War','Western')
print(item.show(50))

print("-------------------------------Q1 ANSWER----------------------------------")

Q1 = item.drop("IMDb URL").coalesce(1).write.option("sep","|").option("header","false").csv("F:\\LoudCloud\\Spark_Test\\dataset\\solutions\\u.item_clean")

file = fs.globStatus(sc._jvm.Path('F:\\LoudCloud\\Spark_Test\\dataset\\solutions\\u.item_clean\\part*'))[0].getPath().getName()
fs.rename(sc._jvm.Path('F:\\LoudCloud\\Spark_Test\\dataset\\solutions\\u.item_clean\\' + file), sc._jvm.Path('F:\\LoudCloud\\Spark_Test\\dataset\\solutions\\u.item_clean.csv'))
fs.delete(sc._jvm.Path('F:\\LoudCloud\\Spark_Test\\dataset\\solutions\\u.item_clean\\'), True)

print("-------------------------------Q2 ANSWER----------------------------------")

Q2Prep = item.withColumn("release date",to_date(col("release date"),"dd-MMM-yyyy")).withColumnRenamed("release date","release_date").withColumn("movie id".lstrip().rstrip(),item["movie id"].cast(IntegerType()))
Q2Prep2 = Q2Prep.withColumn("year",year(col("release_date"))).withColumn("month",month(col("release_date"))).withColumnRenamed("movie title","movie_title").withColumnRenamed("movie id","movie_id")
Q2 = Q2Prep2.filter((col("year")==1995)).orderBy('month',ascending=True).select(col("year"),col("month"),col("movie_title"))
print(Q2.show(40))
Q2Prep2.createOrReplaceTempView("q2p")


print("-------------------------------Q3 ANSWER----------------------------------")

Q3 = Q2Prep2.orderBy(col("year"),col("movie title"))
print(Q3.show(40))

print("-------------------------------Q4 ANSWER----------------------------------")

q4prep = data.join(Q2Prep2.select("movie_title".rstrip().lstrip(),"movie_id"),data["item id"]==Q2Prep2["movie_id"])
q5prep = q4prep.drop("timestamp","user id").withColumn("item id".rstrip().lstrip(),data["item id"].cast(IntegerType())).withColumn("rating".lstrip().rstrip(),data["rating"].cast(IntegerType()))
q5prep.createOrReplaceTempView("q4table")

Q4 = spark.sql("select movie_id,movie_title,SUM(case when rating=4 then 1 else 0 end) as r4,SUM(case when rating=5 then 1 else 0 end) as r5 from q4table group by movie_id,movie_title order by movie_id")
print(Q4.show(40))

Q4.createOrReplaceTempView("q4")

print("-------------------------------Q5 ANSWER----------------------------------")

Q5 = spark.sql("select movie_title,year,month from (select movie_title,year,month,(case when movie_title like '%a%' then 1 else 0 end)+(case when movie_title like '%o%' then 1 else 0 end)+(case when movie_title like '%i%' then 1 else 0 end)+(case when movie_title like '%u%' then 1 else 0 end) as sm from q2p) where sm>2 order by year,month")
print(Q5.show(50))
Q5.createOrReplaceTempView("q5")

print("-------------------------------Q6 ANSWER----------------------------------")

item2 = item.withColumnRenamed("movie title","movie").withColumnRenamed("children's","children").withColumnRenamed("film-noir","filmnoir").withColumnRenamed("sci-fi","scifi")
genre = genre.withColumn("genre_id",lit(1))
item2.createOrReplaceTempView("it")
Q6 = spark.sql("select movie,'unknown' as genre from it where unknown==1").unionAll\
    (spark.sql("select movie,'Action' from it where Action==1")).unionAll\
    (spark.sql("select movie,'Adventure' from it where Adventure==1")).unionAll\
    (spark.sql("select movie,'Animation' from it where Animation==1")).unionAll\
    (spark.sql("select movie,'children' from it where children==1")).unionAll\
    (spark.sql("select movie,'comedy' from it where comedy==1")).unionAll\
    (spark.sql("select movie,'documentary' from it where documentary==1")).unionAll\
    (spark.sql("select movie,'drama' from it where drama==1")).unionAll\
    (spark.sql("select movie,'fantasy' from it where fantasy==1")).unionAll\
    (spark.sql("select movie,'film-noir' from it where filmnoir==1")).unionAll\
    (spark.sql("select movie,'horror' from it where horror==1")).unionAll\
    (spark.sql("select movie,'musical' from it where musical==1")).unionAll\
    (spark.sql("select movie,'mystery' from it where mystery==1")).unionAll\
    (spark.sql("select movie,'romance' from it where romance==1")).unionAll\
    (spark.sql("select movie,'sci-fi' from it where scifi==1")).unionAll\
    (spark.sql("select movie,'thriller' from it where thriller==1")).unionAll\
    (spark.sql("select movie,'war' from it where war==1")).unionAll\
    (spark.sql("select movie,'western' from it where western==1")).orderBy("genre")
print(Q6.show(50))


print("-------------------------------Q7 ANSWER----------------------------------")


Q7 = spark.sql("select mt,tr,year,row_number() over (partition by year order by tr desc) as rnk from (select q4.movie_title as mt,(r4+r5) as tr,year from q4 inner join q5 on q4.movie_title==q5.movie_title order by year)")
print(Q7.show(50))

print("-------------------------------Q8 ANSWER----------------------------------")
Q8 = spark.sql("select occupation,count(occupation)as num_crit from usert group by occupation order by num_crit desc")
print(Q8.show(50))

print("-------------------------------Q9 ANSWER----------------------------------")
data2 = data.withColumnRenamed('user id','user-id')

#Create Dataframe for age group like : [Row(rang=5,ind='0-5')]
i=1
rng = []
while(i<=80/5):
    rng.append((i*5,str(i*5-4)+str('-')+str(i*5)))
    i = i+1

RG= spark.createDataFrame(rng,['rang','ind'])
RG.createOrReplaceTempView('rnge')

Q9prep = data2.withColumn("rating",data2["rating"].cast(IntegerType())).join(item,data2["item id"]==item["movie id"])
Q9prep2 = Q9prep.withColumnRenamed("movie id","movie_id").withColumnRenamed("item id","item_id").join(user,user["user id"]==Q9prep["user-id"]).withColumnRenamed("user id","user_id").withColumn("age",user["age"].cast(IntegerType())).withColumnRenamed("Children's","children").withColumnRenamed("Film-Noir","filmnoir").withColumnRenamed("Sci-Fi","scifi").drop("user-id")
Q9prep2.createOrReplaceTempView("q9t")

Q9prep3 = spark.sql("select rang,SUM(t1.Western) as western,SUM(t1.War) as war,SUM(unknown) as unknown,SUM(Action) as Action,SUM(Adventure) as Adventure,SUM(Animation) as Animation,SUM(children) as Children,SUM(Comedy) as Comedy,SUM(Crime) as Crime,SUM(Documentary) as Documentary,SUM(Drama) as Drama,SUM(Fantasy) as Fantasy,SUM(filmnoir) as filmnoir,SUM(Horror) as Horror,SUM(Musical) as Musical,SUM(Mystery) as Mystery,SUM(Romance) as Romance,SUM(scifi) as scifi,SUM(Thriller) as Thriller,count(rating) as rating from (select *,case when age<=5 then 5 when age>5 and age<=10 then 10 when age>10 and age<=15 then 15 when age>15 and age<=20 then 20 when age>20 and age<=25 then 25 when age>25 and age<=30 then 30 when age>30 and age<=35 then 35 when age>35 and age<=40 then 40 when age>40 and age<=45 then 45 when age>45 and age<=50 then 50 when age>50 and age<=55 then 55 when age>55 and age<=60 then 60 when age>60 and age<=65 then 65 when age>65 and age<=70 then 70 when age>70 and age<=75 then 75 when age>75 and age<=80 then 80 end as newage from q9t)t1  inner join rnge on t1.newage==rnge.rang where rating>3 group by rang")
Q9prep3 = Q9prep3.withColumn("rating",Q9prep3["rating"].cast(DoubleType())).withColumn("rating",Q9prep3["rang"].cast(DoubleType()))

count=0
lst = []
cols = Q9prep3.columns
for rows in Q9prep3.rdd.collect():
    i=1
    for row in range(1,len(rows)):
        count = count + 1
        lst.append(Row(age_group=rows[0],movietype=cols[i],reviews=rows[i]))
        i=i+1

Q9partial = spark.createDataFrame(lst)

Q9partial.createOrReplaceTempView("q9partial")

Q9 = spark.sql("select  * from (select age_group,movietype,reviews,row_number() over (partition by age_group order by reviews desc) as topr from q9partial) where topr in(1,2,3) order by age_group,topr")
Q9 = Q9.join(RG,Q9["age_group"]==RG["rang"],"left_outer").select(RG["ind"].alias("age_group"),"movietype","reviews")
print(Q9.show(40))
