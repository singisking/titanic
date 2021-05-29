from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("file:/home/hduser/MyCruiseData.csv",header=True,inferSchema=True)
s = F.split(df['Name'],',')
df = df.withColumn("FirstName",s.getItem(1))
df = df.withColumn("LastName",s.getItem(0))
df.registerTempTable("G")
df = spark.sql("select *,\
case when substr(FirstName,1,3) == ' Mr' then 'Male' \
	else 'Female' \
	end as Gender \
	from G")
df = df.withColumn("Age",df['Age'].cast(IntegerType()))
df.registerTempTable("H")
df = spark.sql("select *, \
case when Age BETWEEN 0 and 18 then 'Kids' \
	when Age BETWEEN 18 AND 60 then 'Adults' \
	when Age > 60 then 'Elders' \
	else 'Cannot be determined' \
	end as AgeGroup \
	from H")
df.registerTempTable("I")
df = spark.sql("select *, \
case when substr(Cabin,1,1) == 'C' then 'FirstClass' \
	else 'SecondClass' \
	end as Class \
	from I")
df.write.csv("file:/home/hduser/titanic.csv")
 
