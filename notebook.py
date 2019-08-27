#imports
from pyspark.sql.functions import year

#read parquet 
peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
display(peopleDF)
peopleDF.printSchema()

#queries
from pyspark.sql.functions  import year, avg , max , min , sum
peopleDF.select("*").filter("gender == 'F'").filter(year("birthDate") > 1990 ).select(min(year("birthDate"))).show(10)

#temporary views
peopleDF.createOrReplaceTempView("People10M")
display(spark.sql("SELECT * FROM  People10M WHERE firstName = 'Donna' "))

from pyspark.sql.functions import count, desc

top10FemaleFirstNamesDF = (peopleDF
                           .select("firstName")
                          .filter("gender = 'F'")
                          .groupBy("firstName")
                          .count()
                          .limit(10)
                          .show())
                          
top10FemaleFirstNamesDF.createOrReplaceTempView("Top10FemaleFirstNames")
resultsDF = spark.sql("select * from Top10FemaleFirstNames")
display(resultsDF)

##############################################
##Starting with peopleDF, create a DataFrame called carensDF where:

##The result set has a single record.
##The data set has a single column named total.
##The result counts only
##Females (gender)
##First Name is "Caren" (firstName)
##Born before March 1980 (birthDate)
###################################################


from pyspark.sql.functions import *
#print(peopleDF.filter("gender='F' and firstName='Caren'").show(20))
carensDF = peopleDF.filter("gender='F' and firstName='Caren'")\
.filter(\
  (year(col("birthDate")) < 1980)  | \
  ((year(col("birthDate")) == 1980) & (month(col("birthDate")) < 3))\
       )\
.agg(count("*").alias("total"))
carensDF.show()

##another of doing this : 
peopleDF.createOrReplaceTempView("peopleDF")
