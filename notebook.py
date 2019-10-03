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


from pyspark.sql.functions import col, max, desc
maxNamesDF = ssaDF.select("total","year").filter("year in  (1885, 1915, 1945, 1975,  2005)").groupBy("year").max().orderBy("max(total)" , ascending=False).withColumnRenamed("year","max_year").withColumnRenamed("max(total)","max_total")
maxNamesDF.show(10)
outerQueryDF = ssaDF.join(maxNamesDF, (col("year") == col("max_year"))  & (col("max_total") == col("total")))
outerQueryDF.show(5)



#in json when categories is an Array - denorm , count group by
from pyspark.sql.functions import *
databricksBlogDFExp = databricksBlogDF.select("title",explode(col("categories")).alias("categ"))
databricksBlogDFExp.groupBy("categ").count().orderBy("count" , ascending=False).show()
databricksBlogDFExp.show()


# Explode function
from pyspark.sql.functions import explode , col
uniqueCategoriesDF = databricksBlogDF.select("title",explode(("categories")) ).distinct()
display(uniqueCategoriesDF)


# filter contains - explode
from pyspark.sql.functions import array_contains
articlesByMichaelDF = databricksBlogDF.select("title" , explode(col("authors")).alias("author")).filter(col("author") == "Michael Armbrust")



#nested columns
from pyspark.sql.functions import date_format
display(databricksBlogDF.select("title",date_format("dates.publishedOn","yyyy-MM-dd").alias("publishedOn")))
display(databricksBlogDF.select("dates.createdOn", "dates.publishedOn"))


# contains
from pyspark.sql.functions import lower, upper, month, col

homicidesNewYorkDF = (crimeDataNewYorkDF 
  .select(month(col("reportDate")).alias("month"), col("offenseDescription").alias("offense")) 
  .filter(lower(col("offenseDescription")).contains("murder") | lower(col("offenseDescription")).contains("homicide"))
)

display(homicidesNewYorkDF)

# TODO

# rename "city" column to "cities" to avoid having two similarly named cols; also columns are case-blind
cityDataDF = spark.read.parquet("dbfs:/mnt/training/City-Data.parquet").withColumnRenamed("city", "cities")
#display(cityDataDF)
robberyRatesByCityDF = combinedRobberiesByMonthDF.join(cityDataDF.select("estPopulation2016","cities"), col("city")==col("cities"))
display(robberyRatesByCityDF.select("city","month","robberies","estPopulation2016").withColumn("robberies_per_capita",lit(col("robberies")/col("estPopulation2016"))*100).select("city","month","robberies_per_capita"))


from pyspark.sql.functions import lit
robberiesByMonthPhiladelphiaDF_new = robberiesByMonthPhiladelphiaDF.withColumn("city", lit("Philadelphia"))
robberiesByMonthDallasDF_new = robberiesByMonthDallasDF.withColumn("city", lit("Dallas"))
robberiesByMonthLosAngelesDF_new = robberiesByMonthLosAngelesDF.withColumn("city", lit("Los Angeles"))
combinedRobberiesByMonthDF =  robberiesByMonthLosAngelesDF_new.select( "city","month","robberies").union(robberiesByMonthPhiladelphiaDF_new.select( "city","month","robberies").union(robberiesByMonthDallasDF_new.select( "city","month","robberies")))





