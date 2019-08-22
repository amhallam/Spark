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