from pyspark.sql import SparkSession


spark =SparkSession.builder.appName

#create Connection

input_uri = "mongodb://182.23.45.57/dataengineering.employee1"
output_uri = "mongodb://182.23.45.57/dataengineering.employee1"

myspark = SparkSession \
    .builder \
    .appName("Ticket") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2") \
    .getOrCreate()
sqlContext = SparkSession(myspark)

#extract data from json

employee_df= myspark.read.format("com.mongodb.spark.sql.DefaultSource").load()
myspark.sparkContext.setLogLevel("ERROR")
#employee_df.show()
employee_df.createOrReplaceTempView("tempemployee")

#transform data from json
#sqlContext.sql("SELECT * FROM tempemployee").show()
#sqlContext.sql("SELECT count (*) FROM tempemployee WHERE salary > 4000000").show()
new_df = sqlContext.sql("SELECT first_name, salary FROM tempemployee WHERE salary > 5000000")
new_df.count()
#new_df.show()

new_df.write.format("mongo") \
    .option("uri","mongodb://182.23.45.57:27017/") \
    .option("database","dataengineering") \
    .option("collection", "employee3") \
    .mode("append").save()
    