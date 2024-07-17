from pyspark.sql import SparkSession

input_uri = "mongodb://182.23.45.57/Ticket.Ticketlist"
output_uri = "mongodb://182.23.45.57/Ticket.Ticketlist"


myspark = SparkSession \
    .builder \
    .appName("Ticket") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2") \
    .getOrCreate()
sqlContext =SparkSession(myspark)

ticket_list_df= myspark.read.format("com.mongodb.spark.sql.DefaultSource").load()

print(ticket_list_df.show())
myspark.sparkContext.setLogLevel("ERROR")