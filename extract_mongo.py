from pyspark.sql import SparkSession #import library

input_uri = "mongodb://182.23.45.57/Ticket.Ticketlist" # get input uri database and collection
output_uri = "mongodb://182.23.45.57/Ticket.Ticketlist" # get output uri database and collection


myspark = SparkSession \
    .builder \
    .appName("Ticket") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2") \
    .getOrCreate()  #Initiate SparkSession
sqlContext =SparkSession(myspark) # initiate sqlcontext 

ticket_list_df= myspark.read.format("com.mongodb.spark.sql.DefaultSource").load() #initiate data frame

print(ticket_list_df.show()) #display data frame
myspark.sparkContext.setLogLevel("ERROR") #display log level error