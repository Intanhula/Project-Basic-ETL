##import required libraries
import pyspark
from pyspark.sql import SparkSession


##create spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Extract Data SQL") \
    .config('spark.driver.extraClassPath', "/home/postgresql-42.7.3.jar") \
    .getOrCreate()



##read table from db job_detail
def extract_job_detail_to_df():
    job_detail_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://182.23.45.67:5432/job_detail") \
        .option("dbtable","job_detail") \
        .option("user" , "postgres") \
        .option("password", "Lmd%55@123") \
        .option("driver" , "org.postgresql.Driver") \
        .load()
    return job_detail_df

 
##print job_detail
#print(job_detail_df.show())

##read table from db users
def extract_users_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://182.23.45.68:5432/postgres") \
        .option("dbtable","users_job.users") \
        .option("user" , "postgres") \
        .option("password", "Lmd%55@123") \
        .option("driver" , "org.postgresql.Driver") \
        .load()
    return users_df
    

##print users
#print(users_df.show())

def transform_avg_ratings(job_detail_df, users_df):
#Use groupBy and mean to aggregate the column
    max_rating= job_detail_df.groupBy('id').max('availability')
    

# Join the tables using the users column
    df = users_df.join(max_rating, users_df.id_job == job_detail_df.id)
    df = df.drop("id")
    return df


def load_df_to_db(df):
    mode = "overwrite"
    url = "jdbc:postgresql://182.23.45.67:5432/job_detail"
    properties = {"user": "postgres",
                   "password": "Lmd%55@123",
                   "driver": "org.postgresql.Driver"
                   }
    df.write.jdbc(url=url,
                  table = "max_ratings",
                  mode = mode,
                  properties = properties)

if __name__ == "__main__":
    job_detail_df= extract_job_detail_to_df()
    users_df = extract_users_to_df()
    rating_df = transform_avg_ratings(job_detail_df, users_df)
    load_df_to_db(rating_df)
##print final data frame
#print(df.show())


    



