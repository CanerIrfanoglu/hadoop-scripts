from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions



def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

if __name__ == "__main__":
    # Create a SparkSession 
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    # Convert it to a RDD of Row objects with (movieID, rating)
    users = lines.map(parseInput)
    # Convert that to a DataFrame
    usersDataset = spark.createDataFrame(users)

    #Write it into MongoDB
    usersDataset.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        #above command says tie this dataset into mongodb 
        .option("uri","mongodb://127.0.0.1/movielens.users")\
        #as an option specfying mongodb server location
        .mode('append')\
        #appends if exists otherwise creates
        .save()
    #Read it back from mongodb to a new df
        readUsers = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        #above command says tie this dataset into mongodb 
        .option("uri","mongodb://127.0.0.1/movielens.users")\
        #as an option specfying mongodb server location
        .load()

        readUsers.createOrReplaceTempView("users")

        sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
        sqlDF.show()

        #Stop the session
        spark.stop()