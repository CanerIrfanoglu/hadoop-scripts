from starbase import Connection

c = Connection("127.0.0.1", "8000") 
# 127.0.0.1 my ip adress for the local host 8000 is the port number opened virtual machine

ratings = c.table('ratings')

if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

ratings.create('rating') 
# Creating a column family named ratings

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("/Users/Caner/Desktop/data/udemy hadoop/ml-100k/u.data", "r") 
#Local adress of the file
batch = ratings.batch()
# Creating a batch object
# batch interface from the starbase package. Instead of importing 1-line at a time to HBase gonna do it batch.
for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}}) #movieID to for the cols , rating for the cell values.
# row-id is given by the userID, rating column family gonna populate itself w a rating column of the movieID with the given rating value
ratingFile.close()

print ("Committing ratings data to HBase via REST service\n")
batch.commit(finalize = True)

print ("Get back ratings for some users... \n")
print ("Ratings for user ID 1:\n")
print (ratings.fetch("1"))
print ("Ratings for user ID 33:\n")
print (ratings.fetch("33"))