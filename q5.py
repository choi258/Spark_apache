from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from decimal import Decimal

conf = (SparkConf()
    .setMaster("local[*]")
    .setAppName("SQLJob")
)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

lines = sc.textFile("/user/choi257/input/ratings.dat")
parts = lines.map(lambda l: l.split("::"))
ratings = parts.map(lambda p: {
    "userid": int(p[0]),
    "movieid": int(p[1]),
    "rating": int(p[2]),
    "timestamp": int(p[3])
})
schemaRatings = sqlContext.inferSchema(ratings)
schemaRatings.registerAsTable("ratings")

lines = sc.textFile("/user/choi257/input/users.dat")
parts = lines.map(lambda l: l.split("::"))
users = parts.map(lambda p: {
    "userid": int(p[0]),
    "gender": p[1],
    "age": int(p[2]),
    "occupation": int(p[3]),
    "zipcode": p[4]
})
schemaUsers = sqlContext.inferSchema(users)
schemaUsers.registerAsTable("users")


results = sqlContext.sql('select u.age, avg(r.rating) as avgrat from ratings as r, users as u where     u.userid = r.userid group by u.age') 
results_mapped = results.map(lambda p: str(p.age) + "::" + str(round(p.avgrat,1)))

# coalesce to reduce the number of partitions generated in HDFS;
# 4 may be too small for some jobs;
results_coalesced = results_mapped.coalesce(4)
results_coalesced.saveAsTextFile("/user/choi257/spark/out/q5")
