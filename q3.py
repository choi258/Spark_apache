from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = (SparkConf()
    .setMaster("local[*]")
    .setAppName("SQLJob")
)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

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

results = sqlContext.sql('select u.occupation, count(u.occupation) as occupCount from users as u group by u.occupation')
results_mapped = results.map(lambda p: str(p.occupation) + "::" + str(p.occupCount))

# coalesce to reduce the number of partitions generated in HDFS;
# 4 may be too small for some jobs;
results_coalesced = results_mapped.coalesce(4)
results_coalesced.saveAsTextFile("/user/choi257/spark/out/q3")
