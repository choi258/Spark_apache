from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = (SparkConf()
    .setMaster("local[*]")
    .setAppName("SQLJob")
)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

lines = sc.textFile("/user/choi257/input/movies.dat")
parts = lines.map(lambda l: l.split("::"))
movies = parts.map(lambda p: {
    "movieid": int(p[0]),
    "title": p[1],
    "genres": p[2]
})
schemaMovies = sqlContext.inferSchema(movies)
schemaMovies.registerAsTable("movies")

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

results = sqlContext.sql('select distinct m.title from movies as m, ratings as r where     m.movieid = r.movieid and r.rating = 2')
results_mapped = results.map(lambda p: p.title)

# coalesce to reduce the number of partitions generated in HDFS;
# 4 may be too small for some jobs;
results_coalesced = results_mapped.coalesce(4)
results_coalesced.saveAsTextFile("/user/choi257/spark/out/q2")
