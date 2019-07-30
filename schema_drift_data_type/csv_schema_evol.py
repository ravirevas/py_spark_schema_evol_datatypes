from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("jdbc data sources") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars","/home/cloudera/Downloads/mysql-connector-java-5.1.4.jar") \
    .getOrCreate()

driver ="com.mysql.jdbc.Driver"
url ="jdbc:mysql://localhost:3306/retail_db"
#url1="jdbc:mysql://localhost/?user=root&password=cloudera"
tablename = "categories"
dbDataFrame = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/information_schema").option("driver",driver).option("dbtable","schemata").option("user","root").option("password","cloudera").load()
#a=dbDataFrame.select("SCHEMA_NAME").show()
a=dbDataFrame.rdd.map(tuple).collect()
b=dbDataFrame.select("SCHEMA_NAME").collect()
print(b)
print(type(b))

