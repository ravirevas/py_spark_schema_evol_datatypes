from pyspark.sql import SparkSession
from support import *
import subprocess

spark = SparkSession.builder \
    .master("local") \
    .appName("jdbc data sources") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.warehouse.dir", '/user/hive/warehouse/') \
    .config("hive.metastore.uris", 'thrift://127.0.0.1:9083') \
    .config("spark.sql.hive.convertMetastoreParquet","false") \
    .enableHiveSupport() \
    .getOrCreate()

#spark.sql("set -v").show(1000,False)
#spark.sql("ALTER TABLE sparktable3 REPLACE COLUMNS (id  double) CASCADE")
spark.sql("describe departments").show()
df_new_file = spark.read.format("csv").option("header", "true").option("mergeSchema", "true").option("inferSchema",
                                                                                                     "true").load(
    "{}".format(source_file))

df_new_file.createOrReplaceTempView("parquetFiles")
rdd_new_file = spark.sql("describe parquetFiles").rdd.map(lambda x: (x[0], x[1]))
# df.write.mode('append').format('parquet').saveAsTable('sparKtable3')
rdd_old_file = spark.sql("describe sparktable3").rdd.map(lambda x: (x[0], x[1]))
print("\n################NEW FILE SCHEMA################\n")
print(rdd_new_file.collect())

print("\n################OLD FILE SCHEMA################\n")
print(rdd_old_file.collect())
rdd_old_new_schema = rdd_old_file.fullOuterJoin(rdd_new_file)

print("\n##################FULL COMBINED METADATA################\n")
print(rdd_old_new_schema.collect())


new_column_rdd = rdd_old_new_schema.filter(lambda x: x[1][0] is None)
missing_column_rdd = rdd_old_new_schema.filter(lambda y: y[1][1] is None)

print("\n################NEW_COLUMNS(extra) FROM SOURCE##################\n")
print(new_column_rdd.collect())

print("\n###########NO CHANGES IN DATATYPE##########\n")
no_change_rdd = rdd_old_new_schema.filter(lambda v: v[1][1] is not None and v[1][1] == v[1][0] and v[1][0] is not None)
print(no_change_rdd.collect())

print("\n########MISSING COLUMNS (extra COLUMNS FROM hive##############\n")
print(missing_column_rdd.collect())

print("\n############DATA_TYPE_CHANGE DETECTED###########\n")
data_type_changed_rdd = rdd_old_new_schema.filter(
    lambda z: z[1][1] is not None and z[1][1] != z[1][0] and z[1][0] is not None)
print(data_type_changed_rdd.collect())

#print(rdd_old_new_schema.count())
#print(no_change_rdd.count())

if (rdd_old_new_schema.count() == no_change_rdd.count()):
    print("No Schema Change Detected....good record")


elif (rdd_old_new_schema.count() == (new_column_rdd.count() + missing_column_rdd.count())):
    print("NO Common Column....bad record")
################################################metadata###comparison####ends###here##########################################################################################


if(data_type_changed_rdd.count() != 0) :
  alter_data_type=data_type_changed_rdd.collect()


  alter_data_type=[('id', ('char(20)', 'char(30)')), ('product_id', ('varchar(20)', 'int'))]

  print(alter_data_type)
  for i in alter_data_type:
      print("\ntrying to convert {} {}\n".format(i[1][0], i[1][1]))
      alter_status = data_type_convt_check(i[1])
      if (alter_status is '0'):
       col=i[0]+" "+i[0]+" "+i[1][1]
       hive_alter_cmd="hive -e 'ALTER TABLE {} change ".format(hive_table_name) +col+"'"
       print(hive_alter_cmd)
       #subprocess.call(stmt, shell=True)

      elif(alter_status is '1'):
          print("Unvalid Alter....your dict doest not support this kind of alter")

      elif(alter_status is '2'):
          print("Unvalid...key is not prsent new datatype ")

      elif(alter_status is '3'):
          print("Unvalid.....you are decrease the size of old datatype cannot be suuported")

      else:
          print("alternate return code...something went wrong")