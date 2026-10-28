import os
import urllib.request
import ssl

from Tools.scripts.dutree import display
from pyspark.examples.src.main.python.sql.datasource import parquet_example

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
# Sajesh Commented this Top
# urls_and_paths = {
#     "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
#     "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
#     "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
# }
#
# # Create an unverified SSL context
# ssl_context = ssl._create_unverified_context()
#
# for url, path in urls_and_paths.items():
#     # Use the unverified context with urlopen
#     with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
#         data = response.read()
#         out_file.write(data)
# import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]
# Sajesh Commented this Bottom

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\sajes\.jdks\corretto-1.8.0_504'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DON'T TOUCH ABOVE CODE -- TYPE BELOW ####################################



# print("=====================dataframe csv read========================")
# df_csv =spark.read.format("csv").option("header","true").load("df.csv")
# df_csv.show();
#
# print("=====================dataframe json read========================")
# df_json = spark.read.format("json").load("file4.json")
# df_json.show()
#
# print("=====================dataframe Parquet read========================")
# df_parquet = spark.read.format("parquet").load("file5.parquet")
# df_parquet.show()
#
# print("=====================dataframe ORC read========================")
# df_orc = spark.read.format("ORC").load("data.orc")
# df_orc.show()
#
# print("=====================dataframe MySQL Database read -- not working========================")
#
# # df_database=(
# #
# #     spark
# #     .read
# #     .format("jdbc")
# #     .option("url", "jdbc:mysql://kanidb.c5oo4ky8226c.ap-south-2.rds.amazonaws.com:3306/zeyodb")
# #     .option("user", "root")
# #     .option("password", "Aditya908")
# #     .option("dbtable", "customer_tab")
# #     .option("driver", "com.mysql.jdbc.Driver")
# #     .load()
# #
# # )
# #
# # df_database.show()
#
# print("=====================Connecting to Local Oracle Database========================")
#
# sqldf = (
#     spark.read.format("jdbc")
#     .option("url", "jdbc:oracle:thin:@localhost:1521/xe")
#     .option("user", "user_db")
#     .option("password", "Admin")
#     # Using an inline query block instead of just the table name
#     .option("dbtable", "(SELECT * FROM USER_DB.DEPT) tmp")
#     .option("driver", "oracle.jdbc.driver.OracleDriver")
#     .load()
# )
#
# sqldf.show()
#
# print("=====================dataframe csv read========================")
# df_usdata =spark.read.format("csv").option("header","true").load("usdata.csv")
# df_usdata.show()
#
# print("=====================Spark SQL========================")
#
# df_usdata.createOrReplaceTempView("filtemptab")
# spark.sql("select * from filtemptab where state = 'LA'").show()
#
# print("=====================Spark DSL (Domain Specific Language)========================")
#
# df_usdata.filter("state = 'LA'").show()
#
print("=====================Data Preparation========================")

# data = [
#     ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
#     ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
#     ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
#     ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
#     ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
#     ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
# ]
#
# df = spark.createDataFrame(data).toDF(
#     "id", "tdate", "amt", "category", "product", "mode"
# )
# df.show()
#
# print("=====================Select columns from DataFrame========================")
# seldf = df.select("tdate","product")
# seldf.show()
#
# print("=====================Drop columns from DataFrame========================")
# dropdf = df.drop("tdate","product")
# dropdf.show()
#
# print("=====================Filter AND /OR========================")
# df_fil=df.filter("category ='Exercise'")
# df_fil.show()
#
# df_fil_and=df.filter("category ='Exercise' and mode ='cash'")
# df_fil_and.show()
#
# df_fil_or=df.filter("category ='Exercise' or mode ='cash'")
# df_fil_or.show()


#28th Session

# print("=====================Filter IN========================")
# df_infil = df.filter("category in ('Exercise', 'Gymnastics')")
# df_infil.show()
# print("=====================Filter LIKE========================")
# df_likefil =df.filter("product like 'Gymnastics%'")
# df_likefil.show()
# print("=====================Filter NOT LIKE========================")
# df_likefil =df.filter("product not like 'Gymnastics%'")
# df_likefil.show()
# print("=====================Filter IS NULL========================")
# df_isnull =df.filter("product is null")
# df_isnull.show()
# print("=====================Filter IS NOT NULL========================")
# df_isnotnull =df.filter("product is not null")
# df_isnotnull.show()
# print("=====================Filter not equal========================")
# df_nefill = df.filter(" category != 'Exercise' ")
# df_nefill.show()
#
# print("=====================Filter Examples========================")
# df_fil=df.filter("category ='Exercise'")
# df_fil.show()
#
# df_fil_and=df.filter("category ='Exercise' and mode ='cash'")
# df_fil_and.show()
#
# df_fil_or=df.filter("category ='Exercise' or mode ='cash'")
# df_fil_or.show()
#
# df_infil = df.filter("category in ('Exercise', 'Gymnastics')")
# df_infil.show()
#
# df_likefil =df.filter("product like 'Gymnastics%'")
# df_likefil.show()
#
# df_isnull =df.filter("product is null")
# df_isnull.show()
#
# df_isnotnull =df.filter("product is not null")
# df_isnotnull.show()
#
# df_nefill = df.filter(" category != 'Exercise' ")
# df_nefill.show()
#
# print("==============selectExpr - Select and expression=====================")
# df_sel_expr = df.selectExpr("cast(id as int) as id",
# "tdate",
# "amt + 100 as amt",
# "upper(category) as category",
# "concat(product,'~zeyo') as product",
# "mode",
# "split(tdate,'-')[2] as Year",
# "split(tdate,'-')[1] as Date",
# "split(tdate,'-')[0] as Month",
# #"to_char(to_date(tdate, 'MM-dd-yyyy'),'yyyy') as Year_to_char",
#                             # #Not working in Intellij
# "date_format(to_date(tdate, 'MM-dd-yyyy'),'yyyy') as Year_date_format",
# "year(to_date(tdate, 'MM-dd-yyyy')) as Year_year"
#                             )
# df_sel_expr.show()
#
#
# print("==============selectExpr - Select and expression=====================")
# df_sel_expr = df.selectExpr(
# "cast(id as int) as id",
# "tdate",
# "amt + 100 as amt",
# "upper(category) as category",
# "concat(product,'~zeyo') as product",
# "mode",
# "split(tdate,'-')[2] as Year",
# "split(tdate,'-')[1] as Date",
# "split(tdate,'-')[0] as Month",
# """ case
# when mode = 'cash' then 0
# when mode='credit' then 1
# else 2
# end
# as status """
# )
# df_sel_expr.show()
#
#
# #from pyspark.sql.functions import *
# from pyspark.sql.functions import expr
# df_withColumn = df.withColumn("category",expr("upper(category)"))
# df_withColumn.show()

#29th Session


# from pyspark.sql.functions import *
# df_withColumn = (df
#                  .withColumn("category",expr("upper(category)"))
#                  .withColumn("product",expr("product || '~ zeyo'"))
#                  .withColumn("amt" , expr("amt + 100"))
#                  .withColumn("tdate",expr("split(tdate,'-')[2]"))
#                  .withColumn("id", expr("cast(id as int)"))
#                  .withColumn("status", expr("case when mode = 'cash' then 0 else 1 end"))
#                  .withColumnRenamed("tdate", "Year")
#                  )
# df_withColumn.show()


# # Define the data records
# data = [
#     ("Alice Smith", "New York", 1250),
#     ("Bob Jones", "Los Angeles", 850),
#     ("Julia Roberts", "San Jose", 2100),
#     ("Alice Smith", "New York", 450),
#     ("Bob Jones", "Los Angeles", 3120),
#     ("Alice Smith", "New York", 675),
#     ("Bob Jones", "Los Angeles", 850),
#     ("Julia Roberts", "San Jose", 540),
#     ("Julia Roberts", "San Jose", 540)
# ]
#
# # Create the data frame
# df = spark.createDataFrame(data).toDF("Name","City","Amount")
#
# # Show the data frame contents
# df.show()
#
# from pyspark.sql.functions import *
#
#
# df_agg = df.groupBy("Name").agg(
#     sum("Amount").alias("Total_amt"),
#     count("Name").alias("Count"),
#     collect_list("Amount").alias("Amount_list"),
#     collect_set("Amount").alias("Amount_distinct")
#
# )
#
# df_agg.show()
#
# df_agg = df.groupBy("Name","City").agg(
#     sum("Amount").alias("Total_amt"),
#     count("Name").alias("Count"),
#     collect_list("Amount").alias("Amount_list"),
#     collect_set("Amount").alias("Amount_distinct")
#
# )
#
# df_agg.show()



#30th Session

# data4 = [    (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani") ]
# cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()
#
# data3 = [   (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop") ]
# prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()
#
# print("=======================Joins=======================")
#
# df_innerj = cust.join(prod,"id","inner")
# df_innerj.show()
#
# df_leftj = cust.join(prod,"id","left")
# df_leftj.show()
#
# df_rightj = cust.join(prod,"id","right")
# df_rightj.show()
#
# df_fullj = cust.join(prod,"id","full")
# df_fullj.show()
#
# df_leftantij = cust.join(prod,"id","left_anti")
# df_leftantij.show()
#
# df_leftantij = prod.join( cust,"id","left_anti")
# df_leftantij.show()
#
# df_fullj = cust.join(prod,"id","full").filter("name is null")
# df_fullj.show()
#
# df_crossj = cust.crossJoin(prod)
# df_crossj.show()
#
#
# Scenario 1
#
# # Sample data from the image
# data = [
#     ("A", "AA"),
#     ("B", "BB"),
#     ("C", "CC"),
#     ("AA", "AAA"),
#     ("BB", "BBB"),
#     ("CC", "CCC"),
# ]
#
# # Define columns
# columns = ["child", "parent"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, schema=columns)
# df.createOrReplaceTempView("tab")
# # Display result
# df.show()
#
# spark.sql("select b.child,a.child parent ,a.parent grand_parent from tab a join tab b on a.child = b.parent").show()
#
# df1=df
# df2 =df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")
# df_join = df1.join(df2, df1["child"] == df2["parent1"], "inner").selectExpr("child1 as child", "child as parent", "parent as grand_parent")
# df_join.show()
#
# ## Scenario 2
#
# # 1. Left Dataframe (Source)
# source_data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
# source_cols = ["id", "name"]
# df_src = spark.createDataFrame(source_data, schema=source_cols)
#
# # 2. Right Dataframe (Target)
# target_data = [(1, "A"), (2, "B"), (4, "X"), (5, "F")]
# target_cols = ["id1", "name1"]
# df_tgt = spark.createDataFrame(target_data, schema=target_cols)
#
# # Display both data frames
# print("Source DataFrame:")
# df_src.show()
#
# print("Target DataFrame:")
# df_tgt.show()
#
# from pyspark.sql.functions import *
#
# df_join= (df_src.join(df_tgt, df_src["id"] == df_tgt["id1"], "full"). \
#           filter("coalesce(name,'') <> coalesce(name1,'')"). \
#           withColumn("comment",expr("case when id1 is null then 'New in Source' when id1 is not null and name <> name1 then 'mismatch' when id is null then 'New in Target' else '' end")).selectExpr("coalesce(id,id1) as id", "comment"))
# df_join.show()
#
# from pyspark.sql.functions import *
#
# df_join = (
#     df_src.join(df_tgt, df_src["id"] == df_tgt["id1"], "full")
#     .selectExpr(
#         "coalesce(id,id1) as id",
#         "case when name = name1 then 'Matched'  when name <> name1 then 'Mismatch' when name is null then 'New in Source'  when name1 is null then 'New in Target'  end as comment",
#     )
#     .filter("comment <> 'Matched'")
# )
# df_join.show()
#

##31 Session

## Create DataFrame with dept and salary columns
##Dept wise 2nd Highest salary
data = [    ("HR", 50000),    ("IT", 75000),    ("Finance", 60000),    ("Marketing", 55000),
            ("HR", 52000),    ("IT", 80000),    ("Finance", 65000),    ("Marketing", 58000),
            ("HR", 48000),    ("IT", 72000) ]

df = spark.createDataFrame(data, ["dept", "salary"])
df.show()
df.createOrReplaceTempView("emp")

spark.sql("""select dept,salary from (select dept, salary,
dense_rank() over (partition by dept order by salary desc) rk from emp)
where rk =2 """).show()

from pyspark.sql.window import Window

df_win = Window.partitionBy("dept").orderBy(col("salary").desc())
df_denserank = df.withColumn("rk",dense_rank().over (df_win))
df_denserank.filter("rk = 2").drop("rk").show()


from pyspark.sql.functions import *
df_denserank = df.withColumn("rk",expr("dense_rank() over (partition by dept order by salary desc) rk"))
df_denserank.filter("rk = 2").drop("rk").show()

#========================No Delta table imports in here==============================================