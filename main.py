'''
# pip install mysql-connector-python sqlalchemy pandas numbpy mysql-connector-python setuptools pandas pyarrow
# pip install setuptools==65.6.3
# pip install --upgrade setuptools


select * from org.department_count_fact;

select * from org.department_salary_fact;

select * from org.employee_dimension;

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, avg, sum, round, count, col, to_date, udf, concat_ws, coalesce, lit
from pyspark.sql.types import StringType


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# MySQL connection configuration
mysql_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "rootroot",
    "database": "org"
}

# JDBC URL
jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"

# Path to the MySQL JDBC driver
jdbc_driver_path = "jars/mysql-connector-j-9.0.0.jar"

# Create a Spark session
spark = SparkSession.builder.appName("EmployeeData") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()


# UDF to convert names to Camel Case
def camel_case(name):
    if name:
        return name.title()
    return None


# Register the UDF
camel_case_udf = udf(camel_case, StringType())


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Read the CSV file into DataFrame
emp_df = spark.read.csv("src/emp.csv", header=True, inferSchema=True)
dept_df = spark.read.csv("src/dept.csv", header=True, inferSchema=True)

# Show the DataFrame
emp_df.show()
dept_df.show()


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# FACT TABLE 1 - Group by 'department' and calculate the max, min, avg, and sum of 'salary'
dept_sal_fact = emp_df.groupBy("deptno").agg(
    max("sal").alias("max_salary"),
    min("sal").alias("min_salary"),
    round(avg("sal"), 2).alias("avg_salary"),
    sum("sal").alias("sum_salary")
)

# Show the results
dept_sal_fact.show()


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# FACT TABLE 2 - EMPLOYEES IN EACH DEPARTMENT
dept_cnt_fact = emp_df.groupBy("deptno").agg(
    count("*").alias("total_employees"))

dept_cnt_fact.show()


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# DIMENSION TABLE 1 - EMPLOYEE DIMENSION TABLE
emp_order = ["emp_no", "full_name", "email", "phone_number", "hire_date", "job", "totalsal", "mgrno", "deptno"]

emp_dim = emp_df.withColumn("totalsal", (col("sal") + coalesce(col("comm"), lit(0))).cast("int")) \
    .withColumn("full_name", concat_ws(" ", camel_case_udf(col("first_name")), camel_case_udf(col("last_name")))) \
    .withColumn("hire_date", to_date(col("hire_date"), "dd-MMM-yyyy")) \
    .select(*emp_order)

emp_dim.show()



# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Function to write DataFrame to MySQL
def write_to_mysql(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", mysql_config["user"]) \
        .option("password", mysql_config["password"]) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()


# Write DataFrames to MySQL
write_to_mysql(emp_dim, 'employee_dimension')
write_to_mysql(dept_sal_fact, 'department_salary_fact')
write_to_mysql(dept_cnt_fact, 'department_count_fact')

# Stop the Spark session
spark.stop()
