
# Data Engineering Assignment

PySpark Assignments with PySpark DataFrame and PySpark SQL

# Dataset - 1

### Import Necessary Libraries

```python
import pyspark
from pyspark.sql import *
```

### Creating Spark Session
```python
spark = SparkSession.builder.appName("spark_dataset_1").getOrCreate()
spark
```

### Reading CSV File
```python
df_spark = spark.read.csv("dbfs:/FileStore/shared_uploads/akshay.boddhul@fissionlabs.com/train-4.csv", header = True, inferSchema = True)
df_spark.show(10)
```

### Tasks with PySpark DataFrame
#### Question #1: What are the Names of Female Survivors and how many are there?
```python
df_spark.filter( (df_spark.Sex == 'female') & (df_spark.Survived == 1) ).select("Name").show()
df_spark.filter( (df_spark.Sex == 'female') & (df_spark.Survived == 1) ).count()
```

#### Question #2: What is the average fare for each Passenger Class (Class)?

2.1 First use the group by clause on "Pclass" column and get that result into passenger_fare variable
```python
passenger_fare = df_spark.groupBy(df_spark["Pclass"])
```
2.2 Then apply the mean function on the "Fare" column
```python
passenger_fare.mean("Fare").show()
```

#### Question #3: Bucket each person to the age group of young (<30), middle (>=30 & <45), and old (>=45) groups and find who are the survivors. Find the survivor ratio and their gender distribution (Male vs Female). (You can create two separate queries if you want to or combine them)
3.1 Get the all passengers who's age is less than 30 as young_group
```python
young_grp = df_spark.filter( (df_spark.Age >= 1) & (df_spark.Age < 30) & (df_spark.Survived == 1)).sort("Age")
young_grp.show()
```

3.2 Get the all passengers who's age is In Between 30 and 45 as middle_group
```python
middle_grp = df_spark.filter( (df_spark.Age >= 30) & (df_spark.Age < 45) & (df_spark.Survived == 1)).sort("Age")
middle_grp.show()
```
3.3 Get the all passengers who's age is greater than 45 as old_group
```python
old_grp = df_spark.filter( (df_spark.Age >= 45) & (df_spark.Survived == 1)).sort("Age")
old_grp.show()
```

3.4 Finally, show the pie chart of all three groups
```python
import matplotlib.pyplot as plt
fig = plt.figure()

ax = fig.add_axes([0,0,1,1])

ax.axis('equal')

age_group = ["young","middle","old"]

passengers = [young_grp.select("PassengerId").count(),middle_grp.select("PassengerId").count(),old_grp.select("PassengerId").count()]

ax.pie(passengers, labels = age_group,autopct='%1.2f%%')

plt.show()
```

### Tasks with PySpark SQL
#### Create the Temporary view
```python
df_spark.createOrReplaceTempView("titanic_table")
```
#### Execute the basic SQL query to check the output
```python
spark.sql("SELECT * FROM titanic_table LIMIT 20").show()
```
#### Question #1: What are the Names of Female Survivors and how many are there?
```python
spark.sql("SELECT Name FROM titanic_table WHERE Sex = 'female' AND Survived = 1").show()
```

#### Question #2: What is the average fare for each Passenger Class (Class)?

```python
spark.sql("SELECT Pclass, AVG(Fare) AS Average_Fare FROM titanic_table GROUP BY Pclass").show()
```
#### Question #3: Bucket each person to the age group of young (<30), middle (>=30 & <45), and old (>=45) groups and find who are the survivors. Find the survivor ratio and their gender distribution (Male vs Female). (You can create two separate queries if you want to or combine them)
3.1 Get the all passengers who's age is less than 30 as young_group
```python
spark.sql("""
            WITH young_group AS (
                SELECT * FROM titanic_table
                WHERE Age < 30 AND Survived = 1
            )
            SELECT Sex, COUNT(1) AS Count FROM young_group
            GROUP BY Sex
          """).show()
```

3.2 Get the all passengers who's age is In Between 30 and 45 as middle_group
```python
spark.sql("""
            WITH middle_group AS (
                SELECT * FROM titanic_table
                WHERE Age BETWEEN 30 AND 45 AND Survived = 1
            )
            SELECT Sex, COUNT(1) AS Count FROM middle_group
            GROUP BY Sex
          """).show()
```
3.3 Get the all passengers who's age is greater than 45 as old_group
```python
spark.sql("""
            WITH old_group AS (
                SELECT * FROM titanic_table
                WHERE Age >= 45 AND Survived = 1
            )
            SELECT Sex, COUNT(1) AS Count FROM old_group
            GROUP BY Sex
          """).show()
```
