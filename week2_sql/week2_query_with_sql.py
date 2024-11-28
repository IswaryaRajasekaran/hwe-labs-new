from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

### Setup: Create a SparkSession
spark = SparkSession.builder \
    .appName("Reviews Processing") \
    .master("local") \
    .getOrCreate()
   
# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True) 
#reviews.printSchema()
#reviews.show()
# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("ReviewsView")

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
reviews_with_timestamp = reviews.withColumn("review_timestamp",current_timestamp())
reviews_with_timestamp.show()
# Question 4: How many records are in the reviews dataframe? 
records_count = spark.sql("SELECT COUNT(*) FROM ReviewsView")
#records_count.show()
# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
reviews.show(n=5, truncate=False)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
Product_Category = spark.sql("SELECT DISTINCT product_category FROM ReviewsView LIMIT 50")
Product_Category.show()
# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
Most_helpful_review = spark.sql("SELECT product_title, review_headline, review_body, helpful_votes FROM ReviewsView ORDER BY helpful_votes DESC LIMIT 1")
Most_helpful_review.show(truncate=False)
# Question 8: How many reviews exist in the dataframe with a 5 star rating?
five_star_rating = spark.sql("SELECT COUNT(review_headline) FROM ReviewsView WHERE star_rating = 5")
five_star_rating.show()
# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
Reviews_with_integer_field = spark.sql("SELECT cast(star_rating as int), cast(helpful_votes as int), cast(total_votes as int) FROM ReviewsView")
Reviews_with_integer_field.printSchema()
Reviews_with_integer_field.show()
# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.
Total_Purchase_count = spark.sql("select purchase_date, count(purchase_date) as purchase_count from ReviewsView GROUP BY purchase_date ORDER BY purchase_count DESC LIMIT 1")
Total_Purchase_count.show()
##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.

reviews_with_timestamp.write.mode("overwrite").json("reviews_with_timestamp_json_dir")

### Teardown
# Stop the SparkSession
spark.stop()