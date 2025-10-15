from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
from datasketch import HyperLogLog
from elasticsearch import Elasticsearch
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
import os

# GPT Setup
os.environ["OPENAI_API_KEY"] = "OPEN API KEY"
llm = ChatOpenAI(temperature=0)
prompt = PromptTemplate.from_template("Classify the sentiment of this restaurant review: '{review}'.")

# Elasticsearch Setup
es = Elasticsearch("http://localhost:9200")

def analyze_sentiment(review):
    try:
        from langchain.chains import LLMChain
        chain = LLMChain(llm=llm, prompt=prompt)
        return chain.run(review=review)
    except:
        return "neutral"

def save_to_elasticsearch(row):
    doc = row.asDict()
    doc["sentiment"] = analyze_sentiment(doc["review"])
    es.index(index="restaurant_reviews", document=doc)

# Spark Setup
spark = SparkSession.builder.appName("KafkaSparkSentiment").getOrCreate()

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("restaurant_name", StringType()) \
    .add("customer_name", StringType()) \
    .add("review", StringType()) \
    .add("timestamp", StringType()) \
    .add("ip", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "restaurant_reviews") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

query = json_df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.toPandas().apply(save_to_elasticsearch, axis=1)) \
    .start()

query.awaitTermination()

