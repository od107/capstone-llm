import argparse
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

# Recursive function to flatten schema
def flatten_schema(schema, prefix=""):
    fields = []
    for field in schema.fields:
        name = prefix + field.name
        if isinstance(field.dataType, StructType):
            fields += flatten_schema(field.dataType, name + ".")
        else:
            fields.append(name)
    return fields

def clean(spark: SparkSession, environment: str, tag: str):
    spark = SparkSession.builder.getOrCreate()
    qdf = spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/" + tag + "/questions.json")
    adf = spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/" + tag + "/answers.json")
    adf_expl = adf.select("items", psf.explode(adf.items).alias("item")).select('item')
    qdf_expl = qdf.select("items", psf.explode(qdf.items).alias("item")).select('item')

    columns = flatten_schema(qdf_expl.schema)
    qdf_flat = qdf_expl.select(*columns)
    columns = flatten_schema(adf_expl.schema)
    adf_flat = adf_expl.select(*columns)

    questions = qdf_flat.select(
    'question_id',
    'title',
    'body',
    'accepted_answer_id'
    ).withColumnRenamed('body','question_body')
    answers = adf_flat.select('answer_id', 'body').withColumnRenamed('body','answer_body')
    
    combined = questions.join(answers, questions.accepted_answer_id == answers.answer_id, 'left')
    combined = combined.drop('accepted_answer_id')
    combined = combined.drop('answer_id')

    #very_slow to write these small files to S3
    output_s3 = 's3a://dataminded-academy-capstone-llm-data-us/jochen/" + tag + "/output'
    output_local= 'output'


    combined.repartition(combined.count()).write.mode('ignore').json(output_s3)


def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
