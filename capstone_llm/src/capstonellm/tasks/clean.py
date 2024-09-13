import argparse
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    spark = SparkSession.builder.getOrCreate()
    qdf = spark.read.json(f"s3a://{llm_bucket}/input/{tag}/questions.json")
    adf = spark.read.json(f"s3a://{llm_bucket}/input/{tag}/answers.json")
    adf_expl = adf.select("items", psf.explode(adf.items).alias("item")).select('item')
    qdf_expl = qdf.select("items", psf.explode(qdf.items).alias("item")).select('item')

    qdf_flat = qdf_expl.select("item.*")
    adf_flat = adf_expl.select("item.*")

    questions = qdf_flat.select(
    'question_id',
    'title',
    'body',
    'accepted_answer_id',
    'link'
    ).withColumnRenamed('body','question')
    answers = adf_flat.select('answer_id', 'body').withColumnRenamed('body','answer')
    
    combined = questions.join(answers, questions.accepted_answer_id == answers.answer_id, 'left')
    combined = combined.drop('accepted_answer_id')
    combined = combined.drop('answer_id')

    #very_slow to write these small files to S3
    output_local= 'output'

    combined.repartition(combined.count()).write.mode('ignore').json(f"s3a://{llm_bucket}/cleaned/{tag}/")


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
