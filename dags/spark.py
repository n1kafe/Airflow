from datetime import datetime
from airflow.decorators import dag, task

default_args = {
    'owner': "airflow",
    'retries': 1
}

@dag(
    dag_id='spark_taskflow_example',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['sparl', 'airflow']
)
def spark_pipeline():
    
    @task
    def process_data_with_spark():
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .appName('AirflowTest') \
            .master("local[*]") \
            .getOrCreate()
        
        data = [("Alice", 25), ("Bob", 30), ("Charline", 35)]
        df = spark.createDataFrame(data, ["Name", "Age"])

        record_count = df.count()
        print(f"Rows processed: {record_count}")

        spark.stop()

        return record_count
    
    process_data_with_spark()


spark_pipeline()
