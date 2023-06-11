from pyspark.sql import SparkSession, DataFrame
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

class Read_Write:

    def write_parquet(self,df: DataFrame, path: str):
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.parquet(f'{path}/twitter_{current_time}')

