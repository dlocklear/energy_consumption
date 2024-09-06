def load_data(df, output_path):
    df.write.mode("overwrite").parquet(output_path)
