from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, year, quarter, when

# Step 0: Add Distance Traveled Calculation
def calculate_distance_travelled(df):
    """
    Calculate distance travelled based on the mode of transportation.
    These are hypothetical average distances in kilometers.
    """
    df = df.withColumn('distance_travelled', 
        when(col('transport_mode') == 'Car', 15_000)   # Assume 15,000 km per year for cars
        .when(col('transport_mode') == 'Bus', 40_000)   # Assume 40,000 km per year for buses
        .when(col('transport_mode') == 'Train', 25_000) # Assume 25,000 km per year for trains
        .when(col('transport_mode') == 'Bike', 1_000)   # Assume 1,000 km per year for bikes
        .otherwise(0)  # For any other unknown mode, assume 0 km
    )
    return df

# Step 1: Energy Efficiency Calculation
def calculate_energy_efficiency(df):
    """
    Calculate energy efficiency as energy consumption per kilometer traveled.
    """
    df = df.withColumn('energy_efficiency', col('energy_consumption') / col('distance_travelled'))
    return df

# Step 2: Aggregate Data by Yearly Intervals
def aggregate_data_by_year(df):
    """
    Aggregate the data by year.
    """
    df = df.withColumn('year', year(col('date')))
    df_grouped = df.groupBy('year', 'transport_mode').agg(
        sum('energy_consumption').alias('total_energy'),
        sum('distance_travelled').alias('total_distance'),
        sum('energy_efficiency').alias('avg_energy_efficiency')
    )
    return df_grouped

# Step 3: Compare Energy Consumption by Transportation Modes
def compare_energy_consumption(df):
    """
    Compare energy consumption across transportation modes by year.
    """
    # Assuming we have already aggregated by year in the previous steps
    df_comparison = df.groupBy('transport_mode').agg(
        sum('total_energy').alias('total_energy_by_mode'),
        sum('total_distance').alias('total_distance_by_mode'),
        sum('avg_energy_efficiency').alias('overall_avg_efficiency')
    ).orderBy(col('total_energy_by_mode').desc())

    return df_comparison

# Main Transformation Function
def transform_data(df):
    """
    Apply the full transformation pipeline on the DataFrame.
    """

    # Ensure date format is consistent (assuming your dataset has a 'date' column)
    df = df.withColumn('date', to_date(col('date'), 'MM/dd/yyyy'))

    # Step 0: Calculate distance traveled
    df = calculate_distance_travelled(df)

    # Step 1: Calculate energy efficiency
    df = calculate_energy_efficiency(df)

    # Step 2: Aggregate by year
    df_yearly = aggregate_data_by_year(df)

    # Step 3: Compare energy consumption by transportation mode
    df_comparison = compare_energy_consumption(df_yearly)

    return df_comparison  # Return the final transformed DataFrame
