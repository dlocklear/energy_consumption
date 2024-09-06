import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Assuming df is your PySpark DataFrame and has already been transformed
# First, convert your PySpark DataFrame to a Pandas DataFrame
def convert_to_pandas(spark_df):
    """
    Convert PySpark DataFrame to Pandas DataFrame.
    """
    return spark_df.toPandas()

# Function to plot total energy consumption trends by year and transport mode
def plot_energy_consumption_trends(df_pandas):
    """
    Plot total energy consumption trends over time for each transportation mode.
    """
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df_pandas, x='year', y='total_energy', hue='transport_mode', marker='o')
    plt.title('Energy Consumption Trends by Year and Transportation Mode')
    plt.xlabel('Year')
    plt.ylabel('Total Energy Consumption (kWh)')
    plt.legend(title='Transport Mode', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Function to plot energy efficiency comparison between transport modes
def plot_energy_efficiency_comparison(df_pandas):
    """
    Plot bar chart of average energy efficiency across transportation modes.
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df_pandas, x='transport_mode', y='overall_avg_efficiency', palette="Blues_d")
    plt.title('Energy Efficiency by Transportation Mode')
    plt.xlabel('Transport Mode')
    plt.ylabel('Average Energy Efficiency (kWh per km)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Function to plot total energy consumption by transport mode
def plot_total_energy_by_mode(df_pandas):
    """
    Plot bar chart of total energy consumption by transportation mode.
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df_pandas, x='transport_mode', y='total_energy_by_mode', palette="Reds_d")
    plt.title('Total Energy Consumption by Transportation Mode')
    plt.xlabel('Transport Mode')
    plt.ylabel('Total Energy Consumption (kWh)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Function to create all plots for leadership
def create_visualizations(df_spark):
    """
    This function converts the PySpark DataFrame to Pandas and then creates various plots.
    """
    # Convert the Spark DataFrame to Pandas
    df_pandas = convert_to_pandas(df_spark)

    # Call each plot function
    plot_energy_consumption_trends(df_pandas)
    plot_energy_efficiency_comparison(df_pandas)
    plot_total_energy_by_mode(df_pandas)

# This is where you'd run the visualization script in your pipeline
if __name__ == "__main__":
    # Assuming `df_comparison` is the final DataFrame from your transform.py, you can pass it here
    from transform import transform_data
    from extract import extract_data

    # Extract and transform data (as done in main.py)
    input_file_path = "hdfs://path/to/your/data.csv"
    raw_data = extract_data(input_file_path)
    transformed_data = transform_data(raw_data)

    # Create visualizations for leadership
    create_visualizations(transformed_data)
