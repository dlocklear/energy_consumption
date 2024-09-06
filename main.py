from extract import extract_data
from transform import transform_data
from load import load_data

def run_pipeline():
    # Extract
    input_file_path = "data/Consumption_data.xlsx"
    raw_data = extract_data(input_file_path)
    
    # Transform
    transformed_data = transform_data(raw_data)

    # Load
    output_file_path = "data/Consumption_data_transformed.xlsx"
    load_data(transformed_data, output_file_path)

if __name__ == "__main__":
    run_pipeline()
