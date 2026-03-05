"""
Weather Data Transformation Script
Cleans and structures raw weather data from JSON extraction files
Automatically processes ALL timestamped JSON files in data/ directory
"""

import pandas as pd
from datetime import datetime, timezone
import json
import glob
import os


def load_raw_data(filepath=None):
    """
    Load raw weather data from JSON file(s)
    If filepath is None, loads all weather_data_*.json files from data directory
    
    Args:
        filepath: Path to a specific raw data file, or None to load all files
    Returns:
        List of weather data dictionaries
    """
    all_data = []
    
    # If no filepath specified, find all weather_data JSON files
    if filepath is None:
        json_files = glob.glob('data/weather_data_*.json')
        
        if not json_files:
            print(f"Error: No JSON files found in data/ directory")
            print(f"Looking for: data/weather_data_*.json")
            print(f"Please run extract_weather.py first")
            return []
        
        print(f"  Found {len(json_files)} JSON file(s)")
        json_files = sorted(json_files)  # Sort by filename (chronological)
        
        # Load all files
        for file in json_files:
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                    
                # Handle both list and single dict formats
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
                    
                print(f"    Loaded {len(data) if isinstance(data, list) else 1} records from {os.path.basename(file)}")
                
            except FileNotFoundError:
                print(f"  File not found: {file}")
            except json.JSONDecodeError:
                print(f"  Invalid JSON in {file}")
        
        print(f"\n  Total records loaded from all files: {len(all_data)}")
        return all_data
    
    # If specific filepath provided, load just that file
    else:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Handle both list and single dict formats
            if isinstance(data, list):
                print(f"Loaded {len(data)} records from {filepath}")
                return data
            else:
                print(f"Loaded 1 record from {filepath}")
                return [data]
                
        except FileNotFoundError:
            print(f"Error: File {filepath} not found")
            return []
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in {filepath}")
            return []


def transform_weather_data(raw_data):
    """
    Transform raw weather data into a clean, structured format
    
    Args:
        raw_data: List of raw weather dictionaries
    Returns:
        pandas DataFrame with cleaned data
    """
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(raw_data)
    
    print(f"\n--- Starting Transformation ---")
    print(f"Initial records: {len(df)}")
    
    # 1. Handle missing values
    print("\n1. Checking for missing values...")
    missing_before = df.isnull().sum().sum()
    
    # Fill missing numeric values with appropriate defaults or drop
    numeric_cols = ['temperature', 'temperature_min', 'temperature_max', 
                    'feels_like', 'humidity', 'pressure', 'wind_speed', 
                    'wind_direction', 'cloudiness']
    
    for col in numeric_cols:
        if col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                print(f"   - {col}: {missing_count} missing values")
                # Fill with median
                df[col].fillna(df[col].median(), inplace=True)
    
    # Fill missing text values
    text_cols = ['description', 'weather_condition']
    for col in text_cols:
        if col in df.columns:
            df[col].fillna('Unknown', inplace=True)
    
    print(f"   Missing values handled: {missing_before} → {df.isnull().sum().sum()}")
    
    
    # 2. Data type conversions
    print("\n2. Converting data types...")
    
    # Convert timestamp columns to datetime objects
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    if 'data_collected_at' in df.columns:
        df['data_collected_at'] = pd.to_datetime(df['data_collected_at'])
    
    # Ensure numeric columns are proper numeric types
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print("     Data types converted")
    
    
    # 3. Add derived columns
    print("\n3. Creating derived columns...")
    
    # Temperature range
    if 'temperature_max' in df.columns and 'temperature_min' in df.columns:
        df['temperature_range'] = df['temperature_max'] - df['temperature_min']
    
    # Extract date components for easier querying
    if 'timestamp' in df.columns:
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['month'] = df['timestamp'].dt.month
        df['year'] = df['timestamp'].dt.year
    
    # Categorize weather conditions
    if 'temperature' in df.columns:
        df['temp_category'] = pd.cut(df['temperature'], 
                                      bins=[-float('inf'), 0, 10, 20, 30, float('inf')],
                                      labels=['Freezing', 'Cold', 'Mild', 'Warm', 'Hot'])
    
    # Wind speed categories (Beaufort scale simplified)
    if 'wind_speed' in df.columns:
        df['wind_category'] = pd.cut(df['wind_speed'],
                                      bins=[0, 5, 11, 20, float('inf')],
                                      labels=['Calm', 'Moderate', 'Strong', 'Very Strong'])
    
    derived_cols = ['temperature_range', 'date', 'hour', 'day_of_week', 
                   'month', 'year', 'temp_category', 'wind_category']
    existing_derived = [col for col in derived_cols if col in df.columns]
    print(f"   Added {len(existing_derived)} derived columns")
    
    
    # 4. Data validation and quality checks
    print("\n4. Running data quality checks...")
    
    initial_count = len(df)
    
    # Remove duplicates (same city, same timestamp)
    if 'city_name' in df.columns and 'timestamp' in df.columns:
        before_dedup = len(df)
        df.drop_duplicates(subset=['city_name', 'timestamp'], keep='first', inplace=True)
        duplicates_removed = before_dedup - len(df)
        if duplicates_removed > 0:
            print(f"   - Removed {duplicates_removed} duplicate records")
    
    # Remove invalid temperature readings (beyond physically possible)
    if 'temperature' in df.columns:
        invalid_temp = df[(df['temperature'] < -90) | (df['temperature'] > 60)]
        if len(invalid_temp) > 0:
            print(f"   - Found {len(invalid_temp)} invalid temperature readings")
            df = df[(df['temperature'] >= -90) & (df['temperature'] <= 60)]
    
    # Validate humidity (should be 0-100)
    if 'humidity' in df.columns:
        invalid_humidity = df[(df['humidity'] < 0) | (df['humidity'] > 100)]
        if len(invalid_humidity) > 0:
            print(f"   - Found {len(invalid_humidity)} invalid humidity readings")
            df = df[(df['humidity'] >= 0) & (df['humidity'] <= 100)]
    
    print(f"     Quality checks complete: {initial_count} → {len(df)} records")
    
    
    # 5. Standardize text fields
    print("\n5. Standardizing text fields...")
    
    # Capitalize city names consistently
    if 'city_name' in df.columns:
        df['city_name'] = df['city_name'].str.title()
    
    # Uppercase country codes
    if 'country' in df.columns:
        df['country'] = df['country'].str.upper()
    
    # Lowercase descriptions for consistency
    if 'description' in df.columns:
        df['description'] = df['description'].str.lower().str.strip()
    
    print("   Text fields standardized")
    
    
    # 6. Sort data
    if 'timestamp' in df.columns and 'city_name' in df.columns:
        df.sort_values(['city_name', 'timestamp'], inplace=True)
    
    print(f"\n--- Transformation Complete ---")
    print(f"Final records: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Cities: {df['city_name'].nunique() if 'city_name' in df.columns else 'N/A'}")
    
    return df


def save_transformed_data(df, output_path):
    """
    Save transformed data to CSV
    
    Args:
        df: Transformed DataFrame
        output_path: Path to save the CSV file
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df.to_csv(output_path, index=False)
        print(f"\n  Transformed data saved to {output_path}")
        
        # Print summary statistics
        print(f"\n--- Data Summary ---")
        print(f"Total records: {len(df)}")
        if 'timestamp' in df.columns:
            print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        if 'city_name' in df.columns:
            print(f"Cities: {df['city_name'].nunique()}")
            print(f"\nCity breakdown:")
            print(df['city_name'].value_counts().to_string())
        
    except Exception as e:
        print(f"Error saving file: {e}")


def validate_schema(df):
    """
    Validate that the DataFrame has all required columns
    
    Args:
        df: DataFrame to validate
    Returns:
        Boolean indicating if schema is valid
    """
    required_columns = [
        'city_name', 'country', 'temperature', 'humidity', 
        'description', 'timestamp'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Missing required columns: {missing_columns}")
        return False
    
    print("Schema validation passed")
    return True


def main():
    """
    Main transformation pipeline
    """
    print("\n" + "="*60)
    print("WEATHER DATA TRANSFORMATION")
    print("="*60)
    
    # Load raw data from ALL JSON files in data/ directory
    # To process a specific file instead, uncomment and specify:
    # raw_data = load_raw_data('data/weather_data_20250117_143022.json')
    raw_data = load_raw_data()
    
    if not raw_data:
        print("\nNo data to transform. Exiting.")
        print("Please run extract_weather.py first")
        return
    
    # Transform data
    transformed_df = transform_weather_data(raw_data)
    
    # Validate schema
    if not validate_schema(transformed_df):
        print("\nSchema validation failed. Please check your data.")
        return
    
    # Save transformed data
    output_path = 'data/transformed_weather_data.csv'
    save_transformed_data(transformed_df, output_path)
    
    # Optional: Print sample of transformed data
    print("\n--- Sample of Transformed Data ---")
    print(transformed_df.head(10))
    
    print("\n--- Next Steps ---")
    print("Run: python load_weather.py")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()