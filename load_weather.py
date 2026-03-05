"""
Weather Data Loading Script
Loads transformed weather data from CSV into SQLite database
"""

import pandas as pd
import sqlite3
from datetime import datetime
import os


# Database configuration
DB_PATH = 'data/weather.db'
CSV_PATH = 'data/transformed_weather_data.csv'


def create_database_connection():
    """
    Create a connection to the SQLite database
    
    Returns:
        Connection object
    """
    try:
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        conn = sqlite3.connect(DB_PATH)
        print(f"Connected to database: {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None


def create_weather_table(conn):
    """
    Create the weather_data table if it doesn't exist
    
    Args:
        conn: Database connection object
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_name TEXT NOT NULL,
        country TEXT,
        latitude REAL,
        longitude REAL,
        temperature REAL,
        temperature_min REAL,
        temperature_max REAL,
        temperature_range REAL,
        feels_like REAL,
        humidity INTEGER,
        pressure REAL,
        description TEXT,
        weather_condition TEXT,
        wind_speed REAL,
        wind_direction REAL,
        wind_category TEXT,
        cloudiness INTEGER,
        timestamp TEXT NOT NULL,
        data_collected_at TEXT,
        date TEXT,
        hour INTEGER,
        day_of_week INTEGER,
        month INTEGER,
        year INTEGER,
        temp_category TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(city_name, timestamp)
    );
    
    CREATE INDEX IF NOT EXISTS idx_city_timestamp ON weather_data(city_name, timestamp);
    CREATE INDEX IF NOT EXISTS idx_date ON weather_data(date);
    CREATE INDEX IF NOT EXISTS idx_city ON weather_data(city_name);
    """
    
    try:
        cursor = conn.cursor()
        cursor.executescript(create_table_query)
        conn.commit()
        cursor.close()
        print("Table 'weather_data' ready")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")
        conn.rollback()


def load_csv_data(filepath):
    """
    Load transformed weather data from CSV file
    
    Args:
        filepath: Path to the CSV file
    Returns:
        pandas DataFrame or None if failed
    """
    if not os.path.exists(filepath):
        print(f"Error: File {filepath} not found")
        print(f"Please run transform_weather.py first")
        return None
    
    try:
        df = pd.read_csv(filepath)
        print(f"Loaded {len(df)} records from {filepath}")
        
        # SQLite works best with string timestamps, so convert to ISO format
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'data_collected_at' in df.columns:
            df['data_collected_at'] = pd.to_datetime(df['data_collected_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File {filepath} not found")
        print(f"Please run transform_weather.py first")
        return None
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None


def insert_data_batch(conn, df):
    """
    Insert data into database using pandas to_sql for better performance
    
    Args:
        conn: Database connection object
        df: DataFrame with weather data
    Returns:
        Number of rows inserted
    """
    # Prepare column names
    columns = [
        'city_name', 'country', 'latitude', 'longitude',
        'temperature', 'temperature_min', 'temperature_max', 'temperature_range',
        'feels_like', 'humidity', 'pressure', 'description', 'weather_condition',
        'wind_speed', 'wind_direction', 'wind_category', 'cloudiness',
        'timestamp', 'data_collected_at', 'date', 'hour', 'day_of_week',
        'month', 'year', 'temp_category'
    ]
    
    # Filter to only include columns that exist in DataFrame
    available_columns = [col for col in columns if col in df.columns]
    df_to_insert = df[available_columns].copy()
    
    try:
        # Get initial count
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM weather_data")
        initial_count = cursor.fetchone()[0]
        
        # Insert data using temp table approach for proper upsert in SQLite
        # Create temporary table
        df_to_insert.to_sql('temp_weather', conn, if_exists='replace', index=False)
        
        # Insert or replace from temp table
        insert_query = f"""
        INSERT OR REPLACE INTO weather_data ({', '.join(available_columns)})
        SELECT {', '.join(available_columns)}
        FROM temp_weather
        """
        
        cursor.execute(insert_query)
        
        # Drop temp table
        cursor.execute("DROP TABLE temp_weather")
        
        conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM weather_data")
        final_count = cursor.fetchone()[0]
        rows_affected = final_count - initial_count
        
        cursor.close()
        
        print(f"Successfully inserted/updated records")
        print(f"Initial count: {initial_count}")
        print(f"Final count: {final_count}")
        print(f"New records: {rows_affected}")
        
        return len(df_to_insert)
        
    except sqlite3.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        return 0


def verify_data(conn, city_name=None):
    """
    Verify data was loaded correctly
    
    Args:
        conn: Database connection object
        city_name: Optional city name to filter by
    """
    cursor = conn.cursor()
    
    try:
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM weather_data")
        total_count = cursor.fetchone()[0]
        print(f"\n--- Database Verification ---")
        print(f"Total records in database: {total_count}")
        
        # Get date range
        cursor.execute("""
            SELECT MIN(timestamp), MAX(timestamp) 
            FROM weather_data
        """)
        min_date, max_date = cursor.fetchone()
        print(f"Date range: {min_date} to {max_date}")
        
        # Get city breakdown
        cursor.execute("""
            SELECT city_name, COUNT(*) as count 
            FROM weather_data 
            GROUP BY city_name 
            ORDER BY count DESC
        """)
        print(f"\nRecords per city:")
        for city, count in cursor.fetchall():
            print(f"  {city}: {count}")
        
        # Get latest record for each city
        if city_name:
            cursor.execute("""
                SELECT city_name, temperature, humidity, description, timestamp
                FROM weather_data
                WHERE city_name = ?
                ORDER BY timestamp DESC
                LIMIT 5
            """, (city_name,))
        else:
            cursor.execute("""
                SELECT city_name, temperature, humidity, description, timestamp
                FROM weather_data
                WHERE id IN (
                    SELECT MAX(id)
                    FROM weather_data
                    GROUP BY city_name
                )
                ORDER BY city_name
                LIMIT 10
            """)
        
        print(f"\nLatest records:")
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]}°C, {row[2]}% humidity, {row[3]} ({row[4]})")
        
        cursor.close()
        
    except sqlite3.Error as e:
        print(f"Error verifying data: {e}")
        cursor.close()


def get_database_stats(conn):
    """
    Get statistics about the database
    
    Args:
        conn: Database connection object
    """
    print("\n--- Database Statistics ---")
    
    cursor = conn.cursor()
    
    # Database file size
    if os.path.exists(DB_PATH):
        db_size = os.path.getsize(DB_PATH) / 1024  # KB
        print(f"Database file size: {db_size:.2f} KB")
    
    # Table info
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    total_rows = cursor.fetchone()[0]
    print(f"Total records: {total_rows}")
    
    # Unique cities
    cursor.execute("SELECT COUNT(DISTINCT city_name) FROM weather_data")
    unique_cities = cursor.fetchone()[0]
    print(f"Unique cities: {unique_cities}")
    
    # Average temperature
    cursor.execute("SELECT AVG(temperature) FROM weather_data")
    avg_temp = cursor.fetchone()[0]
    if avg_temp:
        print(f"Average temperature: {avg_temp:.2f}°C")
    
    # Temperature range
    cursor.execute("SELECT MIN(temperature), MAX(temperature) FROM weather_data")
    min_temp, max_temp = cursor.fetchone()
    if min_temp and max_temp:
        print(f"Temperature range: {min_temp:.2f}°C to {max_temp:.2f}°C")
    
    cursor.close()


def main():
    """
    Main loading pipeline
    """
    print("\n" + "="*60)
    print("WEATHER DATA LOADING (SQLite)")
    print("="*60 + "\n")
    
    # Step 1: Load CSV file
    df = load_csv_data(CSV_PATH)
    
    if df is None or len(df) == 0:
        print("\nNo data to load. Exiting.")
        print("Please run transform_weather.py first")
        return
    
    print(f"Records to load: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    
    # Step 2: Connect to database
    conn = create_database_connection()
    
    if conn is None:
        print("Failed to connect to database. Exiting.")
        return
    
    # Step 3: Create table if it doesn't exist
    create_weather_table(conn)
    
    # Step 4: Insert data
    rows_inserted = insert_data_batch(conn, df)
    
    # Step 5: Verify the data
    verify_data(conn)
    
    # Step 6: Get database statistics
    get_database_stats(conn)
    
    # Close connection
    conn.close()
    print("\nDatabase connection closed")
    print(f"Database saved at: {DB_PATH}")
    
    print("\n--- Next Steps ---")
    print("Run: python visualize_weather.py")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()