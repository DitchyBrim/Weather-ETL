"""
Weather Data Visualization Script
Creates interactive visualizations and dashboard from SQLite database
"""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os

# Set style for better-looking plots
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


# Database configuration
DB_PATH = 'data/weather.db'
OUTPUT_DIR = 'visualizations'


def connect_to_database():
    """
    Connect to SQLite database and return connection
    
    Returns:
        Connection object or None
    """
    try:
        if not os.path.exists(DB_PATH):
            print(f"Database not found at {DB_PATH}")
            print("Please run load_weather.py first")
            return None
        
        conn = sqlite3.connect(DB_PATH)
        print(f"Connected to database: {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None


def load_weather_data(conn):
    """
    Load all weather data from database
    
    Args:
        conn: Database connection object
    Returns:
        pandas DataFrame
    """
    query = """
    SELECT * FROM weather_data
    ORDER BY timestamp DESC
    """
    
    try:
        df = pd.read_sql(query, conn)
        
        # Convert timestamp columns to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = pd.to_datetime(df['date'])
        
        print(f"Loaded {len(df)} records")
        print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"Cities: {df['city_name'].nunique()}")
        
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def create_output_directory():
    """
    Create directory for saving visualizations
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory ready: {OUTPUT_DIR}/")


def plot_temperature_comparison(df):
    """
    Create bar chart comparing average temperatures across cities
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating temperature comparison chart...")
    
    # Calculate average temperature per city
    avg_temp = df.groupby('city_name')['temperature'].mean().sort_values(ascending=False)
    
    # Create figure
    plt.figure(figsize=(12, 6))
    
    # Create bar plot
    bars = plt.bar(range(len(avg_temp)), avg_temp.values, color='steelblue', edgecolor='navy', alpha=0.7)
    
    # Customize
    plt.xlabel('City', fontsize=12, fontweight='bold')
    plt.ylabel('Average Temperature (°C)', fontsize=12, fontweight='bold')
    plt.title('Average Temperature by City', fontsize=14, fontweight='bold', pad=20)
    plt.xticks(range(len(avg_temp)), avg_temp.index, rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}°C',
                ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/temperature_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {OUTPUT_DIR}/temperature_comparison.png")


def plot_temperature_trends(df):
    """
    Create line chart showing temperature trends over time for each city
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating temperature trends chart...")
    
    plt.figure(figsize=(14, 7))
    
    # Plot line for each city
    for city in df['city_name'].unique():
        city_data = df[df['city_name'] == city].sort_values('timestamp')
        plt.plot(city_data['timestamp'], city_data['temperature'], 
                marker='o', label=city, linewidth=2, markersize=4)
    
    # Customize
    plt.xlabel('Date/Time', fontsize=12, fontweight='bold')
    plt.ylabel('Temperature (°C)', fontsize=12, fontweight='bold')
    plt.title('Temperature Trends Over Time', fontsize=14, fontweight='bold', pad=20)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/temperature_trends.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  aved: {OUTPUT_DIR}/temperature_trends.png")


def plot_humidity_vs_temperature(df):
    """
    Create scatter plot showing relationship between humidity and temperature
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating humidity vs temperature scatter plot...")
    
    plt.figure(figsize=(12, 7))
    
    # Create scatter plot for each city
    for city in df['city_name'].unique():
        city_data = df[df['city_name'] == city]
        plt.scatter(city_data['temperature'], city_data['humidity'], 
                   label=city, alpha=0.6, s=100, edgecolors='black', linewidth=0.5)
    
    # Customize
    plt.xlabel('Temperature (°C)', fontsize=12, fontweight='bold')
    plt.ylabel('Humidity (%)', fontsize=12, fontweight='bold')
    plt.title('Humidity vs Temperature', fontsize=14, fontweight='bold', pad=20)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/humidity_vs_temperature.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f" Saved: {OUTPUT_DIR}/humidity_vs_temperature.png")


def plot_weather_conditions_distribution(df):
    """
    Create pie chart showing distribution of weather conditions
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating weather conditions distribution chart...")
    
    # Count weather conditions
    condition_counts = df['weather_condition'].value_counts()
    
    plt.figure(figsize=(10, 8))
    
    # Create pie chart
    colors = plt.cm.Pastel1.colors
    plt.pie(condition_counts.values, labels=condition_counts.index, autopct='%1.1f%%',
           startangle=90, colors=colors, textprops={'fontsize': 11})
    
    plt.title('Distribution of Weather Conditions', fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/weather_conditions.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {OUTPUT_DIR}/weather_conditions.png")


def plot_temperature_heatmap(df):
    """
    Create heatmap showing temperature by city and hour of day
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating temperature heatmap...")
    
    # Create pivot table
    if df['hour'].nunique() > 1:  # Only if we have multiple hours
        pivot_data = df.pivot_table(
            values='temperature',
            index='city_name',
            columns='hour',
            aggfunc='mean'
        )
        
        plt.figure(figsize=(14, 8))
        
        # Create heatmap
        sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='YlOrRd', 
                   cbar_kws={'label': 'Temperature (°C)'}, linewidths=0.5)
        
        plt.xlabel('Hour of Day', fontsize=12, fontweight='bold')
        plt.ylabel('City', fontsize=12, fontweight='bold')
        plt.title('Average Temperature by City and Hour', fontsize=14, fontweight='bold', pad=20)
        
        plt.tight_layout()
        plt.savefig(f'{OUTPUT_DIR}/temperature_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  Saved: {OUTPUT_DIR}/temperature_heatmap.png")
    else:
        print("  Skipped: Need multiple hours of data for heatmap")


def plot_wind_speed_comparison(df):
    """
    Create box plot comparing wind speeds across cities
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating wind speed comparison chart...")
    
    plt.figure(figsize=(12, 7))
    
    # Prepare data for box plot
    cities = df['city_name'].unique()
    wind_data = [df[df['city_name'] == city]['wind_speed'].dropna() for city in cities]
    
    # Create box plot
    bp = plt.boxplot(wind_data, labels=cities, patch_artist=True,
                     boxprops=dict(facecolor='lightblue', alpha=0.7),
                     medianprops=dict(color='red', linewidth=2),
                     whiskerprops=dict(color='blue'),
                     capprops=dict(color='blue'))
    
    # Customize
    plt.xlabel('City', fontsize=12, fontweight='bold')
    plt.ylabel('Wind Speed (m/s)', fontsize=12, fontweight='bold')
    plt.title('Wind Speed Distribution by City', fontsize=14, fontweight='bold', pad=20)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/wind_speed_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {OUTPUT_DIR}/wind_speed_comparison.png")


def plot_feels_like_vs_actual(df):
    """
    Create scatter plot comparing 'feels like' temperature vs actual temperature
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating feels like vs actual temperature chart...")
    
    plt.figure(figsize=(10, 8))
    
    # Create scatter plot
    for city in df['city_name'].unique():
        city_data = df[df['city_name'] == city]
        plt.scatter(city_data['temperature'], city_data['feels_like'], 
                   label=city, alpha=0.6, s=100, edgecolors='black', linewidth=0.5)
    
    # Add diagonal line (where feels_like = temperature)
    min_temp = df[['temperature', 'feels_like']].min().min()
    max_temp = df[['temperature', 'feels_like']].max().max()
    plt.plot([min_temp, max_temp], [min_temp, max_temp], 
            'k--', alpha=0.3, linewidth=2, label='Equal line')
    
    # Customize
    plt.xlabel('Actual Temperature (°C)', fontsize=12, fontweight='bold')
    plt.ylabel('Feels Like Temperature (°C)', fontsize=12, fontweight='bold')
    plt.title('Feels Like vs Actual Temperature', fontsize=14, fontweight='bold', pad=20)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/feels_like_vs_actual.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Saved: {OUTPUT_DIR}/feels_like_vs_actual.png")


def create_summary_dashboard(df):
    """
    Create a comprehensive dashboard with multiple subplots
    
    Args:
        df: DataFrame with weather data
    """
    print("\nCreating summary dashboard...")
    
    fig = plt.figure(figsize=(16, 12))
    
    # 1. Temperature comparison
    ax1 = plt.subplot(3, 2, 1)
    avg_temp = df.groupby('city_name')['temperature'].mean().sort_values(ascending=False)
    ax1.barh(range(len(avg_temp)), avg_temp.values, color='steelblue', alpha=0.7)
    ax1.set_yticks(range(len(avg_temp)))
    ax1.set_yticklabels(avg_temp.index)
    ax1.set_xlabel('Avg Temperature (°C)')
    ax1.set_title('Average Temperature by City', fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Humidity comparison
    ax2 = plt.subplot(3, 2, 2)
    avg_humidity = df.groupby('city_name')['humidity'].mean().sort_values(ascending=False)
    ax2.barh(range(len(avg_humidity)), avg_humidity.values, color='coral', alpha=0.7)
    ax2.set_yticks(range(len(avg_humidity)))
    ax2.set_yticklabels(avg_humidity.index)
    ax2.set_xlabel('Avg Humidity (%)')
    ax2.set_title('Average Humidity by City', fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    # 3. Weather conditions pie chart
    ax3 = plt.subplot(3, 2, 3)
    condition_counts = df['weather_condition'].value_counts()
    ax3.pie(condition_counts.values, labels=condition_counts.index, autopct='%1.1f%%',
           startangle=90, colors=plt.cm.Pastel1.colors)
    ax3.set_title('Weather Conditions', fontweight='bold')
    
    # 4. Temperature category distribution
    ax4 = plt.subplot(3, 2, 4)
    if 'temp_category' in df.columns and df['temp_category'].notna().any():
        temp_cat_counts = df['temp_category'].value_counts()
        ax4.bar(range(len(temp_cat_counts)), temp_cat_counts.values, 
               color=['blue', 'lightblue', 'yellow', 'orange', 'red'][:len(temp_cat_counts)],
               alpha=0.7, edgecolor='black')
        ax4.set_xticks(range(len(temp_cat_counts)))
        ax4.set_xticklabels(temp_cat_counts.index, rotation=45, ha='right')
        ax4.set_ylabel('Count')
        ax4.set_title('Temperature Category Distribution', fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)
    
    # 5. Wind speed comparison
    ax5 = plt.subplot(3, 2, 5)
    avg_wind = df.groupby('city_name')['wind_speed'].mean().sort_values(ascending=False)
    ax5.barh(range(len(avg_wind)), avg_wind.values, color='lightgreen', alpha=0.7)
    ax5.set_yticks(range(len(avg_wind)))
    ax5.set_yticklabels(avg_wind.index)
    ax5.set_xlabel('Avg Wind Speed (m/s)')
    ax5.set_title('Average Wind Speed by City', fontweight='bold')
    ax5.grid(axis='x', alpha=0.3)
    
    # 6. Temperature range
    ax6 = plt.subplot(3, 2, 6)
    if 'temperature_range' in df.columns:
        avg_range = df.groupby('city_name')['temperature_range'].mean().sort_values(ascending=False)
        ax6.barh(range(len(avg_range)), avg_range.values, color='mediumpurple', alpha=0.7)
        ax6.set_yticks(range(len(avg_range)))
        ax6.set_yticklabels(avg_range.index)
        ax6.set_xlabel('Avg Temp Range (°C)')
        ax6.set_title('Average Temperature Range by City', fontweight='bold')
        ax6.grid(axis='x', alpha=0.3)
    
    plt.suptitle('Weather Data Summary Dashboard', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/summary_dashboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {OUTPUT_DIR}/summary_dashboard.png")


def generate_statistics_report(df, output_file='statistics_report.txt'):
    """
    Generate a text report with statistics
    
    Args:
        df: DataFrame with weather data
        output_file: Output filename
    """
    print("\n📝 Generating statistics report...")
    
    report_path = f'{OUTPUT_DIR}/{output_file}'
    
    with open(report_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("WEATHER DATA ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Records: {len(df)}\n")
        f.write(f"Date Range: {df['timestamp'].min()} to {df['timestamp'].max()}\n")
        f.write(f"Number of Cities: {df['city_name'].nunique()}\n\n")
        
        f.write("-" * 80 + "\n")
        f.write("TEMPERATURE STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Overall Average: {df['temperature'].mean():.2f}°C\n")
        f.write(f"Highest Recorded: {df['temperature'].max():.2f}°C ({df.loc[df['temperature'].idxmax(), 'city_name']})\n")
        f.write(f"Lowest Recorded: {df['temperature'].min():.2f}°C ({df.loc[df['temperature'].idxmin(), 'city_name']})\n\n")
        
        f.write("Temperature by City:\n")
        for city in sorted(df['city_name'].unique()):
            city_data = df[df['city_name'] == city]['temperature']
            f.write(f"  {city:20s}: Avg={city_data.mean():.2f}°C, "
                   f"Min={city_data.min():.2f}°C, Max={city_data.max():.2f}°C\n")
        
        f.write("\n" + "-" * 80 + "\n")
        f.write("HUMIDITY STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Overall Average: {df['humidity'].mean():.1f}%\n")
        f.write(f"Highest: {df['humidity'].max()}% ({df.loc[df['humidity'].idxmax(), 'city_name']})\n")
        f.write(f"Lowest: {df['humidity'].min()}% ({df.loc[df['humidity'].idxmin(), 'city_name']})\n\n")
        
        f.write("-" * 80 + "\n")
        f.write("WIND SPEED STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Overall Average: {df['wind_speed'].mean():.2f} m/s\n")
        f.write(f"Highest: {df['wind_speed'].max():.2f} m/s ({df.loc[df['wind_speed'].idxmax(), 'city_name']})\n\n")
        
        f.write("-" * 80 + "\n")
        f.write("WEATHER CONDITIONS\n")
        f.write("-" * 80 + "\n")
        for condition, count in df['weather_condition'].value_counts().items():
            percentage = (count / len(df)) * 100
            f.write(f"  {condition:20s}: {count:3d} records ({percentage:5.1f}%)\n")
        
        f.write("\n" + "=" * 80 + "\n")
    
    print(f"Saved: {report_path}")


def main():
    """
    Main visualization pipeline
    """
    print("\n" + "=" * 60)
    print("WEATHER DATA VISUALIZATION")
    print("=" * 60)
    
    # Connect to database
    conn = connect_to_database()
    if conn is None:
        return
    
    # Load data
    df = load_weather_data(conn)
    if df is None or len(df) == 0:
        print("No data to visualize")
        conn.close()
        return
    
    # Create output directory
    create_output_directory()
    
    # Generate all visualizations
    plot_temperature_comparison(df)
    plot_temperature_trends(df)
    plot_humidity_vs_temperature(df)
    plot_weather_conditions_distribution(df)
    plot_temperature_heatmap(df)
    plot_wind_speed_comparison(df)
    plot_feels_like_vs_actual(df)
    create_summary_dashboard(df)
    
    # Generate statistics report
    generate_statistics_report(df)
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 60)
    print("ALL VISUALIZATIONS COMPLETE!")
    print("=" * 60)
    print(f"\nAll files saved to: {OUTPUT_DIR}/")
    print("\nGenerated files:")
    print("  - temperature_comparison.png")
    print("  - temperature_trends.png")
    print("  - humidity_vs_temperature.png")
    print("  - weather_conditions.png")
    print("  - temperature_heatmap.png")
    print("  - wind_speed_comparison.png")
    print("  - feels_like_vs_actual.png")
    print("  - summary_dashboard.png")
    print("  - statistics_report.txt")
    print("\n")


if __name__ == "__main__":
    main()