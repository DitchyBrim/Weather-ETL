"""
Weather Data Extraction Script
Extracts weather data from OpenWeatherMap API for multiple cities
Saves raw data to timestamped JSON files
"""

import os
import json
from datetime import datetime, timezone
from pyowm.owm import OWM
import time
from dotenv import load_dotenv


def get_weather(owm_manager, location):
    """
    Get current weather data for a specific location
    
    Args:
        owm_manager: OWM weather manager instance
        location: Location string (e.g., "Paris")
    Returns:
        Dictionary with weather data or None if error
    """
    try:
        # Request current weather data
        observation = owm_manager.weather_at_place(location)
        weather = observation.weather
        
        # Extract all relevant weather information
        weather_data = {
            'city_name': observation.location.name,
            'country': observation.location.country,
            'latitude': observation.location.lat,
            'longitude': observation.location.lon,
            'temperature': weather.temperature('celsius')['temp'],
            'temperature_min': weather.temperature('celsius')['temp_min'],
            'temperature_max': weather.temperature('celsius')['temp_max'],
            'feels_like': weather.temperature('celsius').get('feels_like'),
            'humidity': weather.humidity,
            'pressure': weather.pressure.get('press'),
            'description': weather.detailed_status,
            'weather_condition': weather.status,
            'wind_speed': weather.wind().get('speed'),
            'wind_direction': weather.wind().get('deg'),
            'cloudiness': weather.clouds,
            'timestamp': datetime.fromtimestamp(
                weather.reference_time(), tz=timezone.utc
            ).isoformat(),
            'data_collected_at': datetime.now(timezone.utc).isoformat()
        }
        
        print(f"Successfully fetched weather for {observation.location.name}, {observation.location.country}")
        return weather_data
        
    except Exception as e:
        print(f"Error fetching weather for {location}: {e}")
        return None


def extract_weather_for_cities(api_key, cities, delay=1):
    """
    Extract weather data for multiple cities
    
    Args:
        api_key: OpenWeatherMap API key
        cities: List of city strings (e.g., ["Paris", "London"])
        delay: Delay in seconds between API calls (to respect rate limits)
    Returns:
        List of weather data dictionaries
    """
    print(f"\n--- Starting Weather Data Extraction ---")
    print(f"Cities to fetch: {len(cities)}")
    print(f"API rate limit delay: {delay}s between requests\n")
    
    # Initialize OWM
    owm = OWM(api_key)
    mgr = owm.weather_manager()
    
    all_weather_data = []
    successful = 0
    failed = 0
    
    for idx, city in enumerate(cities, 1):
        print(f"[{idx}/{len(cities)}] Fetching {city}...", end=" ")
        
        weather_data = get_weather(mgr, city)
        
        if weather_data:
            all_weather_data.append(weather_data)
            successful += 1
        else:
            failed += 1
        
        # Respect API rate limits (don't hammer the API)
        if idx < len(cities):  # Don't delay after last request
            time.sleep(delay)
    
    print(f"\n--- Extraction Complete ---")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total records: {len(all_weather_data)}")
    
    return all_weather_data


def save_raw_data(data, output_dir='data'):
    """
    Save raw weather data to timestamped JSON file
    
    Args:
        data: List of weather data dictionaries
        output_dir: Directory to save the file
    Returns:
        Filepath where data was saved
    """
    # Create data directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"weather_data_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    # Save to JSON with pretty formatting
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\nRaw data saved to: {filepath}")
        print(f"File size: {os.path.getsize(filepath) / 1024:.2f} KB")
        print(f"Records: {len(data)}")
        
        return filepath
        
    except Exception as e:
        print(f"Error saving data: {e}")
        return None


def load_cities_from_file(filepath='cities.txt'):
    """
    Load list of cities from a text file (one city per line)
    
    Args:
        filepath: Path to cities file
    Returns:
        List of city strings
    """
    try:
        with open(filepath, 'r') as f:
            cities = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(cities)} cities from {filepath}")
        return cities
    except FileNotFoundError:
        print(f"Cities file not found: {filepath}")
        return []


def print_summary(data):
    """
    Print a summary of the extracted data
    
    Args:
        data: List of weather data dictionaries
    """
    if not data:
        print("\nNo data to summarize.")
        return
    
    print("\n--- Data Summary ---")
    
    # Temperature statistics
    temps = [d['temperature'] for d in data if d.get('temperature') is not None]
    if temps:
        print(f"\nTemperature Statistics:")
        print(f"  Average: {sum(temps)/len(temps):.1f}°C")
        print(f"  Min: {min(temps):.1f}°C ({[d['city_name'] for d in data if d.get('temperature') == min(temps)][0]})")
        print(f"  Max: {max(temps):.1f}°C ({[d['city_name'] for d in data if d.get('temperature') == max(temps)][0]})")
    
    # Humidity statistics
    humidity = [d['humidity'] for d in data if d.get('humidity') is not None]
    if humidity:
        print(f"\nHumidity Statistics:")
        print(f"  Average: {sum(humidity)/len(humidity):.1f}%")
        print(f"  Min: {min(humidity)}% ({[d['city_name'] for d in data if d.get('humidity') == min(humidity)][0]})")
        print(f"  Max: {max(humidity)}% ({[d['city_name'] for d in data if d.get('humidity') == max(humidity)][0]})")
    
    # Weather conditions
    conditions = {}
    for d in data:
        condition = d.get('weather_condition', 'Unknown')
        conditions[condition] = conditions.get(condition, 0) + 1
    
    print(f"\nWeather Conditions:")
    for condition, count in sorted(conditions.items(), key=lambda x: x[1], reverse=True):
        print(f"  {condition}: {count} cities")


def main():
    
    """
    Main extraction pipeline
    """
    load_dotenv()
    # Get API key from environment variable
    api_key = os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        print("Error: OPENWEATHER_API_KEY environment variable not set")
        print("Set it using: export OPENWEATHER_API_KEY='your_key_here'")
        print("Or add it to your .env file")
        return
    
    # Define cities to fetch weather for
    # Format: "City,CountryCode" (use ISO 3166 country codes)
    cities = [
        "London",
        "Tokyo,JP",
        "Sydney,AU",
        "Moscow,RU",
        "Toronto,CA",
        "Davao, PH",
        "Manila, PH",
        "Panabo, PH"
    ]
    
    # Alternative: Load cities from file
    # Uncomment the line below and create a cities.txt file
    # cities = load_cities_from_file('cities.txt')
    
    if not cities:
        print("No cities to process")
        return
    
    # Extract weather data
    weather_data = extract_weather_for_cities(
        api_key=api_key,
        cities=cities,
        delay=1  # 1 second delay between requests
    )
    
    if not weather_data:
        print("No weather data extracted")
        return
    
    # Save raw data with timestamp
    saved_filepath = save_raw_data(
        data=weather_data,
        output_dir='data'
    )
    
    # Print summary
    print_summary(weather_data)
    print("1. Run: python transform_weather.py")
    print("2. Then run: python load_weather.py")
    print("3. Finally run: python visualize_weather.py")


if __name__ == "__main__":
    
    main()