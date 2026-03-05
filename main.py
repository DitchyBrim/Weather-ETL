from extract_weather import get_weather
from transform_weather import transform_weather
import os

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv() 
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("Please set OPENWEATHER_API_KEY environment variable")
    location = "Paris,FR"
    raw_weather_info = get_weather(api_key, location)
    if raw_weather_info:
        transformed_weather_info = transform_weather(raw_weather_info)
        print(transformed_weather_info)