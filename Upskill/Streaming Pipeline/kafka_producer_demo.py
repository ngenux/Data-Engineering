import time
import json
from kafka import KafkaProducer
import requests


kafka_bootstrap_servers = ['localhost:9092']
kafka_topic_name = 'sampletopic1'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer = lambda v: json.dumps(v).encode('utf-8'))

json_message = None
city_name = None
temperature = None
humidity = None
openweathermap_api_endpoint = None
appid = None

def get_weather_detail(openweather_api_endpoint):
    api_response = requests.get(openweather_api_endpoint)
    json_data = api_response.json()
    print(json_data)
    city_name = json_data["name"]
    humidity = json_data["main"]["humidity"]
    temperature = json_data["main"]["temp"]
    json_message = {"CityName" : city_name, "Temperature" : temperature,"Humidity" : humidity, "CreationTime": time.strftime("%Y-%m-%d %H:%M:%S")}
    return json_message

while True:
    city_name = "Chennai"
    appid = '59241ef61a97b33e4575c0a9351ef4f4'
    openweather_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweather_api_endpoint)
    producer.send(kafka_topic_name,json_message)
    print("Published message 1 : " + json.dumps(json_message))
    print("Wait for 2 seconds...")
    time.sleep(2)

    city_name = "Hyderabad"
    appid = '59241ef61a97b33e4575c0a9351ef4f4'
    openweather_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweather_api_endpoint)
    producer.send(kafka_topic_name,json_message)
    print("Published message 1 : " + json.dumps(json_message))
    print("Wait for 2 seconds...")
    time.sleep(2)