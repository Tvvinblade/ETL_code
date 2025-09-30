import requests
import pandas as pd
import time
import logging
import concurrent.futures

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("weather-demo")

# Глобальные списки
success_cities = []
failed_cities = []

# Рандомный список городов
citys = [
    "Berlin", "London", "Paris", "Madrid", "Rome", "Warsaw", "Vienna", "Prague", "Helsinki", "Stockholm",
    "Oslo", "Copenhagen", "Lisbon", "Dublin", "Brussels", "Budapest", "Zurich", "Athens", "Sofia", "Tallinn",
    "Vilnius", "Riga", "Belgrade", "Zagreb", "Ljubljana", "Skopje", "Podgorica", "Sarajevo", "Tirana", "Chisinau",
    "Minsk", "Moscow", "Istanbul", "Ankara", "Tel Aviv", "Dubai", "New York", "Chicago", "Los Angeles", "San Francisco",
    "Toronto", "Vancouver", "Mexico City", "Buenos Aires", "Santiago", "Lima", "Bogota", "Caracas", "Rio de Janeiro", "São Paulo",
    "Cape Town", "Nairobi", "Lagos", "Accra", "Cairo", "Casablanca", "Algiers", "Tunis", "Johannesburg", "Addis Ababa",
    "Beijing", "Shanghai", "Tokyo", "Osaka", "Seoul", "Hong Kong", "Taipei", "Bangkok", "Singapore", "Kuala Lumpur",
    "Jakarta", "Hanoi", "Manila", "New Delhi", "Mumbai", "Bangalore", "Dhaka", "Karachi", "Islamabad", "Tehran",
    "Sydney", "Melbourne", "Auckland", "Wellington", "Perth", "Brisbane", "Adelaide", "Christchurch", "Honolulu", "Anchorage",
    "San Diego", "Houston", "Dallas", "Miami", "Philadelphia", "Atlanta", "Boston", "Seattle", "Montreal", "Quebec"
]

def fetch_weather(name_city: str, retries: int = 3, delay: int = 2):
    """
    Параметры:
    - name_city: название города для демки
    - retries: число повторных попыток
    - delay: задержка между попытками
    Возвращает: DataFrame с одним JSON-объектом или пустой DF при ошибке
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": "48.023",
        "longitude": "37.8022",
        "hourly": ["rain", "temperature_2m", "relative_humidity_2m", "is_day"],
        "start_date": "2025-09-29",
        "end_date": "2025-09-29"
    }

    result = None

    for attempt in range(1, retries + 1):
        try:
            web = requests.get(url, params=params, timeout=10)

            if web.status_code == 200:
                logger.info(f"[{name_city}] Успешный запрос с {attempt}-й попытки")
                result = web.json()
                success_cities.append(name_city)
                break
            else:
                logger.error(f"[{name_city}] Ошибка {web.status_code}: {web.text}")
        except Exception as e:
            logger.error(f"[{name_city}] Ошибка соединения: {e}")

        logger.info(f"[{name_city}] Ждём {delay} сек перед повтором...")
        time.sleep(delay)

    if not result:
        logger.warning(f"[{name_city}] Все {retries} попыток не удались")
        failed_cities.append(name_city)
        return pd.DataFrame()

    return pd.DataFrame([result])

#Пустой список для хранения датафреймов
results = [] 
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor: #Открываем пул потоков, будет работать в 4 потока
    futures = [executor.submit(fetch_weather, city) for city in citys] # Создаём список задач для каждого города
    for future in concurrent.futures.as_completed(futures): # Забираем результаты по мере готовности
        df = future.result()
        if not df.empty:
            results.append(df)

# Склеиваем все результаты в один DataFrame
df_weather = pd.concat(results, ignore_index=True)

print("Кол-во успешных запросов:", len(success_cities))

if failed_cities:
    print("Проблемные города:", failed_cities)
