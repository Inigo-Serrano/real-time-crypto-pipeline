import boto3 # Para conectar con AWS
from tradingview_client import TradingViewData, Interval
import pandas as pd
import os

from dotenv import load_dotenv

load_dotenv()

AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION")
S3_BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET")

request = TradingViewData() # Hacemos la request

nifty_data = request.get_hist(symbol='LINKUSD',exchange='BINANCE',interval=Interval.daily,n_bars=1484) # Pedimos los datos de
# 1484 días (años de 2022 a 2025) de Chainlink, en el exchange BINANCE

print(nifty_data)

nifty_data.index = pd.to_datetime(nifty_data.index) # Convertimos el índice a datetime para filtrar por año

data_2022 = nifty_data[nifty_data.index.year == 2022] # Filtramos por año
data_2023 = nifty_data[nifty_data.index.year == 2023]
data_2024 = nifty_data[nifty_data.index.year == 2024]
data_2025 = nifty_data[nifty_data.index.year == 2025]

data_2022.to_csv("linkusd2022.csv", index = False) # Guardamos los CSV
data_2023.to_csv("linkusd2023.csv", index = False)
data_2024.to_csv("linkusd2024.csv", index = False)
data_2025.to_csv("linkusd2025.csv", index = False)

session = boto3.Session(
    profile_name=AWS_PROFILE or None,
    region_name=AWS_REGION or None
)

s3 = session.client("s3")

bucket_name = S3_BRONZE_BUCKET

for year in range(2022, 2026):
    s3.put_object(
        Bucket=bucket_name,
        Key=f"LINKUSD/year={year}/"
    )

s3.upload_file(
    "linkusd2022.csv",
    bucket_name,
    "LINKUSD/year=2022/linkusd2022.csv"
)
s3.upload_file(
    "linkusd2023.csv",
    bucket_name,
    "LINKUSD/year=2023/linkusd2023.csv"
)

s3.upload_file(
    "linkusd2024.csv",
    bucket_name,
    "LINKUSD/year=2024/linkusd2024.csv"
)

s3.upload_file(
    "linkusd2025.csv",
    bucket_name,
    "LINKUSD/year=2025/linkusd2025.csv"
)