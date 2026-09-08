import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION")

GLUE_DATABASE = os.getenv("GLUE_DATABASE")
GLUE_CRAWLER_NAME = os.getenv("GLUE_CRAWLER_NAME")
GLUE_ROLE_ARN = os.getenv("GLUE_ROLE_ARN")

S3_BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET")


session = boto3.Session(
    profile_name=AWS_PROFILE or None,
    region_name=AWS_REGION or None
)
glue = session.client('glue') # Accedemos a AWS Glue


def ensure_database(db_name: str): # Función para crear la base de datos
    try:
        glue.create_database(DatabaseInput={"Name": db_name}) # Creamos la base de datos si no existe
        print("Database creada:", db_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException": 
            print("Database ya existe:", db_name)
        else:
            raise

ensure_database(GLUE_DATABASE)

def create_crawler( # Función para crear el crawler
    crawler_name: str, # Nombre del crawler
    db_name: str, # Nombre de la base de datos donde se guardarán las tablas que el crawler cree
    role_arn: str, # ROl de IAM que el crawler usará
    s3_target_path: str, # Ruta del bucket en S3 donde están los datos de linkUSD
):
    try: 
        glue.create_crawler( # Con glue.create_crawler creamos el crawler con los parámetros necesarios
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": s3_target_path}]},
            SchemaChangePolicy={ 
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"}, # Elegimos el mismo funcionamiento que cuando creamos el crawler a mano
            TablePrefix="",  # Esta vez sin prefijo
        )
        print("Crawler creado:", crawler_name)
    except ClientError as e: # Si el crawler ya existe, no lo creamos de nuevo
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print("Crawler ya existe:", crawler_name)
        else:
            raise

required = {
    "GLUE_DATABASE": GLUE_DATABASE,
    "GLUE_CRAWLER_NAME": GLUE_CRAWLER_NAME,
    "GLUE_ROLE_ARN": GLUE_ROLE_ARN,
    "S3_BRONZE_BUCKET": S3_BRONZE_BUCKET,
}

missing = [name for name, value in required.items() if not value]

if missing:
    raise ValueError(
        f"Missing environment variables: {', '.join(missing)}"
    )

create_crawler(
    crawler_name=GLUE_CRAWLER_NAME,
    db_name=GLUE_DATABASE,
    role_arn=GLUE_ROLE_ARN,
    s3_target_path=f"s3://{S3_BRONZE_BUCKET}/LINKUSD/",
)
glue.start_crawler(Name=GLUE_CRAWLER_NAME)
print("Crawler lanzado")
resp = glue.get_tables(
    DatabaseName=GLUE_DATABASE
)
print([t["Name"] for t in resp["TableList"]])
