import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

#import das variaveis

load_dotenv()

commodities = ['CL=F', 'GC-F', 'SI=F']

DB_HOST= os.getenv('DB_HOST_PROD')
DB_PORT= os.getenv('DB_PORT_PROD')
DB_NAME= os.getenv('DB_NAME_PROD')
DB_USER= os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


engine = create_engine(DATABASE_URL)

def buscar_dados(simbolo, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

def buscar_todos_dados(commodities):
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_dados(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

def salvar_postgre(df,schema='public'):
    df.to_sql('commodities', engine, if_exists='replace',index=True, index_label='Date',schema=schema)



if __name__ =="__main__":
    dados_concatenados = buscar_todos_dados(commodities)
    salvar_postgre(dados_concatenados, schema="public")





# pegar a cotacao

# concatenar os meus ativos

# salvar no banco de dados

