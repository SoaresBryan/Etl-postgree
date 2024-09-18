import pandas as pd
from sqlalchemy.orm import Session
from db.connection import engine

def process_dates(df):
    """Converte datas para o formato YYYYMMDD"""
    df['data'] = pd.to_datetime(df['data']).dt.strftime('%Y%m%d')
    return df

def categorize_reprova(df):
    """Transforma a coluna 'situação' em 'Reprovado por Faltas' ou 'Reprovado por Média'"""
    df['situacao'] = df.apply(lambda row: 'Reprovado por Faltas' if row['faltas'] > 25 else 'Reprovado por Média' if row['media'] < 6.0 else 'Aprovado', axis=1)
    return df

def limit_vagas(df):
    """Garante que o número de vagas ocupadas não excede o total de vagas"""
    df['vagas_ocupadas'] = df.apply(lambda row: min(row['vagas_ocupadas'], row['total_vagas']), axis=1)
    return df

def load_to_datamart(df, session: Session):
    """Carrega os dados no Data Mart"""
    df.to_sql('fato_desempenho_academico', con=engine, if_exists='append', index=False)
    session.commit()

def etl_process(data, session: Session):
    """Executa o processo ETL completo"""

    data = process_dates(data)
    data = categorize_reprova(data)
    data = limit_vagas(data)
    
    load_to_datamart(data, session)
