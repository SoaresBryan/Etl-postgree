from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from service.db.connection import get_db
from service.utils.utils import etl_process

import pandas as pd

router = APIRouter()

@router.post("/process-etl/")
def process_etl(file_path: str, db: Session = Depends(get_db)):
    """
    Endpoint para receber o caminho do arquivo de dados e processar o ETL
    """
    # Ler o arquivo CSV ou Excel para um DataFrame
    data = pd.read_csv(file_path)  # Pode ser ajustado para Excel ou outro formato
    
    # Executar o processo ETL
    etl_process(data, db)
    
    return {"message": "Processo ETL concluído com sucesso!"}
