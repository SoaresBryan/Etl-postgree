from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.orm import Session
from service.db.connection import get_db
from service.utils.utils import etl_process, update_coordinator
import pandas as pd
import io

router = APIRouter()

@router.post("/process-etl/")
async def process_etl(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Endpoint para receber um arquivo de dados (.sql ou CSV) e processar o ETL.
    """
    # Ler o conteúdo do arquivo (usando CSV para este exemplo)
    contents = await file.read()
    data = pd.read_csv(io.StringIO(contents.decode('utf-8')))

    # Executar o processo ETL
    etl_process(data, db)

    return {"message": "Processo ETL concluído com sucesso!"}
