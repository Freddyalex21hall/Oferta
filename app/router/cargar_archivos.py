from fastapi import APIRouter, FastAPI, UploadFile, File, Depends
from sqlalchemy.orm import Session
import pandas as pd
from io import BytesIO
from app.router.dependencias import get_current_user
from app.schemas.usuarios import RetornoUsuario
from core.database import get_db
from app.crud.cargar_archivos import insertar_estado_normas

router = APIRouter()

@router.post("/upload-estado-normas/")
async def upload_estado_normas(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    #user_token: RetornoUsuario = Depends(get_current_user)
):
    contents = await file.read()
    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        usecols=[
            "COD PROGRAMA","VERSIÓN PROG","CODIGO VERSION","TIPO PROGRAMA","NIVEL DE FORMACIÓN","NOMBRE PROGRAMA","ESTADO PROGRAMA","Fecha Elaboracion","Año","RED CONOCIMIENTO","NOMBRE_NCL","NCL CODIGO","NCL VERSION","Norma corte a NOVIEMBRE","Versión","Norma - Versión","Mesa Sectorial","Tipo de Norma","Observación","Fecha de revisión","Tipo de competencia","Vigencia","Fecha de Elaboración"
        ], 
        dtype=str
    )

    # df[' '] = pd.to_datetime(df[''], errors='coerce').dt.date

    df = df.rename(columns={
        "COD PROGRAMA": "cod_programa",
        "CODIGO VERSION": "cod_version",
        "VERSIÓN PROG": "cod_version",
        "Fecha Elaboracion": "fecha_elaboracion",
        "FECHA DE ELABORACIÓN": "fecha_elaboracion",
        "FECHA DE REVISIÓN": "fecha_revision",
        "FECHA INDICE": "fecha_indice",
        "Año": "anio",
        "RED CONOCIMIENTO": "red_conocimiento",
        "NOMBRE_NCL": "nombre_ncl",
        "NOMBRE NCL": "nombre_ncl",
        "NCL CODIGO": "cod_ncl",
        "NCL VERSION": "ncl_version",
        "Norma corte a NOVIEMBRE": "norma_corte_noviembre",
        "Versión": "version_norma",
        "Norma - Versión": "norma_version",
        "Mesa Sectorial": "mesa_sectorial",
        "Tipo de Norma": "tipo_norma",
        "Fecha de revisión": "observacion",
        "Tipo de competencia": "tipo_competencia",
        "Vigencia": "vigencia",
    })

    resultados = insertar_estado_normas(db, df)

    return resultados

