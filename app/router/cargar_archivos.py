from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_estado_normas
from core.database import get_db

router = APIRouter()

@router.post("/estado-normas/")
async def upload_estado_normas(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):

    # Leer archivo
    contents = await file.read()

    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        usecols=[
            'COD PROGRAMA', 'VERSIÓN PROG', 'CODIGO VERSION', 'TIPO PROGRAMA', 'NIVEL DE FORMACIÓN', 'NOMBRE PROGRAMA', 'ESTADO PROGRAMA', 'Fecha Elaboracion', 'Año', 'RED CONOCIMIENTO', 'NOMBRE_NCL', 'NOMBRE_NCL', 'NCL CODIGO', 'NCL VERSION', 'Norma corte a NOVIEMBRE', 'Versión', 'Norma - Versión', 'Mesa Sectorial', 'Tipo de Norma', 'Observación', 'Fecha de revisión', 'Tipo de competencia', 'Vigencia', 'Fecha de Elaboración'
        ],
        dtype=str
    )


    # ====== MAPEO HACIA LOS CAMPOS EXACTOS DEL CRUD / TABLA ======
    df = df.rename(columns = {
        "COD PROGRAMA": "cod_programa",
        "VERSIÓN PROG": "version_programa",
        "CODIGO VERSION": "cod_version",
        "TIPO PROGRAMA": "tipo_programa",
        "NIVEL DE FORMACIÓN": "nivel_formacion",
        "NOMBRE PROGRAMA": "nombre_programa",
        "ESTADO PROGRAMA": "estado_programa",
        "Fecha Elaboracion": "fecha_elaboracion",
        "Año": "anio",
        "RED CONOCIMIENTO": "red_conocimiento",
        "NOMBRE_NCL": "nombre_ncl",
        "NCL CODIGO": "cod_ncl",
        "NCL VERSION": "ncl_version",
        "Norma corte a NOVIEMBRE": "norma_corte_noviembre",
        "Versión": "version",
        "Norma - Versión": "norma_version",
        "Mesa Sectorial": "mesa_sectorial",
        "Tipo de Norma": "tipo_norma",
        "Observación": "observacion",
        "Fecha de revisión": "fecha_revision",
        "Tipo de competencia": "tipo_competencia",
        "Vigencia": "vigencia",
        "Fecha de Elaboración": "fecha_elaboracion_2",
    })


    cargados = 0
    errores = []
    

    # ====== PROCESAR FILAS UNA A UNA ======
    for index, row in df.iterrows():
        try:
            insertar_estado_normas(db, row)
            cargados += 1

        except Exception as e:
            errores.append({
                "fila": index + 1,
                "error": str(e)
            })

    return {
        "mensaje": "Carga finalizada",
        "registros_cargados": cargados,
        "errores": errores
    }
