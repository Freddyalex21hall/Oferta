from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_estado_normas
from core.database import get_db

router = APIRouter(prefix="/cargar-archivos", tags=["cargar archivos"])

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
        dtype=str
    )

    # Normalizar encabezados
    df.columns = df.columns.str.strip().str.upper()

    print("\nCOLUMNAS DETECTADAS:")
    print(df.columns.tolist(), "\n")

    # ====== MAPEO HACIA LOS CAMPOS EXACTOS DEL CRUD / TABLA ======
    mapping = {
        "COD PROGRAMA": "cod_programa",
        "VERSIÓN PROG": "version_programa",
        "CODIGO VERSION": "codigo_version",
        "TIPO PROGRAMA": "tipo_programa",
        "NIVEL DE FORMACIÓN": "nivel_formacion",
        "NOMBRE PROGRAMA": "nombre_programa",
        "ESTADO PROGRAMA": "estado_programa",
        "FECHA ELABORACION": "fecha_elaboracion",
        "AÑO": "anio",
        "RED CONOCIMIENTO": "red_conocimiento",
        "NOMBRE_NCL": "nombre_ncl",
        "NCL CODIGO": "ncl_codigo",
        "NCL VERSION": "ncl_version",
        "NORMA CORTE A NOVIEMBRE": "norma_corte_noviembre",
        "VERSIÓN": "version",
        "NORMA - VERSIÓN": "norma_version",
        "MESA SECTORIAL": "mesa_sectorial",
        "TIPO DE NORMA": "tipo_norma",
        "OBSERVACIÓN": "observacion",
        "FECHA DE REVISIÓN": "fecha_revision",
        "TIPO DE COMPETENCIA": "tipo_competencia",
        "VIGENCIA": "vigencia",
        "FECHA DE ELABORACIÓN": "fecha_elaboracion_2",
    }

    # Renombrar columnas según mapping
    df = df.rename(columns=mapping)

    # Reemplazar NaN por None
    df = df.where(pd.notnull(df), None)

    # Campos obligatorios según tu base
    columnas_obligatorias = [
        "cod_programa",
        "codigo_version",
        "anio",
        "nombre_ncl",
        "ncl_codigo",
        "ncl_version",
        "mesa_sectorial",
        "tipo_norma",
        "vigencia"
    ]

    faltantes = [c for c in columnas_obligatorias if c not in df.columns]

    if faltantes:
        return {
            "error": "Faltan columnas requeridas para cargar Estado de Normas",
            "faltantes": faltantes,
            "columnas_recibidas": df.columns.tolist()
        }

    cargados = 0
    errores = []

    # ====== PROCESAR FILAS UNA A UNA ======
    for index, row in df.iterrows():
        try:
            data = row.to_dict()
            insertar_estado_normas(db, data)
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
