from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
import logging
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_estado_normas
from core.database import get_db

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/cargar-archivos")
async def upload_estado_normas(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):

    # Leer archivo
    contents = await file.read()
    logger.info(f"Archivo recibido: {file.filename}, tamaño: {len(contents)} bytes")

    # Leer todo el Excel sin filtrar columnas para aceptar encabezados variados
    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        dtype=str
    )

    logger.info(f"DataFrame leído: {df.shape[0]} filas x {df.shape[1]} columnas")
    logger.info(f"Columnas originales: {list(df.columns)}")
    if df.shape[0] == 0:
        logger.warning("DataFrame vacío")
        return {
            "mensaje": "El archivo está vacío",
            "registros_cargados": 0,
            "errores": ["DataFrame sin filas"]
        }


    # ====== MAPEO HACIA LOS CAMPOS EXACTOS DEL CRUD / TABLA ======
    # Mapeo basado en nombres esperados; la coincidencia es flexible (mayúsculas/minúsculas, acentos, espacios)
    def _norm(s: str):
        if s is None:
            return ""
        return (
            s.strip()
            .upper()
            .replace("_", " ")
            .replace("\u00A0", " ")
            .replace("\n", " ")
        )

    expected = {
        "COD PROGRAMA": "cod_programa",
        "VERSIÓN PROG": "version_programa",
        "VERSION PROG": "version_programa",
        "CODIGO VERSION": "cod_version",
        "TIPO PROGRAMA": "tipo_programa",
        "NIVEL DE FORMACIÓN": "nivel_formacion",
        "NIVEL DE FORMACION": "nivel_formacion",
        "NOMBRE PROGRAMA": "nombre_programa",
        "ESTADO PROGRAMA": "estado_programa",
        "FECHA ELABORACION": "fecha_elaboracion",
        "FECHA DE ELABORACIÓN": "fecha_elaboracion_2",
        "FECHA DE ELABORACION": "fecha_elaboracion_2",
        "AÑO": "anio",
        "ANO": "anio",
        "RED CONOCIMIENTO": "red_conocimiento",
        "NOMBRE_NCL": "nombre_ncl",
        "NOMBRE NCL": "nombre_ncl",
        "NCL CODIGO": "cod_ncl",
        "NCL VERSION": "ncl_version",
        "TIPO DE COMPETENCIA": "tipo_competencia",
        "VIGENCIA": "vigencia",
    }

    # Construir dict de renombrado mapeando columnas reales a los esperados
    columnas_renombrar = {}
    for col in df.columns:
        n = _norm(str(col))
        if n in expected:
            columnas_renombrar[col] = expected[n]
        else:
            # buscar coincidencias parciales
            for k, v in expected.items():
                if k in n or n in k:
                    columnas_renombrar[col] = v
                    break

    df = df.rename(columns=columnas_renombrar)

    logger.info(f"Columnas renombradas: {list(df.columns)}")
    logger.info(f"Renombrados: {columnas_renombrar}")


    cargados = 0
    errores = []
    logger.info(f"Iniciando procesamiento de {df.shape[0]} filas")

    # ====== PROCESAR FILAS UNA A UNA ======
    for index, row in df.iterrows():
        try:
            res = insertar_estado_normas(db, row)
            logger.debug(f"Fila {index+1}: res={res}")
            # insertar_estado_normas debe devolver dict con 'registros_cargados' y 'errores'
            if isinstance(res, dict):
                if res.get("registros_cargados", 0) >= 1 and not res.get("errores"):
                    cargados += 1
                    logger.info(f"Fila {index+1}: OK, cargados={cargados}")
                else:
                    errores.append({
                        "fila": index + 1,
                        "error": res.get("errores") or res
                    })
                    logger.warning(f"Fila {index+1}: con errores o 0 registros")
            else:
                errores.append({
                    "fila": index + 1,
                    "error": "Respuesta inesperada del inserter"
                })
                logger.error(f"Fila {index+1}: respuesta inesperada")

        except Exception as e:
            errores.append({
                "fila": index + 1,
                "error": str(e)
            })
            logger.exception(f"Fila {index+1}: excepción")

    logger.info(f"Carga finalizada: {cargados} cargados, {len(errores)} errores")

    return {
        "mensaje": "Carga finalizada",
        "registros_cargados": cargados,
        "errores": errores
    }
