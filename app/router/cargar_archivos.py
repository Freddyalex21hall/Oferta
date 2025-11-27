from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_datos_en_bd, insertar_estado_normas
from core.database import get_db

router = APIRouter(prefix="/cargar-archivos", tags=["cargar archivos"])

@router.post("/upload-excel-pe04/")
async def upload_excel(
    file: UploadFile = File(...),
    tipo: str = "grupos",
    db: Session = Depends(get_db)
):
    contents = await file.read()

    # Leer el Excel sin restricciones de columnas
    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        dtype=str
    )

    # Normalizar encabezados (quita espacios, mayúsculas)
    df.columns = df.columns.str.strip().str.upper()

    print("\nCOLUMNAS ENCONTRADAS EN EL EXCEL:")
    print(df.columns.tolist(), "\n")

    if tipo == "normas":

        mapping_normas = {
            "COD_PROGRAMA": "cod_programa",
            "COD_VERSION": "cod_version",
            "FECHA_ELABORACION": "fecha_elaboracion",
            "ANIO": "anio",
            "RED_CONOCIMIENTO": "red_conocimiento",
            "NOMBRE_NCL": "nombre_ncl",
            "COD_NCL": "cod_ncl",
            "NCL_VERSION": "ncl_version",
            "NORMA_CORTE_NOVIEMBRE": "norma_corte_noviembre",
            "VERSION": "version",
            "NORMA_VERSION": "norma_version",
            "MESA_SECTORIAL": "mesa_sectorial",
            "TIPO_NORMA": "tipo_norma",
            "OBSERVACION": "observacion",
            "FECHA_REVISION": "fecha_revision",
            "TIPO_COMPETENCIA": "tipo_competencia",
            "VIGENCIA": "vigencia",
            "FECHA_INDICE": "fecha_indice",
        }

        df = df.rename(columns={k: v for k, v in mapping_normas.items() if k in df.columns})

        return insertar_estado_normas(db, df)

    # ============================================================
    #        PROCESAMIENTO DE GRUPOS PE04 (CARGA PRINCIPAL)
    # ============================================================

    mapping_grupos = {
        "IDENTIFICADOR_FICHA": "cod_ficha",
        "CODIGO_CENTRO": "cod_centro",
        "CODIGO_PROGRAMA": "cod_programa",
        "VERSION_PROGRAMA": "la_version",
        "NOMBRE_PROGRAMA_FORMACION": "nombre",
        "ESTADO_CURSO": "estado_grupo",
        "NIVEL_FORMACION": "nombre_nivel",
        "NOMBRE_JORNADA": "jornada",
        "FECHA_INICIO_FICHA": "fecha_inicio",
        "FECHA_TERMINACION_FICHA": "fecha_fin",
        "ETAPA_FICHA": "etapa",
        "MODALIDAD_FORMACION": "modalidad",
        "NOMBRE_RESPONSABLE": "responsable",
        "NOMBRE_EMPRESA": "nombre_empresa",
        "NOMBRE_MUNICIPIO_CURSO": "nombre_municipio",
        "NOMBRE_PROGRAMA_ESPECIAL": "nombre_programa_especial"
    }

    # Renombrar solo columnas que existan en el archivo
    df = df.rename(columns={k: v for k, v in mapping_grupos.items() if k in df.columns})

    # Campos obligatorios
    required_fields = [
        "cod_ficha", "cod_centro", "cod_programa", "la_version",
        "nombre", "fecha_inicio", "fecha_fin",
        "etapa", "responsable", "nombre_municipio"
    ]

    # Verificar si faltan columnas críticas
    faltantes = [col for col in required_fields if col not in df.columns]
    if faltantes:
        return {
            "error": "Algunas columnas obligatorias no existen en el Excel",
            "faltantes": faltantes,
            "columnas_en_excel": df.columns.tolist()
        }

    # Eliminar filas incompletas
    df = df.dropna(subset=required_fields)

    # Convertir columnas numéricas
    for col in ["cod_ficha", "cod_programa", "la_version", "cod_centro"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Convertir fechas
    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce").dt.date
    df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], errors="coerce").dt.date

    # Valores adicionales
    df["hora_inicio"] = "00:00:00"
    df["hora_fin"] = "00:00:00"
    df["aula_actual"] = ""

    # Programas únicos
    df_programas = df[["cod_programa", "la_version", "nombre"]].drop_duplicates()

    # Eliminar "nombre" de df (solo queda en df_programas)
    df = df.drop(columns=["nombre"], errors="ignore")

    # Llamar al CRUD final
    resultados = insertar_datos_en_bd(db, df_programas, df)
    return resultados
