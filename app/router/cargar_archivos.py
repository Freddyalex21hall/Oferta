
from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_datos_en_bd
from core.database import get_db

router = APIRouter()

@router.post("/upload-excel-pe04/")
async def upload_excel(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    contents = await file.read()
    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        skiprows=4,
        usecols=["IDENTIFICADOR_FICHA", "CODIGO_REGIONAL", "NOMBRE_REGIONAL", "CODIGO_CENTRO", "NOMBRE_CENTRO", "CODIGO_PROGRAMA", "VERSION_PROGRAMA", "NOMBRE_PROGRAMA_FORMACION", "ESTADO_CURSO", "NIVEL_FORMACION", "NOMBRE_JORNADA", "FECHA_INICIO_FICHA", "FECHA_TERMINACION_FICHA", "ETAPA_FICHA", "MODALIDAD_FORMACION", "NOMBRE_RESPONSABLE", "NOMBRE_EMPRESA", "NOMBRE_MUNICIPIO_CURSO", "NOMBRE_PROGRAMA_ESPECIAL"],  # Nombres reales
        dtype=str
    )
    print(df.head())  # paréntesis
    print(df.columns)
    print(df.dtypes)

    # Renombrar columnas
    df = df.rename(columns={
        "IDENTIFICADOR_FICHA": "ficha",
        "CODIGO_CENTRO": "cod_centro",
        "NOMBRE_CENTRO": "nombre_centro",
        "CODIGO_REGIONAL": "cod_regional",
        "NOMBRE_REGIONAL": "nombre_regional",
        "CODIGO_PROGRAMA": "cod_programa",
        "NOMBRE_PROGRAMA_FORMACION": "nombre",
        "VERSION_PROGRAMA": "version",
        "ESTADO_CURSO": "estado_grupo",
        "NIVEL_FORMACION": "nivel",
        "NOMBRE_JORNADA": "jornada",
        "FECHA_INICIO_FICHA": "fecha_inicio",
        "FECHA_TERMINACION_FICHA": "fecha_fin",
        "ETAPA_FICHA": "etapa",
        "MODALIDAD_FORMACION": "modalidad",
        "NOMBRE_RESPONSABLE": "responsable",
        "NOMBRE_EMPRESA": "nombre_empresa",
        "NOMBRE_MUNICIPIO_CURSO": "nombre_municipio",
        "NOMBRE_PROGRAMA_ESPECIAL": "nombre_programa_especial"
    })

    print(df.head())  # paréntesis
    print(df.columns)

    # Eliminar filas con valores faltantes en campos obligatorios
    required_fields = [
        "ficha", "cod_centro", "cod_programa", "version", "nombre", 
        "fecha_inicio", "fecha_fin", "etapa", "responsable", "nombre_municipio"
    ]
    df = df.dropna(subset=required_fields)

    # Convertir columnas a tipo numérico
    for col in ["ficha", "cod_programa", "cod_centro", "cod_regional"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    print(df.head())  # paréntesis
    print(df.dtypes)

    # Convertir fechas
    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce").dt.date
    df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], errors="coerce").dt.date

    # Asegurar columnas no proporcionadas
    df["hora_inicio"] = "00:00:00"
    df["hora_fin"] = "00:00:00"
    df["aula_actual"] = ""

    # Crear DataFrame de programas únicos
    df_programas = df[["cod_programa", "version", "nombre", "nivel"]].drop_duplicates()
    df_programas["tiempo_duracion"] = 0
    df_programas["estado"] = 1
    df_programas["url_pdf"] = ""

    print(df_programas.head())

    df_centros = df[["cod_centro", "nombre_centro", "cod_regional", "nombre_regional"]].drop_duplicates()

    print(df_centros.head())


    resultados = insertar_datos_en_bd(db, df_programas, df_centros)
    return resultados