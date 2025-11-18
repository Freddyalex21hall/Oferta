
from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_datos_en_bd, insertar_estado_normas
from core.database import get_db

router = APIRouter()


@router.post("/upload-excel-pe04/")
async def upload_excel(
    file: UploadFile = File(...),
    tipo: str = "grupos",  # 'grupos' (por defecto) o 'normas'
    db: Session = Depends(get_db)
):
    contents = await file.read()

    if tipo == "normas":
        usecols = [
            "COD_PROGRAMA", "COD_VERSION", "FECHA_ELABORACION", "ANIO", "RED_CONOCIMIENTO",
            "NOMBRE_NCL", "COD_NCL", "NCL_VERSION", "NORMA_CORTE_NOVIEMBRE",
            "VERSION", "NORMA_VERSION", "MESA_SECTORIAL", "TIPO_NORMA",
            "OBSERVACION", "FECHA_REVISION", "TIPO_COMPETENCIA", "VIGENCIA", "FECHA_INDICE"
        ]
        skiprows = 0
    else:
        usecols = ["IDENTIFICADOR_FICHA", "CODIGO_CENTRO", "CODIGO_PROGRAMA", "VERSION_PROGRAMA", "NOMBRE_PROGRAMA_FORMACION", "ESTADO_CURSO", "NIVEL_FORMACION", "NOMBRE_JORNADA", "FECHA_INICIO_FICHA", "FECHA_TERMINACION_FICHA", "ETAPA_FICHA", "MODALIDAD_FORMACION", "NOMBRE_RESPONSABLE", "NOMBRE_EMPRESA", "NOMBRE_MUNICIPIO_CURSO", "NOMBRE_PROGRAMA_ESPECIAL"]
        skiprows = 4

    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        skiprows=skiprows,
        usecols=usecols,
        dtype=str
    )
    print(df.head())
    print(df.columns)
    print(df.dtypes)

    # Renombrar columnas según el tipo de carga
    if tipo == "normas":
        df = df.rename(columns={
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
        })
    else:
        df = df.rename(columns={
            "IDENTIFICADOR_FICHA": "cod_ficha",
            "CODIGO_CENTRO": "cod_centro",
            "CODIGO_PROGRAMA": "cod_programa",
            "VERSION_PROGRAMA": "la_version",
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
            "NOMBRE_PROGRAMA_ESPECIAL": "nombre_programa_especial",
            "NOMBRE_PROGRAMA_FORMACION": "nombre"
        })

    print(df.head())

    # si quieren que funcione en todos los centros de pais 
    # crear codigo para llenar regionales centros y eliminar la siguiente linea.
    # df = df[df["cod_centro"] == '9121']

    print(df.head())

    # Si es carga de normas, llamar al CRUD específico
    if tipo == "normas":
        resultados = insertar_estado_normas(db, df)
        return resultados

    # Eliminar filas con valores faltantes en campos obligatorios
    required_fields = [
        "cod_ficha", "cod_centro", "cod_programa", "la_version", "nombre", 
        "fecha_inicio", "fecha_fin", "etapa", "responsable", "nombre_municipio"
    ]
    df = df.dropna(subset=required_fields)

    # Convertir columnas a tipo numérico
    for col in ["cod_ficha", "cod_programa", "la_version", "cod_centro"]:
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

    # Crear DataFrame de programas únicos (sin columnas no usadas por el CRUD)
    df_programas = df[["cod_programa", "la_version", "nombre"]].drop_duplicates()

    print(df_programas.head())

    # Eliminar la columna nombre del df.
    df = df.drop('nombre', axis=1)
    print(df.head())

    resultados = insertar_datos_en_bd(db, df_programas, df)
    return resultados