from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_datos_en_bd, insertar_dim_tiempo
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
    usecols=[
        "PRF_CODIGO",
        "PRF_VERSION",
        "COD_VER",
        "TIPO DE FORMACION",
        "PRF_DENOMINACION",
        "NIVEL DE FORMACION",
        "PRF_DURACION_MAXIMA",
        "PRF_DUR_ETAPA_LECTIVA",
        "PRF_DUR_ETAPA_PROD",
        "PRF_FCH_REGISTRO",
        "Fecha Activo (En Ejecución)",
        "PRF_EDAD_MIN_REQUERIDA",
        "PRF_GRADO_MIN_REQUERIDO",
        "PRF_DESCRIPCION_REQUISITO",
        "PRF_RESOLUCION",
        "PRF_FECHA_RESOLUCION",
        "PRF_APOYO_FIC",
        "PRF_CREDITOS",
        "PRF_ALAMEDIDA",
        "Linea Tecnológica ",
        "Red Tecnológica",
        "Red de Conocimiento",
        "Modalidad",
        "APUESTAS PRIORITARIAS",
        "FIC",
        "TIPO PERMISO",
        "Multiple Inscripcion",
        "Indice",
        "Ocupación"
    ],
    dtype=str
)


    df = df.rename(columns={
    "PRF_CODIGO": "cod_programa",
    "PRF_VERSION": "version",
    "COD_VER": "codigo_version",
    "TIPO DE FORMACION": "tipo_formacion",
    "PRF_DENOMINACION": "nombre_programa",
    "NIVEL DE FORMACION": "nivel",
    "PRF_DURACION_MAXIMA": "duracion_max",
    "PRF_DUR_ETAPA_LECTIVA": "duracion_lectiva",
    "PRF_DUR_ETAPA_PROD": "duracion_productiva",
    "PRF_FCH_REGISTRO": "fecha_registro",
    "Fecha Activo (En Ejecución)": "fecha_activo",
    "PRF_EDAD_MIN_REQUERIDA": "edad_minima",
    "PRF_GRADO_MIN_REQUERIDO": "grado_minimo",
    "PRF_DESCRIPCION_REQUISITO": "requisitos",
    "PRF_RESOLUCION": "resolucion",
    "PRF_FECHA_RESOLUCION": "fecha_resolucion",
    "PRF_APOYO_FIC": "apoyo_fic",
    "PRF_CREDITOS": "creditos",
    "PRF_ALAMEDIDA": "a_la_medida",
    "Linea Tecnológica ": "linea_tecnologica",
    "Red Tecnológica": "red_tecnologica",
    "Red de Conocimiento": "red_conocimiento",
    "Modalidad": "modalidad",
    "APUESTAS PRIORITARIAS": "apuestas_prioritarias",
    "FIC": "fic",
    "TIPO PERMISO": "tipo_permiso",
    "Multiple Inscripcion": "multiple_inscripcion",
    "Indice": "indice",
    "Ocupación": "ocupacion"
    })


    required_fields = [
        "cod_programa",
        "version",
        "nombre_programa",
        "nivel",
        "tipo_formacion",
        "duracion_max",
        "duracion_lectiva",
        "duracion_productiva",
        "fecha_registro",
        "resolucion",
        "fecha_resolucion",
        "linea_tecnologica",
        "red_tecnologica",
        "red_conocimiento",
        "modalidad"
    ]
    df = df.dropna(subset=required_fields)

    for col in ["cod_programa", "version", "nombre_programa", "nivel", "tipo_formacion", "duracion_max", "duracion_lectiva", "duracion_productiva", "fecha_registro", "resolucion", "fecha_resolucion", "linea_tecnologica", "red_tecnologica", "red_conocimiento", "modalidad"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["fecha_registro"] = pd.to_datetime(df["fecha_registro"], errors="coerce").dt.date
    df["fecha_resolucion"] = pd.to_datetime(df["fecha_resolucion"], errors="coerce").dt.date

    # --- CORRECCIÓN: limpiar NaN de los DF antes de insertarlos ---
    df_programas = df[["cod_programa", "version", "nombre_programa", "nivel", "tipo_formacion", "duracion_max", "duracion_lectiva", "duracion_productiva", "fecha_registro", "resolucion", "fecha_resolucion", "linea_tecnologica", "red_tecnologica", "red_conocimiento", "modalidad"]].drop_duplicates().fillna("")
    df_programas["tiempo_duracion"] = df_programas["duracion_max"] - df_programas["duracion_lectiva"] - df_programas["duracion_productiva"]
    df_programas["tiempo_duracion"] = df_programas["tiempo_duracion"].astype(int)
    df_programas["apoyo_fic"] = df_programas["apoyo_fic"].astype(int)
    df_programas["estado"] = 1
    df_programas["url_pdf"] = ""

    resultados = insertar_datos_en_bd(db, df_programas)
    return resultados

@router.post("/upload-excel-dim-tiempo/")
async def upload_excel(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    contents = await file.read()
    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        usecols=[
            "fecha", "anio", "mes", "nombre_mes", "dia"
        ],
        dtype=str
    )

    df = df.rename(columns={
        "fecha": "fecha",
        "anio": "anio",
        "mes": "mes",
        "nombre_mes": "nombre_mes",
        "dia": "dia",
    })
    
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date

    dim_tiempo = df[["fecha", "anio", "mes", "nombre_mes", "dia"]].drop_duplicates().fillna("")
    
    resultados = insertar_dim_tiempo(db, dim_tiempo)
    return resultados
