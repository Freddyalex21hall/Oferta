from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos import insertar_datos_en_bd, insertar_municipios, insertar_catalogo_programas
from core.database import get_db

router = APIRouter()

@router.post("/upload-excel-catalogo-programas/")
async def upload_excel(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    contents = await file.read()
    usecols = [
        "PRF_CODIGO", "PRF_VERSION", "COD_VER", "TIPO_FORMACION", "PRF_DENOMINACION", 
        "NIVEL_FORMACION", "PRF_DURACION_MAXIMA", "PRF_DUR_ETAPA_LECTIVA", "PRF_DUR_ETAPA_PROD",
        "PRF_FCH_REGISTRO", "FECHA_ACTIVO", "PRF_EDAD_MIN_REQUERIDA", "PRF_GRADO_MIN_REQUERIDO", 
        "PRF_DESCRIPCION_REQUISITO", "PRF_RESOLUCION", "PRF_FECHA_RESOLUCION", "PRF_APOYO_FIC",
        "PRF_CREDITOS", "PRF_ALAMEDIDA", "LINEA_TECNOLOGICA", "RED_TECNOLOGICA", "RED_CONOCIMIENTO",
        "MODALIDAD", "APUESTAS_PRIORITARIAS", "FIC", "TIPO_PERMISO", "MULTIPLE_INSCRIPCION", "INDICE", "OCUPACION"
    ]
    df = pd.read_excel(BytesIO(contents), engine="openpyxl", usecols=usecols, dtype=str)

    df = df.rename(columns={
        "PRF_CODIGO": "cod_programa",
        "PRF_VERSION": "PRF_version",
        "COD_VER": "cod_version",
        "TIPO_FORMACION": "tipo_formacion",
        "PRF_DENOMINACION": "nombre_programa",
        "NIVEL_FORMACION": "nivel_formacion",
        "PRF_DURACION_MAXIMA": "duracion_maxima",
        "PRF_DUR_ETAPA_LECTIVA": "dur_etapa_lectiva",
        "PRF_DUR_ETAPA_PROD": "dur_etapa_productiva",
        "PRF_FCH_REGISTRO": "fecha_registro",
        "FECHA_ACTIVO": "fecha_activo",
        "PRF_EDAD_MIN_REQUERIDA": "edad_min_requerida",
        "PRF_GRADO_MIN_REQUERIDO": "grado_min_requerido",
        "PRF_DESCRIPCION_REQUISITO": "descripcion_req",
        "PRF_RESOLUCION": "resolucion",
        "PRF_FECHA_RESOLUCION": "fecha_resolucion",
        "PRF_APOYO_FIC": "apoyo_fic",
        "PRF_CREDITOS": "creditos",
        "PRF_ALAMEDIDA": "alamedida",
        "LINEA_TECNOLOGICA": "linea_tecnologica",
        "RED_TECNOLOGICA": "red_tecnologica",
        "RED_CONOCIMIENTO": "red_conocimiento",
        "MODALIDAD": "modalidad",
        "APUESTAS_PRIORITARIAS": "apuestas_prioritarias",
        "FIC": "fic",
        "TIPO_PERMISO": "tipo_permiso",
        "MULTIPLE_INSCRIPCION": "multiple_inscripcion",
        "INDICE": "indice",
        "OCUPACION": "ocupacion"
    })

    # Campos obligatorios para insertar
    required_fields = ["cod_programa", "nombre_programa", "tipo_formacion", "nivel_formacion", "duracion_maxima", "fecha_registro"]
    df = df.dropna(subset=required_fields)

    # Conversión de tipos numéricos
    for col in ["PRF_version", "duracion_maxima", "dur_etapa_lectiva", "dur_etapa_productiva", "creditos"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Convertir fechas a tipo date
    for col in ["fecha_registro", "fecha_activo", "fecha_resolucion"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Elimina '' y NaN/NaT, solo permite objetos date o None en columnas de fecha
    for col in ["fecha_registro", "fecha_activo", "fecha_resolucion"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x if pd.notnull(x) and x != '' else None)

    # Derivar el campo estado
    df['estado'] = df['fecha_activo'].notnull()

    # Campos a insertar
    final_fields = [
        "cod_programa", "PRF_version", "cod_version", "tipo_formacion", "nombre_programa",
        "nivel_formacion", "duracion_maxima", "dur_etapa_lectiva", "dur_etapa_productiva",
        "fecha_registro", "fecha_activo", "edad_min_requerida", "grado_min_requerido", "descripcion_req",
        "resolucion", "fecha_resolucion", "apoyo_fic", "creditos", "alamedida", "linea_tecnologica",
        "red_tecnologica", "red_conocimiento", "modalidad", "apuestas_prioritarias", "fic", "tipo_permiso",
        "multiple_inscripcion", "indice", "ocupacion", "estado"
    ]
    if "url_pdf" not in df.columns:
        df["url_pdf"] = ""
    final_fields.append("url_pdf")

    df_programas = df[final_fields].drop_duplicates()

    # Para los campos STRING (excepto fechas), puedes usar .fillna("") (opcional)
    for col in df_programas.columns:
        if col not in ["fecha_registro", "fecha_activo", "fecha_resolucion"]:
            df_programas[col] = df_programas[col].fillna("")

    resultados = insertar_catalogo_programas(db, df_programas)
    return resultados
