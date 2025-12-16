from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from typing import List
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from io import BytesIO
from app.crud.cargar_archivos_catalogo import insertar_datos_en_bd, insertar_municipios, insertar_catalogo_programas
from app.crud import catalogo as crud_catalogo
from app.schemas.catalogo import RetornoCatalogo
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
            # dayfirst=True para interpretar dd/mm/yyyy correctamente (común en Colombia)
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date

    # Elimina '' y NaN/NaT, solo permite objetos date o None en columnas de fecha
    for col in ["fecha_registro", "fecha_activo", "fecha_resolucion"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x if pd.notnull(x) and x != '' else None)

    # Derivar el campo estado
    df['estado'] = df['fecha_activo'].notnull()

    # Reemplazar campos vacíos con "N/A" por defecto
    campos_n_a = ['edad_min_requerida', 'grado_min_requerido', 'descripcion_req', 'resolucion']
    for col in campos_n_a:
        if col in df.columns:
            df[col] = df[col].fillna('N/A').apply(lambda x: 'N/A' if (isinstance(x, str) and x.strip() == '') else x)

    # Campos a insertar
    final_fields = [
        "cod_programa", "PRF_version", "cod_version", "tipo_formacion", "nombre_programa",
        "nivel_formacion", "duracion_maxima", "dur_etapa_lectiva", "dur_etapa_productiva",
        "fecha_registro", "fecha_activo", "edad_min_requerida", "grado_min_requerido", "descripcion_req",
        "resolucion", "fecha_resolucion", "apoyo_fic", "creditos", "alamedida", "linea_tecnologica",
        "red_tecnologica", "red_conocimiento", "modalidad", "apuestas_prioritarias", "fic", "tipo_permiso",
        "multiple_inscripcion", "indice", "ocupacion", "estado"
    ]

    df_programas = df[final_fields].drop_duplicates()

   # Rellenar todos los campos vacíos con "N/A" (excepto fechas)
    for col in df_programas.columns:
        if col not in ["fecha_registro", "fecha_activo", "fecha_resolucion"]:
            df_programas[col] = df_programas[col].fillna("N/A").apply(lambda x: "N/A" if (isinstance(x, str) and x.strip() == "") else x)

    resultados = insertar_catalogo_programas(db, df_programas)
    return resultados


@router.post("/upload-excel-catalogo/")
async def upload_excel_catalogo(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    contents = await file.read()
    try:
        df = pd.read_excel(BytesIO(contents), engine="openpyxl", dtype=str)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo leer el archivo Excel: {exc}") from exc

    if df is None or df.empty:
        raise HTTPException(status_code=400, detail="El archivo no contiene datos válidos")

    # Normalizar nombres de columnas para facilitar el mapeo
    df.columns = df.columns.astype(str).str.strip()
    columnas_disponibles = {col.upper(): col for col in df.columns}

    alias_columnas = {
        "COD_CATALOGO": "cod_catalogo",
        "CODIGO_CATALOGO": "cod_catalogo",
        "CODIGO": "cod_catalogo",
        "CODCATALOGO": "cod_catalogo",
        "NOMBRE_CATALOGO": "nombre_catalogo",
        "NOMBRE": "nombre_catalogo",
        "DESCRIPCION": "descripcion",
        "DESCRIPCIÓN": "descripcion",
        "ESTADO": "estado",
        "COD_MUNICIPIO": "cod_municipio",
        "CODIGO_MUNICIPIO": "cod_municipio",
        "ID_MUNICIPIO": "cod_municipio",
        "MUNICIPIO": "nombre_municipio",
        "NOMBRE_MUNICIPIO": "nombre_municipio"
    }

    renombres = {}
    usados = set()
    for alias, destino in alias_columnas.items():
        alias_upper = alias.upper()
        if alias_upper in columnas_disponibles and destino not in usados:
            renombres[columnas_disponibles[alias_upper]] = destino
            usados.add(destino)

    df = df.rename(columns=renombres)

    def normalizar_estado(valor):
        if pd.isna(valor):
            return True
        val = str(valor).strip().lower()
        if val in ("1", "true", "si", "sí", "activo"):
            return True
        if val in ("0", "false", "no", "inactivo"):
            return False
        return True

    if "estado" in df.columns:
        df["estado"] = df["estado"].apply(normalizar_estado)

    resultados = {}

    columnas_catalogo = [col for col in ["cod_catalogo", "nombre_catalogo", "descripcion", "estado"] if col in df.columns]
    if {"cod_catalogo", "nombre_catalogo"}.issubset(set(columnas_catalogo)):
        df_catalogos = df[columnas_catalogo].copy()
        df_catalogos = df_catalogos.dropna(subset=["cod_catalogo", "nombre_catalogo"])
        df_catalogos["cod_catalogo"] = df_catalogos["cod_catalogo"].astype(str).str.strip()
        df_catalogos["nombre_catalogo"] = df_catalogos["nombre_catalogo"].astype(str).str.strip()
        df_catalogos = df_catalogos[df_catalogos["cod_catalogo"] != ""]
        df_catalogos = df_catalogos.drop_duplicates(subset=["cod_catalogo"])

        if not df_catalogos.empty:
            resultados["catalogo"] = insertar_datos_en_bd(db, df_catalogos)
        else:
            resultados["catalogo"] = {"mensaje": "Sin registros de catálogo válidos"}
    else:
        resultados["catalogo"] = {"mensaje": "No se encontraron columnas de catálogo requeridas"}

    if {"cod_municipio", "nombre_municipio"}.issubset(df.columns):
        df_municipios = df[["cod_municipio", "nombre_municipio"]].copy()
        df_municipios = df_municipios.dropna(subset=["cod_municipio"])
        df_municipios["cod_municipio"] = df_municipios["cod_municipio"].astype(str).str.strip()
        df_municipios["nombre_municipio"] = df_municipios["nombre_municipio"].astype(str).str.strip()
        df_municipios = df_municipios[df_municipios["cod_municipio"] != ""]
        df_municipios = df_municipios.drop_duplicates(subset=["cod_municipio"])

        if not df_municipios.empty:
            existentes = db.execute(text("SELECT cod_municipio FROM municipios")).scalars().all()
            existentes = set(str(cod).strip() for cod in existentes if cod is not None)
            df_municipios = df_municipios[~df_municipios["cod_municipio"].isin(existentes)]

            if not df_municipios.empty:
                resultados["municipios"] = insertar_municipios(db, df_municipios)
            else:
                resultados["municipios"] = {"mensaje": "Todos los municipios ya estaban registrados"}
        else:
            resultados["municipios"] = {"mensaje": "Sin registros de municipios válidos"}
    else:
        resultados["municipios"] = {"mensaje": "No se encontraron columnas de municipios"}

    return resultados

# Endpoints GET para obtener datos de catálogo
@router.get("/obtener-todos", response_model=List[RetornoCatalogo])
def obtener_todos_catalogos(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Obtiene todos los catálogos"""
    return crud_catalogo.get_all_catalogos(db, skip, limit)


@router.get("/{id_catalogo}", response_model=RetornoCatalogo)
def obtener_catalogo_por_id(id_catalogo: int, db: Session = Depends(get_db)):
    """Obtiene un catálogo por ID"""
    catalogo = crud_catalogo.get_catalogo_by_id(db, id_catalogo)
    if not catalogo:
        raise HTTPException(status_code=404, detail="Catálogo no encontrado")
    return catalogo


@router.get("/codigo/{cod_catalogo}", response_model=RetornoCatalogo)
def obtener_catalogo_por_codigo(cod_catalogo: str, db: Session = Depends(get_db)):
    """Obtiene un catálogo por código"""
    catalogo = crud_catalogo.get_catalogo_by_codigo(db, cod_catalogo)
    if not catalogo:
        raise HTTPException(status_code=404, detail="Catálogo no encontrado")
    return catalogo

    if not catalogo:
        raise HTTPException(status_code=404, detail="Catálogo no encontrado")
    return catalogo