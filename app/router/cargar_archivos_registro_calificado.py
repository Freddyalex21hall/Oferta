from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
import unicodedata
import re
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos_registro_calificado import insertar_registro_calificado_en_bd
from core.database import get_db
from app.router.dependencies import get_current_user
from app.schemas.usuarios import RetornoUsuario

router = APIRouter()


@router.post("/upload-excel-registro-calificado/")
async def upload_excel_registro_calificado(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user),
):
    """
    Endpoint para cargar registros calificados desde un archivo Excel.
    Se intenta leer el Excel con diferentes `skiprows` para soportar variantes.
    """
    contents = await file.read()

    # Intentar leer con diferentes skiprows (flexible)
    df = None
    for skiprows in [0, 1, 2, 3, 4, 5]:
        try:
            df_test = pd.read_excel(BytesIO(contents), engine="openpyxl", skiprows=skiprows, nrows=0)
            if len(df_test.columns) > 0:
                df = pd.read_excel(BytesIO(contents), engine="openpyxl", skiprows=skiprows, dtype=str)
                break
        except Exception:
            continue

    if df is None:
        try:
            df = pd.read_excel(BytesIO(contents), engine="openpyxl", dtype=str)
        except Exception as e:
            return {"exitoso": False, "mensaje": f"No se pudo leer el archivo Excel: {str(e)}"}

    if df is None or df.empty:
        return {"exitoso": False, "mensaje": "Archivo vacío o sin datos"}

    # Normalizar nombres de columnas (remover acentos/puntuación y normalizar espacios)
    df.columns = df.columns.astype(str).str.strip()

    def normalize_colname(s: str) -> str:
        if s is None:
            return ""
        s = str(s)
        # NFD + remove diacritics
        s = unicodedata.normalize('NFD', s)
        s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
        # lower, remove punctuation, replace underscores/dots with space
        s = s.lower()
        s = re.sub(r"[_\.\-]+", " ", s)
        # remove any non alnum or space
        s = re.sub(r"[^a-z0-9\s]", "", s)
        # collapse spaces
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # Mapeo flexible con nombres normalizados
    columnas_mapeo = {
        "codigo programa": "cod_programa",
        "cod programa": "cod_programa",
        "cod del programa": "cod_programa",
        "codigo": "cod_programa",
        "cod": "cod_programa",

        "tipo tramite": "tipo_tramite",
        "tramite": "tipo_tramite",

        "fecha radicado": "fecha_radicado",
        "fecha rad": "fecha_radicado",

        "numero resolucion": "numero_resolucion",
        "num resolucion": "numero_resolucion",
        "resolucion": "numero_resolucion",

        "fecha resolucion": "fecha_resolucion",
        "fecha vencimiento": "fecha_vencimiento",

        "vigencia": "vigencia",
        "modalidad": "modalidad",
        "clasificacion": "clasificacion",
        "estado catalogo": "estado_catalogo",
        "estado": "estado_catalogo",
    }

    # mapa normalizado de columnas disponibles -> original
    columnas_disponibles_norm = {normalize_colname(col): col for col in df.columns}
    columnas_renombrar = {}
    columnas_usadas = set()

    for alias, target in columnas_mapeo.items():
        norm_alias = normalize_colname(alias)
        found = None
        # exact match
        if norm_alias in columnas_disponibles_norm:
            found = columnas_disponibles_norm[norm_alias]
        else:
            # partial contains match
            for norm_col, orig_col in columnas_disponibles_norm.items():
                if norm_alias in norm_col or norm_col in norm_alias:
                    if target not in columnas_usadas:
                        found = orig_col
                        break
        if found and target not in columnas_usadas:
            columnas_renombrar[found] = target
            columnas_usadas.add(target)
            # remove so we don't reuse same original
            columnas_disponibles_norm.pop(normalize_colname(found), None)

    df = df.rename(columns=columnas_renombrar)

    # Validar cod_programa
    if "cod_programa" not in df.columns:
        return {"exitoso": False, "mensaje": "No se encontró columna para 'cod_programa' en el archivo"}

    # Filtrar filas sin cod_programa
    df = df.dropna(subset=["cod_programa"])
    if df.empty:
        return {"exitoso": False, "mensaje": "No hay filas con 'cod_programa' válido"}

    # Normalizar tipos
    df["cod_programa"] = pd.to_numeric(df["cod_programa"], errors="coerce").astype("Int64")
    if "numero_resolucion" in df.columns:
        df["numero_resolucion"] = pd.to_numeric(df["numero_resolucion"], errors="coerce").astype("Int64")

    # Convertir fechas
    for date_col in ["fecha_radicado", "fecha_resolucion", "fecha_vencimiento"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # Llamar al CRUD para insertar/actualizar
    resultados = insertar_registro_calificado_en_bd(db, df)
    return resultados
