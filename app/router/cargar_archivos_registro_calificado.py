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
        "COD DEL PROGRAMA": "cod_programa",
        "codigo": "cod_programa",
        "cod": "cod_programa",

        "TIPO DE TRÁMITE": "tipo_tramite",
        "tramite": "tipo_tramite",

        "FECHA RADICADO": "fecha_radicado",
        "fecha rad": "fecha_radicado",

        "NUMERO DE RESOLUCION": "numero_resolucion",
        "num resolucion": "numero_resolucion",
        "resolucion": "numero_resolucion",

        "FECHA DE RESOLUCION": "fecha_resolucion",
        "Fecha de vencimiento": "fecha_vencimiento",

        "VIGENCIA RC": "vigencia",
        "MODALIDAD": "modalidad",
        "CLASIFICACIÓN PARA TRÁMITE": "clasificacion",
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
    # Fallback heurístico: buscar columna cuyo nombre normalizado contenga 'cod' y 'program'
    if "cod_programa" not in df.columns:
        for norm_col, orig_col in {normalize_colname(c): c for c in df.columns}.items():
            if "cod" in norm_col and ("program" in norm_col or "programa" in norm_col or "codigo" in norm_col):
                df = df.rename(columns={orig_col: "cod_programa"})
                break

    # Si aún no se encuentra, intentar localizar fila de encabezado en otras filas
    header_found = False
    header_row_used = None
    if "cod_programa" not in df.columns:
        try:
            raw = pd.read_excel(BytesIO(contents), engine="openpyxl", header=None, dtype=str)
            max_check_rows = min(25, raw.shape[0])
            # Crear conjunto de alias normalizados a buscar
            norm_aliases = {normalize_colname(a) for a in columnas_mapeo.keys()}

            for r in range(max_check_rows):
                candidate = raw.iloc[r].fillna("").astype(str).tolist()
                candidate_norm = [normalize_colname(c) for c in candidate]
                # Si aparece algún alias en esta fila, usarla como encabezado
                if any(c in norm_aliases for c in candidate_norm):
                    # reconstruir df usando esta fila como header
                    try:
                        df = pd.read_excel(BytesIO(contents), engine="openpyxl", header=r, dtype=str)
                        header_found = True
                        header_row_used = r
                        break
                    except Exception:
                        continue

            # Si no se encontró encabezado, intentar localizar la columna por contenido de celdas
            if not header_found:
                max_check_cells = min(50, raw.shape[0])
                found_col_idx = None
                for col in range(raw.shape[1]):
                    for rowi in range(max_check_cells):
                        cell = raw.iat[rowi, col]
                        if pd.isna(cell):
                            continue
                        norm_cell = normalize_colname(cell)
                        if "cod" in norm_cell and ("program" in norm_cell or "programa" in norm_cell or "codigo" in norm_cell):
                            found_col_idx = col
                            break
                    if found_col_idx is not None:
                        break

                if found_col_idx is not None:
                    # crear columna cod_programa a partir de esa columna y mantener el df actual
                    try:
                        raw_full = pd.read_excel(BytesIO(contents), engine="openpyxl", header=None, dtype=str)
                        df_temp = raw_full.iloc[:, found_col_idx].astype(str).str.strip()
                        df["cod_programa"] = df_temp.values[: len(df)]
                        header_row_used = f"col_index_{found_col_idx}"
                        header_found = True
                    except Exception:
                        pass
        except Exception:
            pass

    # Último intento: más agresivo, eliminar todo lo que no sea alfanumérico
    if "cod_programa" not in df.columns:
        for orig_col in list(df.columns):
            try:
                raw = str(orig_col).casefold()
                # eliminar todo menos letras y numeros
                raw_slim = re.sub(r"[^0-9a-z]", "", raw)
                if "cod" in raw_slim and ("program" in raw_slim or "programa" in raw_slim or "codigo" in raw_slim):
                    df = df.rename(columns={orig_col: "cod_programa"})
                    break
            except Exception:
                continue
    if "cod_programa" not in df.columns:
        # Preparar diagnóstico para facilitar debugging
        detected = list(df.columns)
        normalized = {c: normalize_colname(c) for c in detected}
        attempted = {alias: normalize_colname(alias) for alias in columnas_mapeo.keys()}
        extra = {}
        if 'header_row_used' in locals() and header_row_used is not None:
            extra['header_row_used'] = header_row_used
        else:
            extra['header_row_used'] = None

        return {
            "exitoso": False,
            "mensaje": "No se encontró columna para 'cod_programa' en el archivo",
            "columnas_detectadas": detected,
            "columnas_normalizadas": normalized,
            "mapeo_intentado": attempted,
            "info_extra": extra,
        }

    # Filtrar filas sin cod_programa
    df = df.dropna(subset=["cod_programa"])
    if df.empty:
        return {"exitoso": False, "mensaje": "No hay filas con 'cod_programa' válido"}

    # Normalizar tipos
    # Mantener cod_programa como texto (la tabla en la DB es VARCHAR)
    df["cod_programa"] = df["cod_programa"].astype(str).str.strip()
    if "numero_resolucion" in df.columns:
        df["numero_resolucion"] = pd.to_numeric(df["numero_resolucion"], errors="coerce").astype("Int64")

    # Convertir fechas
    for date_col in ["fecha_radicado", "fecha_resolucion", "fecha_vencimiento"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # Llamar al CRUD para insertar/actualizar
    resultados = insertar_registro_calificado_en_bd(db, df)
    return resultados
