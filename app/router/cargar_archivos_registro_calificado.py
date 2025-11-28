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
    contents = await file.read()

    # Intentar leer con diferentes skiprows
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

    # Normalizar nombres de columnas
    df.columns = df.columns.astype(str).str.strip()

    # -----------------------------------------------------
    # 1️⃣ NORMALIZADOR
    # -----------------------------------------------------
    def normalize_colname(s: str) -> str:
        if s is None:
            return ""
        s = unicodedata.normalize("NFD", s)
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
        s = s.lower()
        s = re.sub(r"[_\.\-]+", " ", s)
        s = re.sub(r"[^a-z0-9\s]", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # -----------------------------------------------------
    # 2️⃣ NUEVO MAPEO — LIMPIO, SIMPLE y EFECTIVO
    # -----------------------------------------------------
    mapeo_flexible = {
        # cod_programa
        "codigo del programa": "cod_programa",
        "codigo programa": "cod_programa",
        "cod del programa": "cod_programa",
        "cod programa": "cod_programa",
        "codigo": "cod_programa",
        "cod": "cod_programa",

        # tipo_tramite
        "tipo de tramite": "tipo_tramite",
        "tramite": "tipo_tramite",

        # fecha_radicado
        "fecha radicado": "fecha_radicado",
        "fecha rad": "fecha_radicado",

        # numero_resolucion
        "numero de resolucion": "numero_resolucion",
        "num resolucion": "numero_resolucion",
        "resolucion": "numero_resolucion",

        # fecha_resolucion
        "fecha de resolucion": "fecha_resolucion",

        # fecha_vencimiento
        "fecha de vencimiento": "fecha_vencimiento",

        # vigencia
        "vigencia rc": "vigencia",
        "vigencia": "vigencia",

        # modalidad
        "modalidad": "modalidad",

        # clasificacion
        "clasificacion para tramite": "clasificacion",
        "clasificacion": "clasificacion",

        # estados
        "estado catalogo": "estado_catalogo",
        "estado": "estado_catalogo",
    }

    # Normalizar columnas existentes
    columnas_norm = {normalize_colname(c): c for c in df.columns}

    columnas_renombrar = {}
    usados = set()

    for alias, target in mapeo_flexible.items():
        norm_alias = normalize_colname(alias)

        # coincidencia exacta
        if norm_alias in columnas_norm:
            columnas_renombrar[columnas_norm[norm_alias]] = target
            usados.add(target)
            continue

        # coincidencia parcial flexible
        for norm_col, original in columnas_norm.items():
            if norm_alias in norm_col or norm_col in norm_alias:
                if target not in usados:
                    columnas_renombrar[original] = target
                    usados.add(target)
                    break

    df = df.rename(columns=columnas_renombrar)

    # -----------------------------------------------------
    # 3️⃣ DETECCIÓN FINAL DE cod_programa (mejorada)
    # -----------------------------------------------------
    if "cod_programa" not in df.columns:
        for col in df.columns:
            norm = normalize_colname(col)
            if "cod" in norm and ("program" in norm or "programa" in norm):
                df = df.rename(columns={col: "cod_programa"})
                break

    if "cod_programa" not in df.columns:
        return {
            "exitoso": False,
            "mensaje": "No se pudo encontrar la columna 'cod_programa'.",
            "columnas_detectadas": list(df.columns),
        }

    # -----------------------------------------------------
    # 4️⃣ LIMPIEZA DE DATOS
    # -----------------------------------------------------
    df = df.dropna(subset=["cod_programa"])

    df["cod_programa"] = df["cod_programa"].astype(str).str.strip()

    if "numero_resolucion" in df.columns:
        df["numero_resolucion"] = pd.to_numeric(df["numero_resolucion"], errors="coerce").astype("Int64")

    for date_col in ["fecha_radicado", "fecha_resolucion", "fecha_vencimiento"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # -----------------------------------------------------
    # 5️⃣ GUARDAR EN LA BD
    # -----------------------------------------------------
    resultados = insertar_registro_calificado_en_bd(db, df)
    return resultados
