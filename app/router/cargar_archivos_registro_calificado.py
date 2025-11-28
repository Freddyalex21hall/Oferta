from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
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

    # Normalizar nombres de columnas
    df.columns = df.columns.astype(str).str.strip()

    # Mapeo flexible similar a cargar_archivos_historico
    columnas_mapeo = {
        "CODIGO_PROGRAMA": "cod_programa",
        "COD_PROGRAMA": "cod_programa",
        "COD_PROGRAM": "cod_programa",
        "CODIGO": "cod_programa",
        "COD DEL PROGRAMA" : "cod_programa",

        "TIPO_TRAMITE" : "tipo_tramite",
        "TRAMITE": "tipo_tramite",
        "TIPO DE TRÁMITE" : "tipo_tramite",

        "FECHA_RADICADO": "fecha_radicado",
        "FECHA_RAD": "fecha_radicado",
        "FECHA RADICADO" : "fecha_radicado",

        "NUMERO_RESOLUCION": "numero_resolucion",
        "NUM_RESOLUCION": "numero_resolucion",
        "RESOLUCION": "numero_resolucion",
        "NUMERO DE RESOLUCION" : "numero_resolucion",

        "FECHA_RESOLUCION": "fecha_resolucion",
        "FECHA_VENCIMIENTO": "fecha_vencimiento",
        "Fecha de vencimiento": "fecha_vencimiento",

        "VIGENCIA": "vigencia",
        "VIGENCIA RC" : "vigencia",
        "MODALIDAD": "modalidad",
        "CLASIFICACION": "clasificacion",
        "ESTADO_CATALOGO": "estado_catalogo",
        "ESTADO": "estado_catalogo",
    }

    columnas_disponibles_upper = {col.upper().strip(): col for col in df.columns}
    columnas_renombrar = {}
    columnas_usadas = set()

    for col_excel, col_bd in columnas_mapeo.items():
        key = col_excel.upper().strip()
        found = None
        if key in columnas_disponibles_upper:
            found = columnas_disponibles_upper[key]
        else:
            for col_up, col_orig in columnas_disponibles_upper.items():
                if key in col_up or col_up in key:
                    if col_bd not in columnas_usadas:
                        found = col_orig
                        break
        if found and col_bd not in columnas_usadas:
            columnas_renombrar[found] = col_bd
            columnas_usadas.add(col_bd)
            columnas_disponibles_upper.pop(found.upper().strip(), None)

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
