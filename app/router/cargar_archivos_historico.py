from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
import logging
from app.crud.cargar_archivos_historico import insertar_historico_completo_en_bd
from core.database import get_db
from app.router.dependencies import get_current_user
from app.schemas.usuarios import RetornoUsuario

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/upload-excel-historico/")
async def upload_excel_historico(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Endpoint para cargar datos históricos de aprendices por grupo desde un archivo Excel.
    Lee todas las columnas del archivo (grupos + histórico) y:
    - Si el grupo existe: solo actualiza el histórico
    - Si el grupo NO existe: crea el grupo completo y luego el histórico
    """
    
    contents = await file.read()
    
    df = None
    skip_rows_options = [0, 1, 2, 3, 4, 5]
    
    for skip_rows in skip_rows_options:
        try:
            df_test = pd.read_excel(
                BytesIO(contents),
                engine="openpyxl",
                skiprows=skip_rows,
                nrows=0
            )
            if len(df_test.columns) > 0:
                df = pd.read_excel(
                    BytesIO(contents),
                    engine="openpyxl",
                    skiprows=skip_rows,
                    dtype=str
                )
                print(f"Archivo leído correctamente con skiprows={skip_rows}")
                break
        except Exception as e:
            print(f"Error al leer con skiprows={skip_rows}: {str(e)}")
            continue
    
    if df is None:
        try:
            df = pd.read_excel(
                BytesIO(contents),
                engine="openpyxl",
                dtype=str
            )
            print("Archivo leído sin skiprows")
        except Exception as e:
            return {
                "registros_insertados": 0,
                "registros_actualizados": 0,
                "grupos_creados": 0,
                "total_errores": 1,
                "errores": [f"Error al leer el archivo Excel: {str(e)}"],
                "exitoso": False,
                "mensaje": f"No se pudo leer el archivo Excel. Error: {str(e)}"
            }
    
    if df is None or len(df) == 0:
        return {
            "registros_insertados": 0,
            "registros_actualizados": 0,
            "grupos_creados": 0,
            "total_errores": 1,
            "errores": ["El archivo Excel está vacío o no se pudo leer correctamente"],
            "exitoso": False,
            "mensaje": "El archivo Excel está vacío o no tiene datos válidos"
        }
    
    print("DataFrame original:")
    print(df.head())
    print("Columnas disponibles:", df.columns.tolist())
    print(df.dtypes)
    columnas_mapeo = {
        # Identificador principal
        "FICHA": "ficha",
        "IDENTIFICADOR_FICHA": "ficha",
        # Columnas de grupos - Centro
        "CODIGO": "cod_centro",  # Primer CODIGO suele ser centro
        "CODIGO_CENTRO": "cod_centro",
        "NOMBRE": "nombre_centro",  # Primer NOMBRE suele ser centro
        "NOMBRE_CENTRO": "nombre_centro",
        "CODIGO_REGIONAL": "cod_regional",
        "NOMBRE_REGIONAL": "nombre_regional",
        # Programa de formación
        "CODIGO_PROGRAMA": "cod_programa",
        "PROGRAMA_FORMACION": "nombre_programa",
        "VERSION": "version",
        "NIVEL": "nivel",
        "JORNADA": "jornada",
        "MODALIDAD": "modalidad",
        # Fechas y estado
        "FECHA_INICIO": "fecha_inicio",
        "FECHA_FIN": "fecha_fin",
        "ESTADO": "estado_curso",
        "ETAPA_FICHA": "etapa_ficha",
        # Municipio
        "ID_MUNI": "cod_municipio",
        "MUNICIPIO": "nombre_municipio",
        "CODIGO_MUNICIPIO": "cod_municipio",
        # Estrategia
        "FIC_MC": "cod_estrategia",
        "CODIGO_ESTRATEGIA": "cod_estrategia",
        # Responsable y empresa
        "NOMBRE_RESPONSABLE": "nombre_responsable",
        "RESPONSABLE": "nombre_responsable",
        "NOMBRE_EMPRESA": "nombre_empresa",
        "EMPRESA": "nombre_empresa",
        # Columnas de histórico
        "INSCRITOS": "num_aprendices_inscritos",
        "MATRICULADOS": "num_aprendices_matriculados",
        "EN_TRAINING": "num_aprendices_en_transito",
        "EN_TRANSITO": "num_aprendices_en_transito",
        "FORMACION": "num_aprendices_formacion",
        "INDUCCION": "num_aprendices_induccion",
        "CONDICION": "num_aprendices_condicionados",
        "CONDICIONADOS": "num_aprendices_condicionados",
        "APLAZADOS": "num_aprendices_aplazados",
        "RETIRO": "num_aprendices_retirado_voluntario",
        "RETIRADO_VOLUNTARIO": "num_aprendices_retirado_voluntario",
        "CANCELADOS": "num_aprendices_cancelados",
        "REPROBADO": "num_aprendices_reprobados",
        "REPROBADOS": "num_aprendices_reprobados",
        "NO_APROBADO": "num_aprendices_no_aptos",
        "NO_APTOS": "num_aprendices_no_aptos",
        "REINGRESO": "num_aprendices_reingresados",
        "REINGRESADOS": "num_aprendices_reingresados",
        "POR_CERTIFICAR": "num_aprendices_por_certificar",
        "CERTIFICADOS": "num_aprendices_certificados",
        "TRASLADADOS": "num_aprendices_trasladados"
    }
    
    df.columns = df.columns.astype(str).str.strip()
    
    columnas_renombrar = {}
    columnas_usadas = set()
    
    columnas_disponibles_upper = {col.upper().strip(): col for col in df.columns}
    
    for col_excel, col_bd in columnas_mapeo.items():
        col_excel_upper = col_excel.upper().strip()
        
        col_encontrada = None
        if col_excel_upper in columnas_disponibles_upper:
            col_encontrada = columnas_disponibles_upper[col_excel_upper]
        else:
            for col_upper, col_original in columnas_disponibles_upper.items():
                if col_excel_upper in col_upper or col_upper in col_excel_upper:
                    if col_bd not in columnas_usadas:
                        col_encontrada = col_original
                        break
        
        if col_encontrada and col_bd not in columnas_usadas:
            columnas_renombrar[col_encontrada] = col_bd
            columnas_usadas.add(col_bd)
            if col_encontrada.upper().strip() in columnas_disponibles_upper:
                del columnas_disponibles_upper[col_encontrada.upper().strip()]
    
    df = df.rename(columns=columnas_renombrar)
    
    if "ficha" not in df.columns:
        posibles_fichas = []
        for col in df.columns:
            col_upper = col.upper().strip()
            if any(keyword in col_upper for keyword in ["FICHA", "IDENTIFICADOR", "ID_GRUPO", "CODIGO_FICHA", "NUMERO_FICHA"]):
                posibles_fichas.append(col)
        
        if posibles_fichas:
            ficha_col = None
            for col in posibles_fichas:
                col_upper = col.upper().strip()
                if "FICHA" in col_upper and "IDENTIFICADOR" not in col_upper:
                    ficha_col = col
                    break
            if not ficha_col:
                ficha_col = posibles_fichas[0]
            df = df.rename(columns={ficha_col: "ficha"})
            print(f"Columna FICHA encontrada y renombrada desde: {ficha_col}")

    print("\nDataFrame renombrado:")
    print(df.head())
    print("Columnas después de renombrar:", df.columns.tolist())

    if "ficha" not in df.columns:
        return {
            "registros_insertados": 0,
            "registros_actualizados": 0,
            "grupos_creados": 0,
            "total_errores": 1,
            "errores": ["No se encontró la columna FICHA o IDENTIFICADOR_FICHA en el archivo"],
            "exitoso": False,
            "mensaje": "El archivo debe contener una columna FICHA o IDENTIFICADOR_FICHA"
        }

    df = df.dropna(subset=["ficha"])
    
    if len(df) == 0:
        return {
            "registros_insertados": 0,
            "registros_actualizados": 0,
            "grupos_creados": 0,
            "total_errores": 1,
            "errores": ["No se encontraron registros con ficha válida"],
            "exitoso": False,
            "mensaje": "No se encontraron registros válidos para procesar"
        }

    df["ficha"] = pd.to_numeric(df["ficha"], errors="coerce")
    df = df.dropna(subset=["ficha"])
    df["ficha"] = df["ficha"].astype("Int64")
    df["id_grupo"] = df["ficha"]

    columnas_grupos_numericas = ["cod_centro", "cod_regional", "cod_programa", "cod_municipio"]
    for col in columnas_grupos_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "fecha_inicio" in df.columns:
        df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce").dt.date
    if "fecha_fin" in df.columns:
        df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], errors="coerce").dt.date

    columnas_historico_numericas = [
        "num_aprendices_inscritos",
        "num_aprendices_matriculados",
        "num_aprendices_en_transito",
        "num_aprendices_formacion",
        "num_aprendices_induccion",
        "num_aprendices_condicionados",
        "num_aprendices_aplazados",
        "num_aprendices_retirado_voluntario",
        "num_aprendices_cancelados",
        "num_aprendices_reprobados",
        "num_aprendices_no_aptos",
        "num_aprendices_reingresados",
        "num_aprendices_por_certificar",
        "num_aprendices_certificados",
        "num_aprendices_trasladados"
    ]

    for col in columnas_historico_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int16")

    print("\nDataFrame procesado:")
    print(df.head())
    print("Columnas finales:", df.columns.tolist())

    if "cod_municipio" in df.columns:
        registros_originales = len(df)
        df["cod_municipio_str"] = df["cod_municipio"].apply(
            lambda x: str(x).replace(".0", "") if pd.notna(x) else ""
        )
        df_filtrado = df[df["cod_municipio_str"].str.contains("66", na=False)]
        df_filtrado = df_filtrado.drop(columns=["cod_municipio_str"])
        
        registros_filtrados = len(df_filtrado)
        registros_omitidos = registros_originales - registros_filtrados
        
        logger.info(
            f"Filtro de municipio aplicado: {registros_originales} registros originales, "
            f"{registros_filtrados} con cod_municipio que contiene '66', "
            f"{registros_omitidos} omitidos"
        )
        
        if len(df_filtrado) == 0:
            return {
                "registros_insertados": 0,
                "registros_actualizados": 0,
                "grupos_creados": 0,
                "total_errores": 1,
                "errores": [f"No se encontraron registros con cod_municipio que contenga '66'. Se omitieron {registros_omitidos} registros."],
                "exitoso": False,
                "mensaje": f"No hay registros válidos para procesar. Se omitieron {registros_omitidos} registros que no cumplen el criterio de municipio."
            }
        
        df = df_filtrado
        print(f"\nFiltro aplicado: {registros_omitidos} registros omitidos, {registros_filtrados} registros válidos")
    else:
        logger.warning("No se encontró la columna cod_municipio en el DataFrame. No se aplicó filtro.")

    resultados = insertar_historico_completo_en_bd(db, df)
    
    return resultados