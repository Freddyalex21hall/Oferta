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


def _eliminar_duplicados_historico(df: pd.DataFrame):
    """
    Elimina registros duplicados comparando la información completa del grupo/histórico.
    Conserva la primera aparición del registro y descarta el resto.
    """
    columnas_comparacion = [
        "cod_regional",           # 1. CODIGO_REGIONAL
        "nombre_regional",        # 2. NOMBRE_REGIONAL
        "cod_centro",             # 3. CODIGO_CENTRO
        "nombre_centro",          # 4. NOMBRE_CENTRO
        "datos_centro",          # 5. DATOS
        "cod_programa",          # 6. CODIGO_PROGRAMA_FORMACION
        "version",                # 7. VERSION
        "tipo_programa",          # 8. TIPO_PROGRAMA
        "nivel",                  # 9. NIVEL
        "jornada",                # 10. JORNADA
        "cod_municipio",          # 11. ID_MUNICIPIO
        "nombre_municipio",       # 12. MUNICIPIO
        "cod_estrategia",         # 13. FIC_MC
        "modalidad",              # 14. MODALIDAD
        "ficha",                  # 15. FICHA
        "fecha_inicio",           # 16. FECHA_INICIO
        "fecha_fin",              # 17. FECHA_FIN
        "duracion_meses",         # 18. MESES_DURACION
        "estado_curso",           # 19. ESTADO
        "codigo_estado",          # 20. CODIGO_ESTADO
        "nombre_estado",          # 21. NOMBRE_ESTADO
        "num_aprendices_inscritos",      # 22. INSCRITOS
        "num_aprendices_matriculados",   # 23. MATRICULADOS
        "num_aprendices_en_transito",    # 24. EN_TRAINING
        "num_aprendices_formacion",      # 25. FORMACION
        "num_aprendices_induccion",      # 26. INDUCCION
        "num_aprendices_condicionados",  # 27. CONDICIONADOS
        "num_aprendices_aplazados",      # 28. APLAZADOS
        "num_aprendices_retirado_voluntario",  # 29. RETIRO
        "num_aprendices_cancelados",     # 30. CANCELADOS
        "num_aprendices_reprobados",     # 31. REPROBADOS
        "num_aprendices_no_aptos",       # 32. NO_APROBADOS
        "num_aprendices_reingresados",   # 33. REINGRESO
        "num_aprendices_por_certificar", # 34. POR_CERTIFICAR
        "num_aprendices_certificados",  # 35. CERTIFICADOS
        "num_aprendices_trasladados"     # 36. TRASLADOS
        # "id_grupo",
        # "ficha",
        # "cod_programa",
        # "cod_centro",
        # "modalidad",
        # "jornada",
        # "etapa_ficha",
        # "estado_curso",
        # "cod_municipio",
        # "cod_estrategia",
        # "num_aprendices_inscritos",
        # "num_aprendices_matriculados",
        # "num_aprendices_en_transito",
        # "num_aprendices_formacion",
        # "num_aprendices_induccion",
        # "num_aprendices_condicionados",
        # "num_aprendices_aplazados",
        # "num_aprendices_retirado_voluntario",
        # "num_aprendices_cancelados",
        # "num_aprendices_reprobados",
        # "num_aprendices_no_aptos",
        # "num_aprendices_reingresados",
        # "num_aprendices_por_certificar",
        # "num_aprendices_certificados",
        # "num_aprendices_trasladados",
    ]

    columnas_presentes = [col for col in columnas_comparacion if col in df.columns]
    if not columnas_presentes:
        return df, 0

    df_sin_duplicados = df.drop_duplicates(subset=columnas_presentes, keep="first")
    eliminados = len(df) - len(df_sin_duplicados)
    if eliminados > 0:
        logger.info(
            "Se eliminaron %s registros duplicados basados en las columnas: %s",
            eliminados,
            ", ".join(columnas_presentes),
        )
    return df_sin_duplicados, eliminados

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
    
    logger.debug("Excel leído y columnas iniciales normalizadas")
    
    # Mapeo exacto de las 36 columnas del Excel según el orden mostrado
    columnas_mapeo = {
        # Columnas 1-5: Regional y Centro
        "CODIGO_REGIONAL": "cod_regional",
        "NOMBRE_REGIONAL": "nombre_regional",
        "CODIGO_CENTRO": "cod_centro",
        "NOMBRE_CENTRO": "nombre_centro",
        "DATOS": "datos_centro",
        "DATOS_CENTRO": "datos_centro",
        
        # Columnas 6-9: Programa de formación
        "CODIGO_PROGRAMA_FORMACION": "cod_programa",
        "VERSION": "version",
        "VERSION_PROGRAMA": "version",
        "TIPO_PROGRAMA": "tipo_programa",
        "NIVEL": "nivel",
        "NIVEL_FORMACION": "nivel",
        
        # Columnas 10-14: Configuración del grupo
        "JORNADA": "jornada",
        "ID_MUNICIPIO": "cod_municipio",
        "MUNICIPIO": "nombre_municipio",
        "FIC_MC": "cod_estrategia",
        "FIC_MOD_FORMACION": "cod_estrategia",
        "MODALIDAD": "modalidad",
        "MODALIDAD_FORMACION": "modalidad",
        
        # Columnas 15-21: Información de la ficha
        "FICHA": "ficha",
        "FECHA_INICIO": "fecha_inicio",
        "FECHA_FIN": "fecha_fin",
        "MESES_DURACION": "duracion_meses",
        "DRURACION_PROGRAMA": "duracion_meses",
        "ESTADO": "estado_curso",
        "ESTADO_FICHA": "estado_curso",
        "CODIGO_ESTADO": "codigo_estado",
        "NOMBRE_ESTADO": "nombre_estado",
        
        # Columnas 22-36: Datos históricos de aprendices
        "INSCRITOS": "num_aprendices_inscritos",
        "MATRICULADOS": "num_aprendices_matriculados",
        "EN_TRAINING": "num_aprendices_en_transito",
        "EN_TRANSITO": "num_aprendices_en_transito",
        "FORMACION": "num_aprendices_formacion",
        "INDUCCION": "num_aprendices_induccion",
        "CONDICIONADOS": "num_aprendices_condicionados",
        "APLAZADOS": "num_aprendices_aplazados",
        "RETIRO": "num_aprendices_retirado_voluntario",
        "RETIROS_VOLUNTARIOS": "num_aprendices_retirado_voluntario",
        "CANCELADOS": "num_aprendices_cancelados",
        "REPROBADOS": "num_aprendices_reprobados",
        "NO_APROBADOS": "num_aprendices_no_aptos",
        "NO_APTOS": "num_aprendices_no_aptos",
        "REINGRESO": "num_aprendices_reingresados",
        "REINGRESADO": "num_aprendices_reingresados",
        "POR_CERTIFICAR": "num_aprendices_por_certificar",
        "CERTIFICADOS": "num_aprendices_certificados",
        "TRASLADOS": "num_aprendices_trasladados",
        "TRASLADADOS": "num_aprendices_trasladados",
        
        # Variantes adicionales para compatibilidad
        "IDENTIFICADOR_FICHA": "ficha",
        "CODIGO_PROGRAMA": "cod_programa",
        "COD_PROGRAMA": "cod_programa",
        "CODIGO_CENTRO": "cod_centro",
        "COD_REGIONAL": "cod_regional",
        "COD_MUNICIPIO": "cod_municipio",
        "CODIGO_MUNICIPIO": "cod_municipio",
        "FECHA_INI": "fecha_inicio",
        "ESTADO_CURSO": "estado_curso",
        "EN_TRANSITO": "num_aprendices_en_transito",
        "RETIRADO_VOLUNTARIO": "num_aprendices_retirado_voluntario",
        "NO_APTOS": "num_aprendices_no_aptos",
        "REINGRESADOS": "num_aprendices_reingresados",
        "PROGRAMA_FORMACION": "nombre_programa",
    }
    
    df.columns = df.columns.astype(str).str.strip()
    
    # Normalizar espacios y caracteres especiales en nombres de columnas
    def normalizar_nombre_columna(col):
        """Normaliza el nombre de columna para comparación"""
        if pd.isna(col):
            return ""
        col_str = str(col).strip()
        # Reemplazar espacios múltiples por uno solo
        col_str = " ".join(col_str.split())
        return col_str
    
    df.columns = [normalizar_nombre_columna(col) for col in df.columns]
    
    columnas_renombrar = {}
    columnas_usadas = set()
    
    # Crear diccionario con todas las variantes posibles (con y sin espacios)
    columnas_disponibles_upper = {}
    for col in df.columns:
        col_upper = col.upper().strip()
        # Agregar variante exacta
        columnas_disponibles_upper[col_upper] = col
        # Agregar variante sin espacios
        columnas_disponibles_upper[col_upper.replace(" ", "")] = col
        # Agregar variante con guión bajo
        columnas_disponibles_upper[col_upper.replace(" ", "_")] = col
    
    # Mapeo prioritario: primero buscar coincidencias exactas, luego parciales
    for col_excel, col_bd in columnas_mapeo.items():
        col_excel_upper = col_excel.upper().strip()
        col_excel_sin_espacios = col_excel_upper.replace(" ", "")
        col_excel_con_guion = col_excel_upper.replace(" ", "_")
        
        col_encontrada = None
        
        # 1. Buscar coincidencia exacta (con espacios, sin espacios, con guión)
        for variante in [col_excel_upper, col_excel_sin_espacios, col_excel_con_guion]:
            if variante in columnas_disponibles_upper:
                col_candidata = columnas_disponibles_upper[variante]
                if col_bd not in columnas_usadas:
                    col_encontrada = col_candidata
                    break
        
        # 2. Evitar mapeos parciales para no confundir columnas similares
        #    Si no hay coincidencia exacta entre variantes, no se mapea aquí
        #    (se intentará por posición solo cuando haya exactamente 36 columnas estándar)
        
        if col_encontrada and col_bd not in columnas_usadas:
            columnas_renombrar[col_encontrada] = col_bd
            columnas_usadas.add(col_bd)
            # Eliminar todas las variantes de la columna encontrada
            col_encontrada_upper = col_encontrada.upper().strip()
            variantes_a_eliminar = [
                col_encontrada_upper,
                col_encontrada_upper.replace(" ", ""),
                col_encontrada_upper.replace(" ", "_")
            ]
            for variante in variantes_a_eliminar:
                if variante in columnas_disponibles_upper:
                    del columnas_disponibles_upper[variante]
    
    # Mapeo por posición para las 36 columnas estándar del Excel
    # Si alguna columna no se mapeó, intentar mapearla por posición
    columnas_originales = list(df.columns)
    
    # Orden esperado de las 36 columnas del Excel:
    orden_columnas_esperado = [
        "cod_regional",           # 1. CODIGO_REGIONAL
        "nombre_regional",        # 2. NOMBRE_REGIONAL
        "cod_centro",             # 3. CODIGO_CENTRO
        "nombre_centro",          # 4. NOMBRE_CENTRO
        "datos_centro",          # 5. DATOS
        "cod_programa",          # 6. CODIGO_PROGRAMA_FORMACION
        "version",                # 7. VERSION
        "tipo_programa",          # 8. TIPO_PROGRAMA
        "nivel",                  # 9. NIVEL
        "jornada",                # 10. JORNADA
        "cod_municipio",          # 11. ID_MUNICIPIO
        "nombre_municipio",       # 12. MUNICIPIO
        "cod_estrategia",         # 13. FIC_MC
        "modalidad",              # 14. MODALIDAD
        "ficha",                  # 15. FICHA
        "fecha_inicio",           # 16. FECHA_INICIO
        "fecha_fin",              # 17. FECHA_FIN
        "duracion_meses",         # 18. MESES_DURACION
        "estado_curso",           # 19. ESTADO
        "codigo_estado",          # 20. CODIGO_ESTADO
        "nombre_estado",          # 21. NOMBRE_ESTADO
        "num_aprendices_inscritos",      # 22. INSCRITOS
        "num_aprendices_matriculados",   # 23. MATRICULADOS
        "num_aprendices_en_transito",    # 24. EN_TRAINING
        "num_aprendices_formacion",      # 25. FORMACION
        "num_aprendices_induccion",      # 26. INDUCCION
        "num_aprendices_condicionados",  # 27. CONDICIONADOS
        "num_aprendices_aplazados",      # 28. APLAZADOS
        "num_aprendices_retirado_voluntario",  # 29. RETIRO
        "num_aprendices_cancelados",     # 30. CANCELADOS
        "num_aprendices_reprobados",     # 31. REPROBADOS
        "num_aprendices_no_aptos",       # 32. NO_APROBADOS
        "num_aprendices_reingresados",   # 33. REINGRESO
        "num_aprendices_por_certificar", # 34. POR_CERTIFICAR
        "num_aprendices_certificados",  # 35. CERTIFICADOS
        "num_aprendices_trasladados"     # 36. TRASLADOS
    ]
    
    # Si hay exactamente 36 columnas y alguna no está mapeada, intentar mapear por posición
    if len(columnas_originales) == 36:
        for idx, col_esperada in enumerate(orden_columnas_esperado):
            if col_esperada not in columnas_usadas and idx < len(columnas_originales):
                col_original = columnas_originales[idx]
                if col_original not in columnas_renombrar:
                    columnas_renombrar[col_original] = col_esperada
                    columnas_usadas.add(col_esperada)
                    logger.info(f"Mapeo por posición: columna {idx+1} '{col_original}' -> '{col_esperada}'")
    
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

    logger.debug("Columnas mapeadas y renombradas")

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

    # Convertir columnas numéricas de grupos
    columnas_grupos_numericas = ["cod_centro", "cod_regional", "cod_municipio"]
    for col in columnas_grupos_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # cod_programa puede ser numérico o string, mantenerlo como string
    if "cod_programa" in df.columns:
        df["cod_programa"] = df["cod_programa"].astype(str).replace("nan", None)

    # Procesar fechas
    if "fecha_inicio" in df.columns:
        df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce").dt.date
    if "fecha_fin" in df.columns:
        df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], errors="coerce").dt.date
    
    # Procesar duracion_meses
    if "duracion_meses" in df.columns:
        df["duracion_meses"] = pd.to_numeric(df["duracion_meses"], errors="coerce")
    
    # Procesar campos de estado (si existen)
    if "codigo_estado" in df.columns:
        df["codigo_estado"] = pd.to_numeric(df["codigo_estado"], errors="coerce")
    if "nombre_estado" in df.columns:
        df["nombre_estado"] = df["nombre_estado"].astype(str)
    
    # Procesar tipo_programa
    if "tipo_programa" in df.columns:
        df["tipo_programa"] = df["tipo_programa"].astype(str)
    
    # Procesar datos_centro (campo DATOS del Excel)
    if "datos_centro" in df.columns:
        df["datos_centro"] = df["datos_centro"].astype(str)

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

    # Convertir columnas numéricas preservando los valores exactos del Excel
    for col in columnas_historico_numericas:
        if col in df.columns:
            # Primero convertir a numérico, preservando NaN para valores no numéricos
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Solo reemplazar NaN por 0 si es necesario (valores vacíos), pero preservar 0 explícitos
            # Usar Int64 nullable para preservar valores exactos y permitir NaN
            df[col] = df[col].fillna(0).astype("Int64")

    logger.debug("DataFrame procesado para inserción")

    # Eliminar registros duplicados para evitar reprocesar la misma información
    df, registros_duplicados = _eliminar_duplicados_historico(df)
    if registros_duplicados:
        logger.info("Total de registros duplicados descartados: %s", registros_duplicados)

    # Validación de regional: cod_regional = 66 y nombre_regional contiene "RISARALDA"
    registros_originales = len(df)
    
    # Convertir cod_regional a numérico para comparación
    if "cod_regional" in df.columns:
        df["cod_regional"] = pd.to_numeric(df["cod_regional"], errors="coerce")
    
    # Aplicar filtro de regional
    if "cod_regional" in df.columns and "nombre_regional" in df.columns:
        # Filtrar por cod_regional = 66 y nombre_regional contiene "RISARALDA"
        df["nombre_regional_str"] = df["nombre_regional"].apply(
            lambda x: str(x).upper() if pd.notna(x) else ""
        )
        df_filtrado = df[
            (df["cod_regional"] == 66) & 
            (df["nombre_regional_str"].str.contains("RISARALDA", na=False))
        ]
        df_filtrado = df_filtrado.drop(columns=["nombre_regional_str"])
        
        registros_filtrados = len(df_filtrado)
        registros_omitidos = registros_originales - registros_filtrados
        
        logger.info(
            f"Filtro de regional aplicado: {registros_originales} registros originales, "
            f"{registros_filtrados} con cod_regional=66 y nombre_regional contiene 'RISARALDA', "
            f"{registros_omitidos} omitidos"
        )
        
        if len(df_filtrado) == 0:
            return {
                "registros_insertados": 0,
                "registros_actualizados": 0,
                "grupos_creados": 0,
                "total_errores": 1,
                "errores": [f"No se encontraron registros con cod_regional=66 y nombre_regional que contenga 'RISARALDA'. Se omitieron {registros_omitidos} registros."],
                "exitoso": False,
                "mensaje": f"No hay registros válidos para procesar. Se omitieron {registros_omitidos} registros que no cumplen el criterio de regional."
            }
        
        df = df_filtrado
        logger.info(f"Filtro de regional aplicado: omitidos={registros_omitidos}, válidos={registros_filtrados}")
    else:
        logger.warning("No se encontraron las columnas cod_regional o nombre_regional en el DataFrame. No se aplicó filtro de regional.")

    resultados = insertar_historico_completo_en_bd(db, df)
    
    return resultados
