from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging
import pandas as pd

logger = logging.getLogger(__name__)

HISTORICO_COLUMNAS = [
    "num_aprendices_inscritos",
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
    "num_aprendices_trasladados",
]

def insertar_historico_completo_en_bd(db: Session, df_completo):
    """
    Inserta/actualiza grupos e histórico desde un archivo Excel completo.
    
    Lógica:
    1. Compara con la tabla grupos usando el id (ficha)
    2. Si el grupo existe: solo actualiza el histórico usando id_grupo
    3. Si el grupo NO existe: crea el grupo completo y luego el histórico
    
    Args:
        db: Sesión de SQLAlchemy
        df_completo: DataFrame con datos de grupos e histórico
        
    Returns:
        dict: Resumen de la operación con contadores y errores
    """
    registros_historico_insertados = 0
    registros_historico_actualizados = 0
    grupos_creados = 0
    programas_creados = 0
    centros_creados = 0
    municipios_creados = 0
    estrategias_creadas = 0
    errores = []

    try:
        fichas = df_completo["ficha"].unique().tolist()
        query_grupos_existentes = text("""
            SELECT ficha 
            FROM grupos 
            WHERE ficha IN :fichas
        """)
        result = db.execute(query_grupos_existentes, {"fichas": tuple(fichas)})
        fichas_existentes = {row.ficha for row in result}

        df_con_grupo = df_completo[df_completo["ficha"].isin(fichas_existentes)].copy()
        df_sin_grupo = df_completo[~df_completo["ficha"].isin(fichas_existentes)].copy()

        logger.info(f"Registros con grupo existente: {len(df_con_grupo)}, sin grupo: {len(df_sin_grupo)}")

        programas_creados, centros_creados, municipios_creados, estrategias_creadas, errores_aux = \
            crear_dependencias_grupos(db, df_completo)
        errores.extend(errores_aux)

        if len(df_sin_grupo) > 0:
            grupos_creados, errores_aux = crear_grupos_desde_df(db, df_sin_grupo)
            errores.extend(errores_aux)

        if len(df_con_grupo) > 0:
            actualizados_grupos, errores_aux = actualizar_grupos_desde_df(db, df_con_grupo)
            errores.extend(errores_aux)

        (
            registros_historico_insertados,
            registros_historico_actualizados,
            registros_historico_descartados,
            errores_aux,
        ) = \
            insertar_actualizar_historico(db, df_completo)
        errores.extend(errores_aux)
        # Commit de la transacción
        db.commit()
        
        logger.info(
            f"Carga completa - Grupos creados: {grupos_creados}, "
            f"Histórico insertados: {registros_historico_insertados}, "
            f"Histórico actualizados: {registros_historico_actualizados}, "
            f"Histórico descartados: {registros_historico_descartados}"
        )

    except Exception as e:
        db.rollback()
        error_msg = f"Error crítico durante la carga completa: {str(e)}"
        errores.append(error_msg)
        logger.error(error_msg)
        raise

    return {
        "registros_insertados": registros_historico_insertados,
        "registros_actualizados": registros_historico_actualizados,
        "registros_descartados": registros_historico_descartados,
        "grupos_creados": grupos_creados,
        "grupos_actualizados": actualizados_grupos if 'actualizados_grupos' in locals() else 0,
        "programas_creados": programas_creados,
        "centros_creados": centros_creados,
        "municipios_creados": municipios_creados,
        "estrategias_creadas": estrategias_creadas,
        "total_errores": len(errores),
        "errores": errores,
        "exitoso": len(errores) == 0,
        "mensaje": "Carga completa con errores" if errores else "Carga completa exitosa"
    }


def crear_dependencias_grupos(db: Session, df):
    """Crea programas, centros, municipios y estrategias si no existen"""
    programas_creados = 0
    centros_creados = 0
    municipios_creados = 0
    estrategias_creadas = 0
    errores = []
    
    # 1. Crear programas de formación
    if "cod_programa" in df.columns:
        cols_programa = ["cod_programa"]
        if "version" in df.columns:
            cols_programa.append("version")
        if "nombre_programa" in df.columns:
            cols_programa.append("nombre_programa")
        if "nivel" in df.columns:
            cols_programa.append("nivel")
        
        df_programas = df[cols_programa].drop_duplicates()
        df_programas = df_programas.dropna(subset=["cod_programa"])
        
        insert_programa_sql = text("""
            INSERT INTO programas_formacion (
                cod_programa, cod_version, nombre_programa, nivel_formacion
            ) VALUES (
                :cod_programa, :cod_version, :nombre_programa, :nivel_formacion
            )
            ON DUPLICATE KEY UPDATE
                cod_version = COALESCE(VALUES(cod_version), cod_version),
                nombre_programa = COALESCE(VALUES(nombre_programa), nombre_programa),
                nivel_formacion = COALESCE(VALUES(nivel_formacion), nivel_formacion)
        """)
        
        for idx, row in df_programas.iterrows():
            try:
                params = {
                    "cod_programa": str(row["cod_programa"]) if pd.notna(row["cod_programa"]) else None,
                    "cod_version": str(row["version"]) if "version" in row and pd.notna(row.get("version")) else None,
                    "nombre_programa": str(row["nombre_programa"]) if "nombre_programa" in row and pd.notna(row.get("nombre_programa")) else None,
                    "nivel_formacion": str(row["nivel"]) if "nivel" in row and pd.notna(row.get("nivel")) else None
                }
                if params["cod_programa"]:
                    result = db.execute(insert_programa_sql, params)
                    if result.rowcount >= 0:  # Puede ser 0 si ya existe (ON DUPLICATE KEY UPDATE)
                        programas_creados += 1
            except SQLAlchemyError as e:
                errores.append(f"Error al crear programa (índice {idx}, cod_programa: {row.get('cod_programa', 'N/A')}): {str(e)}")
                logger.error(f"Error al crear programa: {e}")
    
    # 2. Crear/actualizar centros de formación
    # Primero verificar si el centro existe en la BD para usar su información
    if "cod_centro" in df.columns:
        cols_centro = ["cod_centro"]
        if "nombre_centro" in df.columns:
            cols_centro.append("nombre_centro")
        if "cod_regional" in df.columns:
            cols_centro.append("cod_regional")
        if "nombre_regional" in df.columns:
            cols_centro.append("nombre_regional")
        
        df_centros = df[cols_centro].drop_duplicates(subset=["cod_centro"])
        df_centros = df_centros.dropna(subset=["cod_centro"])
        
        # Consultar centros existentes en la BD
        cod_centros_existentes = df_centros["cod_centro"].unique().tolist()
        query_centros_existentes = text("""
            SELECT cod_centro, nombre_centro, cod_regional, nombre_regional
            FROM centros_formacion
            WHERE cod_centro IN :cod_centros
        """)
        
        centros_bd = {}
        if cod_centros_existentes:
            try:
                result = db.execute(query_centros_existentes, {"cod_centros": tuple(cod_centros_existentes)})
                for row in result:
                    centros_bd[int(row.cod_centro)] = {
                        "nombre_centro": row.nombre_centro,
                        "cod_regional": row.cod_regional,
                        "nombre_regional": row.nombre_regional
                    }
            except SQLAlchemyError as e:
                logger.warning(f"Error al consultar centros existentes: {e}")
        
        insert_centro_sql = text("""
            INSERT INTO centros_formacion (
                cod_centro, nombre_centro, cod_regional, nombre_regional
            ) VALUES (
                :cod_centro, :nombre_centro, :cod_regional, :nombre_regional
            )
            ON DUPLICATE KEY UPDATE
                nombre_centro = COALESCE(VALUES(nombre_centro), nombre_centro),
                cod_regional = COALESCE(VALUES(cod_regional), cod_regional),
                nombre_regional = COALESCE(VALUES(nombre_regional), nombre_regional)
        """)
        
        for idx, row in df_centros.iterrows():
            try:
                cod_centro = int(row["cod_centro"]) if pd.notna(row["cod_centro"]) else None
                if not cod_centro:
                    continue
                
                # Si el centro existe en BD, usar sus datos como base
                centro_existente = centros_bd.get(cod_centro, {})
                
                # Priorizar datos del Excel, pero si faltan, usar los de la BD
                nombre_centro = str(row["nombre_centro"]) if "nombre_centro" in row and pd.notna(row.get("nombre_centro")) else centro_existente.get("nombre_centro")
                cod_regional = int(row["cod_regional"]) if "cod_regional" in row and pd.notna(row.get("cod_regional")) else (centro_existente.get("cod_regional") if centro_existente.get("cod_regional") is not None else None)
                nombre_regional = str(row["nombre_regional"]) if "nombre_regional" in row and pd.notna(row.get("nombre_regional")) else centro_existente.get("nombre_regional")
                
                params = {
                    "cod_centro": cod_centro,
                    "nombre_centro": nombre_centro,
                    "cod_regional": cod_regional,
                    "nombre_regional": nombre_regional
                }
                
                result = db.execute(insert_centro_sql, params)
                if result.rowcount == 1:
                    centros_creados += 1
            except (ValueError, SQLAlchemyError) as e:
                errores.append(f"Error al crear/actualizar centro (índice {idx}, cod_centro: {row.get('cod_centro', 'N/A')}): {str(e)}")
                logger.error(f"Error al crear/actualizar centro: {e}")
    
    # 3. Crear municipios
    if "cod_municipio" in df.columns and "nombre_municipio" in df.columns:
        df_municipios = df[["cod_municipio", "nombre_municipio"]].drop_duplicates()
        df_municipios = df_municipios.dropna(subset=["cod_municipio"])
        
        insert_municipio_sql = text("""
            INSERT INTO municipios (
                cod_municipio, nombre
            ) VALUES (
                :cod_municipio, :nombre
            )
            ON DUPLICATE KEY UPDATE
                nombre = COALESCE(VALUES(nombre), nombre)
        """)
        
        for idx, row in df_municipios.iterrows():
            try:
                params = {
                    "cod_municipio": str(row["cod_municipio"]) if pd.notna(row["cod_municipio"]) else None,
                    "nombre": str(row["nombre_municipio"]) if pd.notna(row["nombre_municipio"]) else None
                }
                if params["cod_municipio"]:
                    result = db.execute(insert_municipio_sql, params)
                    if result.rowcount == 1:
                        municipios_creados += 1
            except SQLAlchemyError as e:
                errores.append(f"Error al crear municipio (índice {idx}): {str(e)}")
                logger.error(f"Error al crear municipio: {e}")
    
    # 4. Crear estrategias
    if "cod_estrategia" in df.columns:
        df_estrategias = df[["cod_estrategia"]].drop_duplicates()
        df_estrategias = df_estrategias.dropna(subset=["cod_estrategia"])
        
        insert_estrategia_sql = text("""
            INSERT INTO estrategia (
                cod_estrategia, nombre
            ) VALUES (
                :cod_estrategia, ''
            )
            ON DUPLICATE KEY UPDATE cod_estrategia = cod_estrategia
        """)
        
        for idx, row in df_estrategias.iterrows():
            try:
                params = {
                    "cod_estrategia": str(row["cod_estrategia"]) if pd.notna(row["cod_estrategia"]) else None
                }
                if params["cod_estrategia"]:
                    result = db.execute(insert_estrategia_sql, params)
                    if result.rowcount == 1:
                        estrategias_creadas += 1
            except SQLAlchemyError as e:
                errores.append(f"Error al crear estrategia (índice {idx}): {str(e)}")
                logger.error(f"Error al crear estrategia: {e}")
    
    return programas_creados, centros_creados, municipios_creados, estrategias_creadas, errores


def crear_grupos_desde_df(db: Session, df):
    """Crea grupos desde el DataFrame"""
    grupos_creados = 0
    errores = []
    
    insert_grupo_sql = text("""
        INSERT INTO grupos (
            ficha, cod_programa, cod_centro, modalidad, jornada, etapa_ficha,
            estado_curso, fecha_inicio, fecha_fin, cod_municipio, cod_estrategia,
            nombre_responsable, cupo_asignado, num_aprendices_fem, num_aprendices_mas,
            num_aprendices_nobin, num_aprendices_matriculados, num_aprendices_activos,
            tipo_doc_empresa, num_doc_empresa, nombre_empresa
        ) VALUES (
            :ficha, :cod_programa, :cod_centro, :modalidad, :jornada, :etapa_ficha,
            :estado_curso, :fecha_inicio, :fecha_fin, :cod_municipio, :cod_estrategia,
            :nombre_responsable, :cupo_asignado, :num_aprendices_fem, :num_aprendices_mas,
            :num_aprendices_nobin, :num_aprendices_matriculados, :num_aprendices_activos,
            :tipo_doc_empresa, :num_doc_empresa, :nombre_empresa
        )
    """)
    
    for idx, row in df.iterrows():
        try:
            params = {
                "ficha": int(row["ficha"]) if "ficha" in row and pd.notna(row["ficha"]) else None,
                "cod_programa": str(row["cod_programa"]) if "cod_programa" in row and pd.notna(row.get("cod_programa")) else None,
                "cod_centro": int(row["cod_centro"]) if "cod_centro" in row and pd.notna(row.get("cod_centro")) else None,
                "modalidad": str(row["modalidad"]) if "modalidad" in row and pd.notna(row.get("modalidad")) else None,
                "jornada": str(row["jornada"]) if "jornada" in row and pd.notna(row.get("jornada")) else None,
                "etapa_ficha": str(row["etapa_ficha"]) if "etapa_ficha" in row and pd.notna(row.get("etapa_ficha")) else None,
                "estado_curso": str(row["estado_curso"]) if "estado_curso" in row and pd.notna(row.get("estado_curso")) else None,
                "fecha_inicio": row.get("fecha_inicio") if "fecha_inicio" in row and pd.notna(row.get("fecha_inicio")) else None,
                "fecha_fin": row.get("fecha_fin") if "fecha_fin" in row and pd.notna(row.get("fecha_fin")) else None,
                "cod_municipio": str(row["cod_municipio"]) if "cod_municipio" in row and pd.notna(row.get("cod_municipio")) else None,
                "cod_estrategia": str(row["cod_estrategia"]) if "cod_estrategia" in row and pd.notna(row.get("cod_estrategia")) else None,
                "nombre_responsable": str(row["nombre_responsable"]) if "nombre_responsable" in row and pd.notna(row.get("nombre_responsable")) else None,
                "cupo_asignado": int(row["cupo_asignado"]) if "cupo_asignado" in row and pd.notna(row.get("cupo_asignado")) else None,
                "num_aprendices_fem": int(row["num_aprendices_fem"]) if "num_aprendices_fem" in row and pd.notna(row.get("num_aprendices_fem")) else None,
                "num_aprendices_mas": int(row["num_aprendices_mas"]) if "num_aprendices_mas" in row and pd.notna(row.get("num_aprendices_mas")) else None,
                "num_aprendices_nobin": int(row["num_aprendices_nobin"]) if "num_aprendices_nobin" in row and pd.notna(row.get("num_aprendices_nobin")) else None,
                "num_aprendices_matriculados": int(row.get("num_aprendices_matriculados", 0)) if "num_aprendices_matriculados" in row and pd.notna(row.get("num_aprendices_matriculados")) else None,
                "num_aprendices_activos": int(row["num_aprendices_activos"]) if "num_aprendices_activos" in row and pd.notna(row.get("num_aprendices_activos")) else None,
                "tipo_doc_empresa": str(row["tipo_doc_empresa"]) if "tipo_doc_empresa" in row and pd.notna(row.get("tipo_doc_empresa")) else None,
                "num_doc_empresa": str(row["num_doc_empresa"]) if "num_doc_empresa" in row and pd.notna(row.get("num_doc_empresa")) else None,
                "nombre_empresa": str(row["nombre_empresa"]) if "nombre_empresa" in row and pd.notna(row.get("nombre_empresa")) else None
            }
            
            if params["ficha"]:
                result = db.execute(insert_grupo_sql, params)
                if result.rowcount == 1:
                    grupos_creados += 1
        except SQLAlchemyError as e:
            errores.append(f"Error al crear grupo (índice {idx}, ficha: {row.get('ficha', 'N/A')}): {str(e)}")
            logger.error(f"Error al crear grupo: {e}")
    
    return grupos_creados, errores


def actualizar_grupos_desde_df(db: Session, df):
    actualizados = 0
    errores = []

    update_sql = text("""
        UPDATE grupos
        SET
            cod_programa = COALESCE(:cod_programa, cod_programa),
            cod_centro = COALESCE(:cod_centro, cod_centro),
            modalidad = COALESCE(:modalidad, modalidad),
            jornada = COALESCE(:jornada, jornada),
            estado_curso = COALESCE(:estado_curso, estado_curso),
            fecha_inicio = COALESCE(:fecha_inicio, fecha_inicio),
            fecha_fin = COALESCE(:fecha_fin, fecha_fin),
            cod_municipio = COALESCE(:cod_municipio, cod_municipio),
            cod_estrategia = COALESCE(:cod_estrategia, cod_estrategia),
            num_aprendices_matriculados = COALESCE(:num_aprendices_matriculados, num_aprendices_matriculados)
        WHERE ficha = :ficha
    """)

    for idx, row in df.iterrows():
        try:
            params = {
                "ficha": int(row["ficha"]) if "ficha" in row and pd.notna(row["ficha"]) else None,
                "cod_programa": str(row["cod_programa"]) if "cod_programa" in row and pd.notna(row.get("cod_programa")) else None,
                "cod_centro": int(row["cod_centro"]) if "cod_centro" in row and pd.notna(row.get("cod_centro")) else None,
                "modalidad": str(row["modalidad"]) if "modalidad" in row and pd.notna(row.get("modalidad")) else None,
                "jornada": str(row["jornada"]) if "jornada" in row and pd.notna(row.get("jornada")) else None,
                "estado_curso": str(row["estado_curso"]) if "estado_curso" in row and pd.notna(row.get("estado_curso")) else None,
                "fecha_inicio": row.get("fecha_inicio") if "fecha_inicio" in row and pd.notna(row.get("fecha_inicio")) else None,
                "fecha_fin": row.get("fecha_fin") if "fecha_fin" in row and pd.notna(row.get("fecha_fin")) else None,
                "cod_municipio": str(row["cod_municipio"]) if "cod_municipio" in row and pd.notna(row.get("cod_municipio")) else None,
                "cod_estrategia": str(row["cod_estrategia"]) if "cod_estrategia" in row and pd.notna(row.get("cod_estrategia")) else None,
                "num_aprendices_matriculados": int(row.get("num_aprendices_matriculados", 0)) if "num_aprendices_matriculados" in row and pd.notna(row.get("num_aprendices_matriculados")) else None,
            }
            if params["ficha"]:
                result = db.execute(update_sql, params)
                if result.rowcount >= 0:
                    actualizados += 1
        except SQLAlchemyError as e:
            errores.append(f"Error al actualizar grupo (índice {idx}, ficha: {row.get('ficha', 'N/A')}): {str(e)}")
            logger.error(f"Error al actualizar grupo: {e}")

    return actualizados, errores


def insertar_actualizar_historico(db: Session, df):
    registros_insertados = 0
    registros_actualizados = 0
    registros_descartados = 0
    errores = []

    columnas = ["id_grupo"] + HISTORICO_COLUMNAS
    update_clause = ", ".join([f"{c} = VALUES({c})" for c in HISTORICO_COLUMNAS])
    chunk_size = 1000

    try:
        total_afectadas = 0
        for start in range(0, len(df), chunk_size):
            batch = df.iloc[start:start+chunk_size]
            values_parts = []
            params = {}
            for i, row in batch.iterrows():
                idx = str(i)
                placeholders = []
                val_id = int(row["id_grupo"]) if "id_grupo" in row and pd.notna(row["id_grupo"]) else None
                params[f"id_grupo_{idx}"] = val_id
                placeholders.append(f":id_grupo_{idx}")
                for col in HISTORICO_COLUMNAS:
                    v = row.get(col, 0)
                    val = int(v) if pd.notna(v) else 0
                    params[f"{col}_{idx}"] = val
                    placeholders.append(f":{col}_{idx}")
                values_parts.append(f"({', '.join(placeholders)})")
            if not values_parts:
                continue
            stmt = text(f"""
                INSERT INTO historico ({', '.join(columnas)})
                VALUES {', '.join(values_parts)}
                ON DUPLICATE KEY UPDATE {update_clause}
            """)
            result = db.execute(stmt, params)
            total_afectadas += result.rowcount or 0
        registros_actualizados = total_afectadas
    except SQLAlchemyError as e:
        errores.append(f"Error en carga masiva de histórico: {str(e)}")
        logger.error(f"Error en carga masiva de histórico: {e}")

    return registros_insertados, registros_actualizados, registros_descartados, errores


def insertar_historico_en_bd(db: Session, df_historico):
    """
    Inserta registros históricos de aprendices por grupo en la base de datos.
    """
    registros_insertados = 0
    registros_actualizados = 0
    registros_descartados = 0
    errores = []

    try:
        (
            registros_insertados,
            registros_actualizados,
            registros_descartados,
            errores_aux,
        ) = insertar_actualizar_historico(db, df_historico)
        errores.extend(errores_aux)

        db.commit()

        logger.info(
            "Carga de histórico completada - %s insertados, %s actualizados, %s descartados",
            registros_insertados,
            registros_actualizados,
            registros_descartados,
        )

    except Exception as e:
        db.rollback()
        error_msg = f"Error crítico durante la carga de histórico: {str(e)}"
        errores.append(error_msg)
        logger.error(error_msg)
        raise

    return {
        "registros_insertados": registros_insertados,
        "registros_actualizados": registros_actualizados,
        "registros_descartados": registros_descartados,
        "total_errores": len(errores),
        "errores": errores,
        "exitoso": len(errores) == 0,
        "mensaje": "Carga de histórico completada con errores" if errores else "Carga de histórico completada exitosamente"
    }
