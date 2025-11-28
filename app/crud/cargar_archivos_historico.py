from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging
import pandas as pd

logger = logging.getLogger(__name__)

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
        # Obtener todas las fichas del DataFrame
        fichas = df_completo["ficha"].unique().tolist()
        # Verificar qué fichas ya existen en grupos
        query_grupos_existentes = text("""
            SELECT ficha 
            FROM grupos 
            WHERE ficha IN :fichas
        """)
        
        result = db.execute(query_grupos_existentes, {"fichas": tuple(fichas)})
        fichas_existentes = {row.ficha for row in result}
        
        # Separar registros: los que tienen grupo y los que no
        df_con_grupo = df_completo[df_completo["ficha"].isin(fichas_existentes)].copy()
        df_sin_grupo = df_completo[~df_completo["ficha"].isin(fichas_existentes)].copy()
        
        logger.info(f"Registros con grupo existente: {len(df_con_grupo)}, sin grupo: {len(df_sin_grupo)}")
        
        # 1. Procesar registros SIN grupo: crear grupos primero
        if len(df_sin_grupo) > 0:
            programas_creados, centros_creados, municipios_creados, estrategias_creadas, errores_aux = \
                crear_dependencias_grupos(db, df_sin_grupo)
            errores.extend(errores_aux)
            
            grupos_creados, errores_aux = crear_grupos_desde_df(db, df_sin_grupo)
            errores.extend(errores_aux)
        
        # 2. Procesar histórico para TODOS los registros (con y sin grupo)
        registros_historico_insertados, registros_historico_actualizados, errores_aux = \
            insertar_actualizar_historico(db, df_completo)
        errores.extend(errores_aux)
        # Commit de la transacción
        db.commit()
        
        logger.info(
            f"Carga completa - Grupos creados: {grupos_creados}, "
            f"Histórico insertados: {registros_historico_insertados}, "
            f"Histórico actualizados: {registros_historico_actualizados}"
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
        "grupos_creados": grupos_creados,
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
    if "cod_programa" in df.columns and "nombre_programa" in df.columns:
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
                    "nombre_programa": str(row["nombre_programa"]) if "nombre_programa" in row and pd.notna(row["nombre_programa"]) else None,
                    "nivel_formacion": str(row["nivel"]) if "nivel" in row and pd.notna(row.get("nivel")) else None
                }
                if params["cod_programa"]:
                    result = db.execute(insert_programa_sql, params)
                    if result.rowcount == 1:
                        programas_creados += 1
            except SQLAlchemyError as e:
                errores.append(f"Error al crear programa (índice {idx}): {str(e)}")
                logger.error(f"Error al crear programa: {e}")
    
    # 2. Crear centros de formación
    if "cod_centro" in df.columns and "nombre_centro" in df.columns:
        cols_centro = ["cod_centro", "nombre_centro"]
        if "cod_regional" in df.columns:
            cols_centro.append("cod_regional")
        if "nombre_regional" in df.columns:
            cols_centro.append("nombre_regional")
        
        df_centros = df[cols_centro].drop_duplicates()
        df_centros = df_centros.dropna(subset=["cod_centro"])
        
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
                params = {
                    "cod_centro": int(row["cod_centro"]) if pd.notna(row["cod_centro"]) else None,
                    "nombre_centro": str(row["nombre_centro"]) if pd.notna(row["nombre_centro"]) else None,
                    "cod_regional": int(row["cod_regional"]) if "cod_regional" in row and pd.notna(row.get("cod_regional")) else None,
                    "nombre_regional": str(row["nombre_regional"]) if "nombre_regional" in row and pd.notna(row.get("nombre_regional")) else None
                }
                if params["cod_centro"]:
                    result = db.execute(insert_centro_sql, params)
                    if result.rowcount == 1:
                        centros_creados += 1
            except SQLAlchemyError as e:
                errores.append(f"Error al crear centro (índice {idx}): {str(e)}")
                logger.error(f"Error al crear centro: {e}")
    
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
                "cod_programa": int(row["cod_programa"]) if "cod_programa" in row and pd.notna(row.get("cod_programa")) else None,
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


def insertar_actualizar_historico(db: Session, df):
    """Inserta o actualiza registros históricos"""
    registros_insertados = 0
    registros_actualizados = 0
    errores = []
    
    insert_historico_sql = text("""
        INSERT INTO historico (
            id_grupo,
            num_aprendices_inscritos,
            num_aprendices_en_transito,
            num_aprendices_formacion,
            num_aprendices_induccion,
            num_aprendices_condicionados,
            num_aprendices_aplazados,
            num_aprendices_retirado_voluntario,
            num_aprendices_cancelados,
            num_aprendices_reprobados,
            num_aprendices_no_aptos,
            num_aprendices_reingresados,
            num_aprendices_por_certificar,
            num_aprendices_certificados,
            num_aprendices_trasladados
        ) VALUES (
            :id_grupo,
            :num_aprendices_inscritos,
            :num_aprendices_en_transito,
            :num_aprendices_formacion,
            :num_aprendices_induccion,
            :num_aprendices_condicionados,
            :num_aprendices_aplazados,
            :num_aprendices_retirado_voluntario,
            :num_aprendices_cancelados,
            :num_aprendices_reprobados,
            :num_aprendices_no_aptos,
            :num_aprendices_reingresados,
            :num_aprendices_por_certificar,
            :num_aprendices_certificados,
            :num_aprendices_trasladados
        )
        ON DUPLICATE KEY UPDATE
            num_aprendices_inscritos = VALUES(num_aprendices_inscritos),
            num_aprendices_en_transito = VALUES(num_aprendices_en_transito),
            num_aprendices_formacion = VALUES(num_aprendices_formacion),
            num_aprendices_induccion = VALUES(num_aprendices_induccion),
            num_aprendices_condicionados = VALUES(num_aprendices_condicionados),
            num_aprendices_aplazados = VALUES(num_aprendices_aplazados),
            num_aprendices_retirado_voluntario = VALUES(num_aprendices_retirado_voluntario),
            num_aprendices_cancelados = VALUES(num_aprendices_cancelados),
            num_aprendices_reprobados = VALUES(num_aprendices_reprobados),
            num_aprendices_no_aptos = VALUES(num_aprendices_no_aptos),
            num_aprendices_reingresados = VALUES(num_aprendices_reingresados),
            num_aprendices_por_certificar = VALUES(num_aprendices_por_certificar),
            num_aprendices_certificados = VALUES(num_aprendices_certificados),
            num_aprendices_trasladados = VALUES(num_aprendices_trasladados)
    """)
    
    for idx, row in df.iterrows():
        try:
            if "id_grupo" not in row or pd.isna(row["id_grupo"]):
                errores.append(f"Error: id_grupo requerido en índice {idx}")
                continue
            
            params = {
                "id_grupo": int(row["id_grupo"]),
                "num_aprendices_inscritos": int(row.get("num_aprendices_inscritos", 0)) if "num_aprendices_inscritos" in row and pd.notna(row.get("num_aprendices_inscritos")) else 0,
                "num_aprendices_en_transito": int(row.get("num_aprendices_en_transito", 0)) if "num_aprendices_en_transito" in row and pd.notna(row.get("num_aprendices_en_transito")) else 0,
                "num_aprendices_formacion": int(row.get("num_aprendices_formacion", 0)) if "num_aprendices_formacion" in row and pd.notna(row.get("num_aprendices_formacion")) else 0,
                "num_aprendices_induccion": int(row.get("num_aprendices_induccion", 0)) if "num_aprendices_induccion" in row and pd.notna(row.get("num_aprendices_induccion")) else 0,
                "num_aprendices_condicionados": int(row.get("num_aprendices_condicionados", 0)) if "num_aprendices_condicionados" in row and pd.notna(row.get("num_aprendices_condicionados")) else 0,
                "num_aprendices_aplazados": int(row.get("num_aprendices_aplazados", 0)) if "num_aprendices_aplazados" in row and pd.notna(row.get("num_aprendices_aplazados")) else 0,
                "num_aprendices_retirado_voluntario": int(row.get("num_aprendices_retirado_voluntario", 0)) if "num_aprendices_retirado_voluntario" in row and pd.notna(row.get("num_aprendices_retirado_voluntario")) else 0,
                "num_aprendices_cancelados": int(row.get("num_aprendices_cancelados", 0)) if "num_aprendices_cancelados" in row and pd.notna(row.get("num_aprendices_cancelados")) else 0,
                "num_aprendices_reprobados": int(row.get("num_aprendices_reprobados", 0)) if "num_aprendices_reprobados" in row and pd.notna(row.get("num_aprendices_reprobados")) else 0,
                "num_aprendices_no_aptos": int(row.get("num_aprendices_no_aptos", 0)) if "num_aprendices_no_aptos" in row and pd.notna(row.get("num_aprendices_no_aptos")) else 0,
                "num_aprendices_reingresados": int(row.get("num_aprendices_reingresados", 0)) if "num_aprendices_reingresados" in row and pd.notna(row.get("num_aprendices_reingresados")) else 0,
                "num_aprendices_por_certificar": int(row.get("num_aprendices_por_certificar", 0)) if "num_aprendices_por_certificar" in row and pd.notna(row.get("num_aprendices_por_certificar")) else 0,
                "num_aprendices_certificados": int(row.get("num_aprendices_certificados", 0)) if "num_aprendices_certificados" in row and pd.notna(row.get("num_aprendices_certificados")) else 0,
                "num_aprendices_trasladados": int(row.get("num_aprendices_trasladados", 0)) if "num_aprendices_trasladados" in row and pd.notna(row.get("num_aprendices_trasladados")) else 0
            }
            
            result = db.execute(insert_historico_sql, params)
            
            if result.rowcount == 1:
                registros_insertados += 1
            elif result.rowcount == 2:
                registros_actualizados += 1
                
        except SQLAlchemyError as e:
            errores.append(f"Error al insertar histórico (índice {idx}, id_grupo: {row.get('id_grupo', 'N/A')}): {str(e)}")
            logger.error(f"Error al insertar histórico: {e}")
    
    return registros_insertados, registros_actualizados, errores


def insertar_historico_en_bd(db: Session, df_historico):
    """
    Inserta registros históricos de aprendices por grupo en la base de datos.
    
    Args:
        db: Sesión de SQLAlchemy
        df_historico: DataFrame con datos históricos de grupos
        
    Returns:
        dict: Resumen de la operación con contadores y errores
    """
    registros_insertados = 0
    registros_actualizados = 0
    errores = []

    try:
        insert_historico_sql = text("""
            INSERT INTO historico (
                id_grupo,
                num_aprendices_inscritos,
                num_aprendices_en_transito,
                num_aprendices_formacion,
                num_aprendices_induccion,
                num_aprendices_condicionados,
                num_aprendices_aplazados,
                num_aprendices_retirado_voluntario,
                num_aprendices_cancelados,
                num_aprendices_reprobados,
                num_aprendices_no_aptos,
                num_aprendices_reingresados,
                num_aprendices_por_certificar,
                num_aprendices_certificados,
                num_aprendices_trasladados
            ) VALUES (
                :id_grupo,
                :num_aprendices_inscritos,
                :num_aprendices_en_transito,
                :num_aprendices_formacion,
                :num_aprendices_induccion,
                :num_aprendices_condicionados,
                :num_aprendices_aplazados,
                :num_aprendices_retirado_voluntario,
                :num_aprendices_cancelados,
                :num_aprendices_reprobados,
                :num_aprendices_no_aptos,
                :num_aprendices_reingresados,
                :num_aprendices_por_certificar,
                :num_aprendices_certificados,
                :num_aprendices_trasladados
            )
            ON DUPLICATE KEY UPDATE
                num_aprendices_inscritos = VALUES(num_aprendices_inscritos),
                num_aprendices_en_transito = VALUES(num_aprendices_en_transito),
                num_aprendices_formacion = VALUES(num_aprendices_formacion),
                num_aprendices_induccion = VALUES(num_aprendices_induccion),
                num_aprendices_condicionados = VALUES(num_aprendices_condicionados),
                num_aprendices_aplazados = VALUES(num_aprendices_aplazados),
                num_aprendices_retirado_voluntario = VALUES(num_aprendices_retirado_voluntario),
                num_aprendices_cancelados = VALUES(num_aprendices_cancelados),
                num_aprendices_reprobados = VALUES(num_aprendices_reprobados),
                num_aprendices_no_aptos = VALUES(num_aprendices_no_aptos),
                num_aprendices_reingresados = VALUES(num_aprendices_reingresados),
                num_aprendices_por_certificar = VALUES(num_aprendices_por_certificar),
                num_aprendices_certificados = VALUES(num_aprendices_certificados),
                num_aprendices_trasladados = VALUES(num_aprendices_trasladados)
        """)

        for idx, row in df_historico.iterrows():
            try:
                params = row.to_dict()
                params = {k: (None if pd.isna(v) else v) for k, v in params.items()}
                
                if not params.get('id_grupo'):
                    msg = f"Error: id_grupo requerido en índice {idx}"
                    errores.append(msg)
                    logger.warning(msg)
                    continue
                
                result = db.execute(insert_historico_sql, params)
                
                if result.rowcount == 1:
                    registros_insertados += 1
                elif result.rowcount == 2:
                    registros_actualizados += 1
                    
            except SQLAlchemyError as e:
                msg = f"Error al insertar histórico (índice {idx}, id_grupo: {row.get('id_grupo', 'N/A')}): {str(e)}"
                errores.append(msg)
                logger.error(msg)
        # Commit de la transacción
        db.commit()
        
        logger.info(
            f"Carga de histórico completada - {registros_insertados} insertados, "
            f"{registros_actualizados} actualizados"
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
        "total_errores": len(errores),
        "errores": errores,
        "exitoso": len(errores) == 0,
        "mensaje": "Carga de histórico completada con errores" if errores else "Carga de histórico completada exitosamente"
    }