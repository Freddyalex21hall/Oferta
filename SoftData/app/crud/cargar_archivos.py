from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)

def insertar_datos_en_bd(db: Session, df_programas, df_municipios, df_estrategias, df_centros ,df_grupos):
    programas_insertados = 0
    programas_actualizados = 0
    municipios_insertados = 0
    municipios_actualizados = 0
    estrategias_insertados = 0
    estrategias_actualizados = 0
    grupos_insertados = 0
    grupos_actualizados = 0
    centros_insertados = 0
    centros_actualizados = 0
    errores = []

    # 1. Insertar programas
    insert_programa_sql = text("""
        INSERT INTO programas_formacion (
            cod_programa, version, nombre_programa, nivel, tiempo_duracion, estado, url_pdf 
        ) VALUES (
            :cod_programa, :version, :nombre_programa, :nivel, :tiempo_duracion, :estado, :url_pdf
        )
        ON DUPLICATE KEY UPDATE version = VALUES(version)
    """)

    for idx, row in df_programas.iterrows():
        try:
            result = db.execute(insert_programa_sql, row.to_dict())
            if result.rowcount == 1:
                programas_insertados += 1
            elif result.rowcount == 2:
                programas_actualizados += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar programa (índice {idx}): {e}"
            errores.append(msg)
            logger.error(f"Error al insertar: {e}")

    # 2. Insertar municipios
    insert_municipio_sql = text("""
        INSERT INTO municipios (
            cod_municipio, nombre_municipio
        ) VALUES (
            :cod_municipio, :nombre_municipio
        )
        ON DUPLICATE KEY UPDATE nombre_municipio = VALUES(nombre_municipio)
    """)

    for idx, row in df_municipios.iterrows():
        try:
            result = db.execute(insert_municipio_sql, row.to_dict())
            if result.rowcount == 1:
                municipios_insertados += 1
            elif result.rowcount == 2:
                municipios_actualizados += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar municipio (índice {idx}): {e}"
            errores.append(msg)
            logger.error(f"Error al insertar: {e}")
            
    # 3. Insertar estrategias
    insert_estrategia_sql = text("""
        INSERT INTO estrategia (
            cod_estrategia, nombre_estrategia
        ) VALUES (
            :cod_estrategia, :nombre_estrategia
        )
        ON DUPLICATE KEY UPDATE nombre_estrategia = VALUES(nombre_estrategia)
    """)

    for idx, row in df_estrategias.iterrows():
        try:
            result = db.execute(insert_estrategia_sql, row.to_dict())
            if result.rowcount == 1:
                estrategias_insertados += 1
            elif result.rowcount == 2:
                estrategias_actualizados += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar estrategia (índice {idx}): {e}"
            errores.append(msg)
            logger.error(f"Error al insertar: {e}")
            
    # 4. Insertar centros
    insert_centro_sql = text("""
        INSERT INTO centros_formacion (
            cod_centro, nombre_centro, cod_regional, nombre_regional
        ) VALUES (
            :cod_centro, :nombre_centro, :cod_regional, :nombre_regional
        )
        ON DUPLICATE KEY UPDATE cod_centro = cod_centro
    """)

    for idx, row in df_centros.iterrows():
        try:
            result = db.execute(insert_centro_sql, row.to_dict())
            if result.rowcount == 1:
                centros_insertados += 1
            elif result.rowcount == 2:
                centros_actualizados += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar centro (índice {idx}): {e}"
            errores.append(msg)
            logger.error(f"Error al insertar: {e}")
            
            
    # 5. Insertar grupos
    insert_grupo_sql = text("""
        INSERT INTO grupos (
            ficha, cod_programa, cod_centro, modalidad, jornada, etapa_ficha, estado_curso, fecha_inicio, fecha_fin, cod_municipio, cod_estrategia, nombre_responsable, cupo_asignado, num_aprendices_fem, num_aprendices_mas, num_aprendices_nobin, num_aprendices_matriculados, num_aprendices_activos, tipo_doc_empresa, num_doc_empresa, nombre_empresa 
        ) VALUES (
            :ficha, :cod_programa, :cod_centro, :modalidad, :jornada, :etapa_ficha, :estado_curso, :fecha_inicio, :fecha_fin, :cod_municipio, :cod_estrategia, :nombre_responsable, :cupo_asignado, :num_aprendices_fem, :num_aprendices_mas, :num_aprendices_nobin, :num_aprendices_matriculados, :num_aprendices_activos, :tipo_doc_empresa, :num_doc_empresa, :nombre_empresa
        )
        ON DUPLICATE KEY UPDATE ficha = ficha
    """)

    for idx, row in df_grupos.iterrows():
        try:
            result = db.execute(insert_grupo_sql, row.to_dict())
            if result.rowcount == 1:
                grupos_insertados += 1
            elif result.rowcount == 2:
                grupos_actualizados += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar grupo (índice {idx}): {e}"
            errores.append(msg)
            logger.error(f"Error al insertar: {e}")


    # Confirmar cambios
    db.commit()

    return {
        "programas_insertados": programas_insertados,
        "programas_actualizados": programas_actualizados,
        "municipios_insertados": municipios_insertados,
        "municipios_actualizados": municipios_actualizados,
        "estrategias_insertados": estrategias_insertados,
        "estrategias_actualizados": estrategias_actualizados,
        "centros_insertados": centros_insertados,
        "centros_actualizados": centros_actualizados,
        "grupos_insertados": grupos_insertados,
        "grupos_actualizados": grupos_actualizados,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }

# insertar dim_tiempo
def insertar_dim_tiempo(db: Session, dim_tiempo):
    dias_insertados = 0
    dias_actualizados = 0
    errores = []

    # 1. Insertar tiempo
    insert_programa_sql = text("""
        INSERT INTO dim_tiempo (
            fecha, anio, mes, nombre_mes, dia
        ) VALUES (
            :fecha, :anio, :mes, :nombre_mes, :dia
        )
    """)

    for idx, row in dim_tiempo.iterrows():
        try:
            result = db.execute(insert_programa_sql, row.to_dict())
            if result.rowcount == 1:
                dias_insertados += 1
            elif result.rowcount == 2:
                dias_actualizados += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar programa (índice {idx}): {e}"
            errores.append(msg)
            logger.error(f"Error al insertar: {e}")

    
    # Confirmar cambios
    db.commit()

    return {
        "dias_insertados": dias_insertados,
        "dias_actualizados": dias_actualizados,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }
