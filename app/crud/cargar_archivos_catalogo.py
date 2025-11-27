from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)

def insertar_catalogo_programas(db: Session, df_programas):
    programas_insertados = 0
    programas_actualizados = 0
    errores = []

    insert_programa_sql = text("""
        INSERT INTO programas_formacion (
            cod_programa, PRF_version, cod_version, nombre_programa, tipo_formacion, nivel_formacion, duracion_maxima, 
            dur_etapa_lectiva, dur_etapa_productiva, fecha_registro, fecha_activo,
            edad_min_requerida, grado_min_requerido, descripcion_req, resolucion, fecha_resolucion,
            apoyo_fic, creditos, alamedida, linea_tecnologica, red_tecnologica, red_conocimiento,
            modalidad, apuestas_prioritarias, fic, tipo_permiso, multiple_inscripcion, indice,
            ocupacion, estado, url_pdf
        ) VALUES (
            :cod_programa, :PRF_version, :cod_version, :nombre_programa, :tipo_formacion, :nivel_formacion, :duracion_maxima,
            :dur_etapa_lectiva, :dur_etapa_productiva, :fecha_registro, :fecha_activo,
            :edad_min_requerida, :grado_min_requerido, :descripcion_req, :resolucion, :fecha_resolucion,
            :apoyo_fic, :creditos, :alamedida, :linea_tecnologica, :red_tecnologica, :red_conocimiento,
            :modalidad, :apuestas_prioritarias, :fic, :tipo_permiso, :multiple_inscripcion, :indice,
            :ocupacion, :estado, :url_pdf
        )
        ON DUPLICATE KEY UPDATE 
            cod_version = VALUES(cod_version)
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

    # Confirmar cambios
    db.commit()

    return {
        "programas_insertados": programas_insertados,
        "programas_actualizados": programas_actualizados,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }
