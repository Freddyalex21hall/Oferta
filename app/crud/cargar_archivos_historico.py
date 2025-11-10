from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging
import pandas as pd

logger = logging.getLogger(__name__)

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
        # Insertar/actualizar histórico
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
                # Convertir a dict y manejar valores None/NaN
                params = row.to_dict()
                params = {k: (None if pd.isna(v) else v) for k, v in params.items()}
                
                # Validar que id_grupo exista (campo requerido)
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