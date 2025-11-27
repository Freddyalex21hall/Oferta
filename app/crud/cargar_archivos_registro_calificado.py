from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def insertar_registro_calificado_en_bd(db: Session, df_registros: pd.DataFrame):
    """
    Inserta o actualiza registros en la tabla `registro_calificado`.
    Se usa `cod_programa` como clave primaria para INSERT ... ON DUPLICATE KEY UPDATE.
    """
    insertados = 0
    actualizados = 0
    errores = []

    insert_sql = text("""
        INSERT INTO registro_calificado (
            cod_programa, tipo_tramite, fecha_radicado,
            numero_resolucion, fecha_resolucion, fecha_vencimiento,
            vigencia, modalidad, clasificacion, estado_catalogo
        ) VALUES (
            :cod_programa, :tipo_tramite, :fecha_radicado,
            :numero_resolucion, :fecha_resolucion, :fecha_vencimiento,
            :vigencia, :modalidad, :clasificacion, :estado_catalogo
        )
        ON DUPLICATE KEY UPDATE
            tipo_tramite = VALUES(tipo_tramite),
            fecha_radicado = VALUES(fecha_radicado),
            numero_resolucion = VALUES(numero_resolucion),
            fecha_resolucion = VALUES(fecha_resolucion),
            fecha_vencimiento = VALUES(fecha_vencimiento),
            vigencia = VALUES(vigencia),
            modalidad = VALUES(modalidad),
            clasificacion = VALUES(clasificacion),
            estado_catalogo = VALUES(estado_catalogo)
    """)

    for idx, row in df_registros.iterrows():
        try:
            params = {
                "cod_programa": int(row["cod_programa"]) if pd.notna(row.get("cod_programa")) else None,
                "tipo_tramite": str(row.get("tipo_tramite")) if pd.notna(row.get("tipo_tramite")) else None,
                "fecha_radicado": row.get("fecha_radicado") if pd.notna(row.get("fecha_radicado")) else None,
                "numero_resolucion": int(row["numero_resolucion"]) if "numero_resolucion" in row and pd.notna(row.get("numero_resolucion")) else None,
                "fecha_resolucion": row.get("fecha_resolucion") if pd.notna(row.get("fecha_resolucion")) else None,
                "fecha_vencimiento": row.get("fecha_vencimiento") if pd.notna(row.get("fecha_vencimiento")) else None,
                "vigencia": str(row.get("vigencia")) if pd.notna(row.get("vigencia")) else None,
                "modalidad": str(row.get("modalidad")) if pd.notna(row.get("modalidad")) else None,
                "clasificacion": str(row.get("clasificacion")) if pd.notna(row.get("clasificacion")) else None,
                "estado_catalogo": str(row.get("estado_catalogo")) if pd.notna(row.get("estado_catalogo")) else None,
            }

            if not params["cod_programa"]:
                errores.append(f"Fila {idx}: cod_programa inválido o ausente")
                continue

            result = db.execute(insert_sql, params)
            if result.rowcount == 1:
                insertados += 1
            elif result.rowcount == 2:
                actualizados += 1

        except SQLAlchemyError as e:
            msg = f"Error al insertar/actualizar registro calificado (índice {idx}, cod_programa: {row.get('cod_programa', 'N/A')}): {str(e)}"
            errores.append(msg)
            logger.error(msg)

    # Commit once after loop
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        errores.append(f"Error al hacer commit: {str(e)}")

    return {
        "insertados": insertados,
        "actualizados": actualizados,
        "total_errores": len(errores),
        "errores": errores,
        "exitoso": len(errores) == 0,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }
