from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging
import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _safe_val(v: Any):
    """Convierte valores pandas/extraños a tipos python simples."""
    if v is None:
        return None
    try:
        # pandas NaT/NaN vienen como float('nan') o datetime-like; tratar como None
        if isinstance(v, float) and (v != v):
            return None
    except Exception:
        pass
    # datetime.date / datetime.datetime -> date
    if isinstance(v, datetime.datetime):
        return v.date()
    return v


def insertar_datos_en_bd(db: Session, df_programas, df):
    """Inserta/actualiza programas (tabla `programas_formacion`) y filas de `grupo`.

    El router ya renombra columnas; aquí se asumen las mismas claves.
    """
    programas_insertados = 0
    programas_actualizados = 0
    grupos_insertados = 0
    grupos_actualizados = 0
    errores = []

    # 1. Insertar/actualizar programas (tabla correcta: programas_formacion)
    insert_programa_sql = text("""
        INSERT INTO programas_formacion (
            cod_programa, version, nombre, red_conocimiento
        ) VALUES (
            :cod_programa, :version, :nombre, :red_conocimiento
        )
        ON DUPLICATE KEY UPDATE
            version = VALUES(version),
            nombre = VALUES(nombre)
    """)

    for idx, row in df_programas.iterrows():
        try:
            data = {
                "cod_programa": None if row.get("cod_programa") is None else int(row.get("cod_programa")),
                "version": _safe_val(row.get("la_version")),
                "nombre": _safe_val(row.get("nombre")),
                "red_conocimiento": _safe_val(row.get("red_conocimiento")) if "red_conocimiento" in row.index else None,
            }
            result = db.execute(insert_programa_sql, data)
            if hasattr(result, "rowcount"):
                if result.rowcount == 1:
                    programas_insertados += 1
                elif result.rowcount == 2:
                    programas_actualizados += 1
        except SQLAlchemyError as e:
            db.rollback()
            msg = f"Error al insertar/actualizar programa (índice {idx}): {e}"
            errores.append(msg)
            logger.error(msg)

    # 2. Insertar/actualizar grupos (se asume existencia de tabla `grupo` con columnas usadas por el router)
    insert_grupo_sql = text("""
        INSERT INTO grupo (
            cod_ficha, cod_centro, cod_programa, la_version, estado_grupo,
            nombre_nivel, jornada, fecha_inicio, fecha_fin, etapa,
            modalidad, responsable, nombre_empresa, nombre_municipio,
            nombre_programa_especial, hora_inicio, hora_fin, aula_actual
        ) VALUES (
            :cod_ficha, :cod_centro, :cod_programa, :la_version, :estado_grupo,
            :nombre_nivel, :jornada, :fecha_inicio, :fecha_fin, :etapa,
            :modalidad, :responsable, :nombre_empresa, :nombre_municipio,
            :nombre_programa_especial, :hora_inicio, :hora_fin, :aula_actual
        )
        ON DUPLICATE KEY UPDATE
            estado_grupo = VALUES(estado_grupo),
            etapa = VALUES(etapa),
            responsable = VALUES(responsable),
            nombre_programa_especial = VALUES(nombre_programa_especial),
            hora_inicio = VALUES(hora_inicio),
            hora_fin = VALUES(hora_fin),
            aula_actual = VALUES(aula_actual)
    """)

    for idx, row in df.iterrows():
        try:
            # Normalizar fila a dict simple
            data_row = {}
            for k, v in row.items():
                data_row[k] = _safe_val(v)

            # forzar enteros donde corresponda
            for int_col in ["cod_ficha", "cod_centro", "cod_programa"]:
                if int_col in data_row and data_row[int_col] is not None:
                    try:
                        data_row[int_col] = int(data_row[int_col])
                    except Exception:
                        data_row[int_col] = None

            result = db.execute(insert_grupo_sql, data_row)
            if hasattr(result, "rowcount"):
                if result.rowcount == 1:
                    grupos_insertados += 1
                elif result.rowcount == 2:
                    grupos_actualizados += 1
        except SQLAlchemyError as e:
            db.rollback()
            msg = f"Error al insertar/actualizar grupo (índice {idx}): {e}"
            errores.append(msg)
            logger.error(msg)

    # Confirmar cambios
    try:
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        errores.append(f"Error al confirmar la transacción: {e}")
        logger.error(f"Error al confirmar la transacción: {e}")

    return {
        "programas_insertados": programas_insertados,
        "programas_actualizados": programas_actualizados,
        "grupos_insertados": grupos_insertados,
        "grupos_actualizados": grupos_actualizados,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }
