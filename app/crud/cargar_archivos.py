# app/crud/cargar_archivos.py
import logging
import datetime
import pandas as pd
from dateutil import parser as dateutil_parser
from typing import Any, Dict
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
logging.getLogger("sqlalchemy").setLevel(logging.INFO)

# límites para MEDIUMINT UNSIGNED
MEDIUMINT_UNSIGNED_MAX = 16777215


def _safe_val(v: Any):
    """Normaliza valores pandas/extraños a tipos Python simples o None."""
    if v is None:
        return None
    # pandas NaN
    try:
        if isinstance(v, float) and (v != v):
            return None
    except Exception:
        pass
    # pandas Timestamp / datetime.datetime
    if isinstance(v, datetime.datetime):
        return v.date()
    # pandas Timestamp(date) may be datetime.date already
    if isinstance(v, datetime.date):
        return v
    # strings strip
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    return v


def _parse_date(v: Any):
    """Intenta convertir v a datetime.date; si falla retorna None."""
    v = _safe_val(v)
    if v is None:
        return None
    if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
        return v
    if isinstance(v, datetime.datetime):
        return v.date()
    # si viene string intentar parsear formatos comunes
    if isinstance(v, str):
        s = v.strip()
        # intento: si es sólo dígitos, puede ser número de serie de Excel
        if s.isdigit():
            try:
                iv = int(s)
                # rangos plausibles para fechas Excel modernas
                if 20000 <= iv <= 50000:
                    try:
                        dt = pd.to_datetime(iv, unit='D', origin='1899-12-30')
                        return dt.date()
                    except Exception:
                        pass
            except Exception:
                pass

        # formatos comunes rápidos
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y", "%d %b %Y", "%d %B %Y"):
            try:
                return datetime.datetime.strptime(s, fmt).date()
            except Exception:
                continue

        # Intentar dateutil (más flexible, acepta muchos formatos y meses en diferentes idiomas en algunos casos)
        try:
            dt = dateutil_parser.parse(s, dayfirst=True, fuzzy=True)
            return dt.date()
        except Exception:
            pass

        # Intentar con pandas como último recurso
        try:
            pdt = pd.to_datetime(s, dayfirst=True, errors='coerce', infer_datetime_format=True)
            if not pd.isna(pdt):
                return pdt.date()
        except Exception:
            pass
    return None


def _to_int_safe(value: Any):
    """Convierte a int si es posible, otherwise None."""
    if value is None:
        return None
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float):
        if value != value:
            return None
        try:
            return int(value)
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return None
        # eliminar separadores comunes
        s2 = s.replace(",", "").replace(".", "")
        if s2.isdigit():
            try:
                return int(s2)
            except Exception:
                return None
        # si tiene guiones u otros, intentar extraer dígitos
        digits = "".join(ch for ch in s2 if ch.isdigit())
        if digits:
            try:
                return int(digits)
            except Exception:
                return None
    return None


# Función existente: insertar_datos_en_bd
def insertar_datos_en_bd(db: Session, df_programas, df):
    """Inserta/actualiza programas (programas_formacion) y filas de grupos.

    Se asume que el router ya renombró columnas.
    """
    programas_insertados = 0
    programas_actualizados = 0
    grupos_insertados = 0
    grupos_actualizados = 0
    errores = []

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
            cod_programa = _safe_val(row.get("cod_programa"))
            data = {
                "cod_programa": cod_programa,
                "version": _safe_val(row.get("la_version")) or _safe_val(row.get("version")),
                "nombre": _safe_val(row.get("nombre")),
                "red_conocimiento": _safe_val(row.get("red_conocimiento")) if "red_conocimiento" in row.index else None,
            }
            db.execute(insert_programa_sql, data)
            # No confiar en rowcount de forma estricta para inserts con ON DUPLICATE
            programas_insertados += 1
        except SQLAlchemyError as e:
            db.rollback()
            msg = f"Error al insertar/actualizar programa (índice {idx}): {e}"
            errores.append(msg)
            logger.exception(msg)

    insert_grupo_sql = text("""
        INSERT INTO grupos (
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
            data_row = {}
            for k, v in row.items():
                data_row[k] = _safe_val(v)
            # enteros seguros
            for int_col in ["cod_ficha", "cod_centro"]:
                if int_col in data_row:
                    data_row[int_col] = _to_int_safe(data_row[int_col])
            # fechas
            for date_col in ["fecha_inicio", "fecha_fin"]:
                if date_col in data_row:
                    data_row[date_col] = _parse_date(data_row[date_col])
            db.execute(insert_grupo_sql, data_row)
            grupos_insertados += 1
        except SQLAlchemyError as e:
            db.rollback()
            msg = f"Error al insertar/actualizar grupo (índice {idx}): {e}"
            errores.append(msg)
            logger.exception(msg)

    try:
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        errores.append(f"Error al confirmar la transacción: {e}")
        logger.exception(f"Error al confirmar la transacción: {e}")

    return {
        "programas_insertados": programas_insertados,
        "programas_actualizados": programas_actualizados,
        "grupos_insertados": grupos_insertados,
        "grupos_actualizados": grupos_actualizados,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }

# insertar_estado_normas

def insertar_estado_normas(db: Session, df_normas):
    """
    Inserta registros en la tabla estado_de_normas desde un DataFrame.
    El router ya renombra las columnas, aquí solo se insertan los valores.
    """

    insert_sql = text("""
        INSERT INTO estado_de_normas (
            cod_programa, cod_version, fecha_elaboracion, anio, red_conocimiento,
            nombre_ncl, cod_ncl, ncl_version, norma_corte_noviembre,
            version, norma_version, mesa_sectorial, tipo_norma,
            observacion, fecha_revision, tipo_competencia, vigencia, fecha_indice
        ) VALUES (
            :cod_programa, :cod_version, :fecha_elaboracion, :anio, :red_conocimiento,
            :nombre_ncl, :cod_ncl, :ncl_version, :norma_corte_noviembre,
            :version, :norma_version, :mesa_sectorial, :tipo_norma,
            :observacion, :fecha_revision, :tipo_competencia, :vigencia, :fecha_indice
        )
    """)

    insertados = 0
    errores = []

    # Aceptar tanto Series como dicts
    if hasattr(df_normas, "to_dict"):
        row = df_normas.to_dict()
    elif isinstance(df_normas, dict):
        row = df_normas
    else:
        # intentar convertir
        try:
            row = dict(df_normas)
        except Exception:
            row = {}

    def _get(r, *keys):
        for k in keys:
            if k in r:
                return r[k]
        return None

    # Normalizar y parsear los valores esperados
    try:
        cod_programa_raw = _get(row, 'cod_programa', 'COD PROGRAMA', 'cod_programa')
        cod_programa = _safe_val(cod_programa_raw)

        cod_version = _safe_val(_get(row, 'cod_version', 'CODIGO VERSION', 'cod_version'))
        fecha_elaboracion = _parse_date(_get(row, 'fecha_elaboracion', 'Fecha Elaboracion', 'FECHA ELABORACION'))
        anio = _to_int_safe(_safe_val(_get(row, 'anio', 'Año', 'ANIO')))
        red_conocimiento = _safe_val(_get(row, 'red_conocimiento', 'RED CONOCIMIENTO'))
        nombre_ncl = _safe_val(_get(row, 'nombre_ncl', 'NOMBRE_NCL', 'NOMBRE NCL'))
        cod_ncl = _to_int_safe(_safe_val(_get(row, 'cod_ncl', 'NCL CODIGO', 'NCL_CODIGO')))
        ncl_version = _to_int_safe(_safe_val(_get(row, 'ncl_version', 'NCL VERSION', 'NCL_VERSION')))
        norma_corte_noviembre = _safe_val(_get(row, 'norma_corte_noviembre', 'Norma corte a NOVIEMBRE'))
        version = _safe_val(_get(row, 'version', 'Versión', 'VERSION'))
        norma_version = _safe_val(_get(row, 'norma_version', 'Norma - Versión', 'NORMA - VERSION'))
        mesa_sectorial = _safe_val(_get(row, 'mesa_sectorial', 'Mesa Sectorial'))
        tipo_norma = _safe_val(_get(row, 'tipo_norma', 'Tipo de Norma'))
        observacion = _safe_val(_get(row, 'observacion', 'Observación', 'OBSERVACION'))
        fecha_revision = _parse_date(_get(row, 'fecha_revision', 'Fecha de revisión', 'FECHA DE REVISION'))
        tipo_competencia = _safe_val(_get(row, 'tipo_competencia', 'Tipo de competencia'))
        vigencia = _safe_val(_get(row, 'vigencia', 'Vigencia'))
        fecha_indice = _parse_date(_get(row, 'fecha_elaboracion_2', 'Fecha de Elaboración', 'fecha_elaboracion_2'))

        # Si no se obtuvo `fecha_elaboracion`, intentar usar la segunda columna alternativa
        if fecha_elaboracion is None and fecha_indice is not None:
            fecha_elaboracion = fecha_indice

        # Si aún no hay fecha válida, reportar error y no intentar insertar (columna NOT NULL en BD)
        if fecha_elaboracion is None:
            errores.append({"error": "fecha_elaboracion ausente o no parseable"})
            return {
                "mensaje": "Carga finalizada",
                "registros_cargados": insertados,
                "errores": errores
            }

        data = {
            "cod_programa": cod_programa,
            "cod_version": cod_version,
            "fecha_elaboracion": fecha_elaboracion,
            "anio": anio,
            "red_conocimiento": red_conocimiento,
            "nombre_ncl": nombre_ncl,
            "cod_ncl": cod_ncl,
            "ncl_version": ncl_version,
            "norma_corte_noviembre": norma_corte_noviembre,
            "version": version,
            "norma_version": norma_version,
            "mesa_sectorial": mesa_sectorial,
            "tipo_norma": tipo_norma,
            "observacion": observacion,
            "fecha_revision": fecha_revision,
            "tipo_competencia": tipo_competencia,
            "vigencia": vigencia,
            "fecha_indice": fecha_indice,
        }

        # Ejecutar INSERT
        try:
            # Truncar campos que puedan exceder el tamaño de la columna
            if data.get("nombre_ncl") and isinstance(data.get("nombre_ncl"), str):
                data["nombre_ncl"] = data["nombre_ncl"][:150]

            db.execute(insert_sql, data)
            insertados += 1
        except IntegrityError as ie:
            # IntegrityError, intentar crear placeholder en programas_formacion si es FK faltante
            errstr = str(ie.__dict__.get('orig') or ie)
            logger.warning(f"IntegrityError al insertar norma: {errstr}; intentando crear placeholder de programa")
            try:
                # Preparar código de programa como string
                cp = data.get("cod_programa")
                if cp is None:
                    raise Exception("cod_programa ausente, no se puede crear placeholder")
                cod_prog_str = str(cp)
                placeholder_sql = text("""
                    INSERT IGNORE INTO programas_formacion (
                        cod_programa, nombre_programa, nivel_formacion, estado
                    ) VALUES (
                        :cod_programa, :nombre_programa, :nivel_formacion, :estado
                    )
                """)
                ph_data = {
                    "cod_programa": cod_prog_str,
                    "nombre_programa": f"AUTO-CREATED {cod_prog_str}",
                    "nivel_formacion": data.get("nivel_formacion") or None,
                    "estado": True
                }
                db.execute(placeholder_sql, ph_data)
                db.commit()
                # Reintentar la inserción
                try:
                    db.execute(insert_sql, data)
                    insertados += 1
                except SQLAlchemyError as e2:
                    db.rollback()
                    err2 = str(e2.__dict__.get('orig') or e2)
                    errores.append({"error": err2})
                    logger.exception(f"Error insertando norma tras crear placeholder: {err2}")
            except Exception as e_ph:
                db.rollback()
                errph = str(e_ph)
                errores.append({"error": errstr})
                logger.exception(f"No se pudo crear placeholder para programa: {errph}")
        except SQLAlchemyError as e:
            db.rollback()
            errstr = str(e.__dict__.get('orig') or e)
            errores.append({"error": errstr})
            logger.exception(f"Error insertando norma en fila: {errstr}")

    except Exception as e:
        # error al preparar datos
        errstr = str(e)
        errores.append({"error_prepare": errstr})
        logger.exception(f"Error preparando datos para insertar norma: {errstr}")

    # Confirmar la transacción
    try:
        db.commit()

    except SQLAlchemyError as e:
        db.rollback()
        errores.append({"error_commit": str(e)})
        logger.exception("Error al confirmar la transacción")

    return {
        "mensaje": "Carga finalizada",
        "registros_cargados": insertados,
        "errores": errores
    }

