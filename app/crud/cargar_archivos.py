# app/crud/cargar_archivos.py
import logging
import datetime
from typing import Any, Dict
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
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
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"):
            try:
                return datetime.datetime.strptime(v, fmt).date()
            except Exception:
                continue
        # intento final con fromisoformat (acepta YYYY-MM-DD y variantes)
        try:
            return datetime.date.fromisoformat(v)
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
            cod_programa = _to_int_safe(row.get("cod_programa"))
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
            for int_col in ["cod_ficha", "cod_centro", "cod_programa"]:
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

    df = df_normas.to_dict()

    print(df['cod_programa'])

    try:
            
        data = {
            "cod_programa": df['cod_programa'],
            "cod_version": df['cod_version'],
            "fecha_elaboracion": df['fecha_elaboracion'],
            "anio": df['anio'],
            "red_conocimiento": df['red_conocimiento'],
            "nombre_ncl": df['nombre_ncl'],
            "cod_ncl": df['cod_ncl'],
            "ncl_version": df['ncl_version'],
            "norma_corte_noviembre": df['norma_corte_noviembre'],
            "version": df['version'],
            "norma_version": df['norma_version'],
            "mesa_sectorial": df['mesa_sectorial'],
            "tipo_norma": df['tipo_norma'],
            "observacion": df['observacion'],
            "fecha_revision": df['fecha_revision'],
            "tipo_competencia": df['tipo_competencia'],
            "vigencia": df['vigencia'],
            "fecha_indice": df['fecha_elaboracion_2'],
        }

        db.execute(insert_sql, data)
        insertados += 1

    except SQLAlchemyError as e:
        db.rollback()
        errores.append(
            {"error": str(e)}
        )
        logger.exception(f"Error insertando norma en fila: {e}")

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

