from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import logging
import pandas as pd

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
            # Preparar parametros y truncar strings según longitudes del esquema
            r = row.to_dict()
            def s(key):
                v = r.get(key)
                return None if pd.isna(v) else str(v).strip()

            # Longitudes según mi_db.sql (seguras a bytes puede fallar por utf8mb4, cortamos algo menos)
            max_len = {
                "cod_programa": 16,
                "cod_version": 20,
                "nombre_programa": 255,
                "tipo_formacion": 30,
                "nivel_formacion": 30,
                "edad_min_requerida": 2,
                "grado_min_requerido": 50,
                "resolucion": 250,
                "linea_tecnologica": 50,
                "red_tecnologica": 80,
                "red_conocimiento": 80,
                "modalidad": 30,
                "apuestas_prioritarias": 80,
                "tipo_permiso": 30,
                "indice": 240,  # truncar a 240 para margen con utf8mb4
                "ocupacion": 60,
                "url_pdf": 250,
            }

            params = {
                "cod_programa": s("cod_programa"),
                "PRF_version": r.get("PRF_version"),
                "cod_version": (s("cod_version")[:max_len["cod_version"]] if s("cod_version") else None),
                "nombre_programa": (s("nombre_programa")[:max_len["nombre_programa"]] if s("nombre_programa") else None),
                "tipo_formacion": (s("tipo_formacion")[:max_len["tipo_formacion"]] if s("tipo_formacion") else None),
                "nivel_formacion": (s("nivel_formacion")[:max_len["nivel_formacion"]] if s("nivel_formacion") else None),
                "duracion_maxima": r.get("duracion_maxima"),
                "dur_etapa_lectiva": r.get("dur_etapa_lectiva"),
                "dur_etapa_productiva": r.get("dur_etapa_productiva"),
                "fecha_registro": r.get("fecha_registro"),
                "fecha_activo": r.get("fecha_activo"),
                "edad_min_requerida": (s("edad_min_requerida")[:max_len["edad_min_requerida"]] if s("edad_min_requerida") else None),
                "grado_min_requerido": (s("grado_min_requerido")[:max_len["grado_min_requerido"]] if s("grado_min_requerido") else None),
                "descripcion_req": s("descripcion_req"),
                "resolucion": (s("resolucion")[:max_len["resolucion"]] if s("resolucion") else None),
                "fecha_resolucion": r.get("fecha_resolucion"),
                "apoyo_fic": (s("apoyo_fic")[:2] if s("apoyo_fic") else None),
                "creditos": r.get("creditos"),
                "alamedida": (s("alamedida")[:2] if s("alamedida") else None),
                "linea_tecnologica": (s("linea_tecnologica")[:max_len["linea_tecnologica"]] if s("linea_tecnologica") else None),
                "red_tecnologica": (s("red_tecnologica")[:max_len["red_tecnologica"]] if s("red_tecnologica") else None),
                "red_conocimiento": (s("red_conocimiento")[:max_len["red_conocimiento"]] if s("red_conocimiento") else None),
                "modalidad": (s("modalidad")[:max_len["modalidad"]] if s("modalidad") else None),
                "apuestas_prioritarias": (s("apuestas_prioritarias")[:max_len["apuestas_prioritarias"]] if s("apuestas_prioritarias") else None),
                "fic": (s("fic")[:2] if s("fic") else None),
                "tipo_permiso": (s("tipo_permiso")[:max_len["tipo_permiso"]] if s("tipo_permiso") else None),
                "multiple_inscripcion": (s("multiple_inscripcion")[:2] if s("multiple_inscripcion") else None),
                "indice": (s("indice")[:max_len["indice"]] if s("indice") else None),
                "ocupacion": (s("ocupacion")[:max_len["ocupacion"]] if s("ocupacion") else None),
                "estado": r.get("estado") if not pd.isna(r.get("estado")) else True,
                "url_pdf": (s("url_pdf")[:max_len["url_pdf"]] if s("url_pdf") else None),
            }

            result = db.execute(insert_programa_sql, params)
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


def insertar_datos_en_bd(db: Session, df_catalogos: pd.DataFrame):
    """
    Inserta o actualiza registros dentro de la tabla `catalogo`.
    Espera un DataFrame con al menos las columnas:
    - cod_catalogo (usado como identificador único)
    - nombre_catalogo
    Opcionalmente acepta: descripcion, estado.
    """
    if df_catalogos is None or df_catalogos.empty:
        return {
            "insertados": 0,
            "actualizados": 0,
            "errores": ["DataFrame vacío o no proporcionado"],
            "mensaje": "No hay datos para procesar"
        }

    insertados = 0
    actualizados = 0
    errores = []

    select_sql = text("""
        SELECT id_catalogo
        FROM catalogo
        WHERE cod_catalogo = :cod_catalogo
    """)

    insert_sql = text("""
        INSERT INTO catalogo (
            nombre_catalogo, descripcion, cod_catalogo, estado
        ) VALUES (
            :nombre_catalogo, :descripcion, :cod_catalogo, :estado
        )
    """)

    update_sql = text("""
        UPDATE catalogo
        SET nombre_catalogo = :nombre_catalogo,
            descripcion = :descripcion,
            estado = :estado
        WHERE cod_catalogo = :cod_catalogo
    """)

    for idx, row in df_catalogos.iterrows():
        try:
            cod_catalogo = row.get("cod_catalogo")
            nombre_catalogo = row.get("nombre_catalogo")

            if pd.isna(cod_catalogo) or cod_catalogo == "":
                errores.append(f"Fila {idx}: 'cod_catalogo' es obligatorio")
                continue

            if pd.isna(nombre_catalogo) or nombre_catalogo == "":
                errores.append(f"Fila {idx}: 'nombre_catalogo' es obligatorio")
                continue

            params = {
                "cod_catalogo": str(cod_catalogo).strip(),
                "nombre_catalogo": str(nombre_catalogo).strip(),
                "descripcion": str(row.get("descripcion")).strip() if pd.notna(row.get("descripcion")) else None,
                "estado": bool(row.get("estado")) if not pd.isna(row.get("estado")) else True
            }

            existente = db.execute(select_sql, {"cod_catalogo": params["cod_catalogo"]}).first()
            if existente:
                db.execute(update_sql, params)
                actualizados += 1
            else:
                db.execute(insert_sql, params)
                insertados += 1
        except SQLAlchemyError as e:
            msg = f"Error al procesar fila {idx} (cod_catalogo={row.get('cod_catalogo')}): {str(e)}"
            errores.append(msg)
            logger.error(msg)

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        errores.append(f"Error al confirmar la transacción: {str(e)}")

    return {
        "insertados": insertados,
        "actualizados": actualizados,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }


def insertar_municipios(db: Session, df_municipios: pd.DataFrame):
    """
    Inserta o actualiza registros de la tabla `municipios`.
    El DataFrame debe incluir las columnas `cod_municipio` y
    `nombre` o `nombre_municipio`.
    """
    if df_municipios is None or df_municipios.empty:
        return {
            "insertados": 0,
            "actualizados": 0,
            "errores": ["DataFrame vacío o no proporcionado"],
            "mensaje": "No hay datos para procesar"
        }

    insertados = 0
    actualizados = 0
    errores = []

    if "nombre" not in df_municipios.columns and "nombre_municipio" in df_municipios.columns:
        df_municipios = df_municipios.rename(columns={"nombre_municipio": "nombre"})

    columnas_requeridas = {"cod_municipio", "nombre"}
    if not columnas_requeridas.issubset(df_municipios.columns):
        faltantes = columnas_requeridas - set(df_municipios.columns)
        return {
            "insertados": 0,
            "actualizados": 0,
            "errores": [f"Columnas faltantes: {', '.join(faltantes)}"],
            "mensaje": "No se puede procesar el archivo"
        }

    df_municipios = df_municipios.dropna(subset=["cod_municipio"]).drop_duplicates(subset=["cod_municipio"])

    insert_sql = text("""
        INSERT INTO municipios (cod_municipio, nombre)
        VALUES (:cod_municipio, :nombre)
        ON DUPLICATE KEY UPDATE
            nombre = VALUES(nombre)
    """)

    for idx, row in df_municipios.iterrows():
        try:
            cod_municipio = str(row.get("cod_municipio")).strip() if pd.notna(row.get("cod_municipio")) else None
            nombre = str(row.get("nombre")).strip() if pd.notna(row.get("nombre")) else None

            if not cod_municipio:
                errores.append(f"Fila {idx}: 'cod_municipio' es obligatorio")
                continue

            params = {"cod_municipio": cod_municipio, "nombre": nombre}
            result = db.execute(insert_sql, params)
            if result.rowcount == 1:
                insertados += 1
            elif result.rowcount == 2:
                actualizados += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar municipio (índice {idx}, cod_municipio={row.get('cod_municipio')}): {str(e)}"
            errores.append(msg)
            logger.error(msg)

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        errores.append(f"Error al confirmar la transacción: {str(e)}")

    return {
        "insertados": insertados,
        "actualizados": actualizados,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }
