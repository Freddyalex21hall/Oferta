from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import Optional

from app.schemas.programas_formacion import CrearPrograma, EditarPrograma

logger = logging.getLogger(__name__)


def _existing_columns(db: Session, table: str, candidates: list) -> list:
    """Devuelve la lista de columnas existentes de `candidates` en `table`."""
    try:
        # MySQL: usar information_schema
        query = text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = :table AND column_name IN :candidates
        """)
        rows = db.execute(query, {"table": table, "candidates": tuple(candidates)}).scalars().all()
        return rows
    except Exception:
        return []

def crear_programa(db: Session, programa: CrearPrograma) -> bool:
    try:
        data = programa.model_dump()
        query = text("""
            INSERT INTO programas_formacion (
                version, nombre, nivel, meses_duracion,
                duracion_programa, unidad_medida, estado,
                tipo_programa, url_pdf, red_conocimiento, programa_especial
            ) VALUES (
                :version, :nombre, :nivel, :meses_duracion,
                :duracion_programa, :unidad_medida, :estado,
                :tipo_programa, :url_pdf, :red_conocimiento, :programa_especial
            )
        """)
        db.execute(query, data)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error crear_programa: {e}")
        raise Exception("Error de base de datos al crear programa")

def listar_programas(db: Session):
    try:
        query = text("SELECT * FROM programas_formacion ORDER BY cod_programa ASC")
        rows = db.execute(query).mappings().all()
        # Map DB column names to API response fields expected by RetornoPrograma
        mapped = []
        for r in rows:
            mapped.append({
                "cod_programa": str(r.get("cod_programa")) if r.get("cod_programa") is not None else None,
                "version": r.get("cod_version") or (str(r.get("PRF_version")) if r.get("PRF_version") is not None else None),
                "nombre": r.get("nombre_programa") or r.get("nombre"),
                "nivel": r.get("nivel_formacion") or r.get("nivel"),
                "meses_duracion": r.get("duracion_maxima"),
                "duracion_programa": r.get("dur_etapa_productiva") or r.get("duracion_maxima"),
                "unidad_medida": r.get("alamedida") or r.get("unidad_medida"),
                "estado": r.get("estado"),
                "tipo_programa": r.get("tipo_formacion") or r.get("tipo_programa"),
                "url_pdf": r.get("url_pdf"),
                "red_conocimiento": r.get("red_conocimiento"),
                "programa_especial": r.get("programa_especial")
            })
        return mapped
    except SQLAlchemyError as e:
        logger.error(f"Error listar_programas: {e}")
        raise Exception("Error de base de datos al listar programas")

def obtener_programa_por_id(db: Session, cod_programa: int):
    try:
        query = text("SELECT * FROM programas_formacion WHERE cod_programa = :id")
        r = db.execute(query, {"id": cod_programa}).mappings().first()
        if not r:
            return None
        return {
            "cod_programa": str(r.get("cod_programa")) if r.get("cod_programa") is not None else None,
            "version": r.get("cod_version") or (str(r.get("PRF_version")) if r.get("PRF_version") is not None else None),
            "nombre": r.get("nombre_programa") or r.get("nombre"),
            "nivel": r.get("nivel_formacion") or r.get("nivel"),
            "meses_duracion": r.get("duracion_maxima"),
            "duracion_programa": r.get("dur_etapa_productiva") or r.get("duracion_maxima"),
            "unidad_medida": r.get("alamedida") or r.get("unidad_medida"),
            "estado": r.get("estado"),
            "tipo_programa": r.get("tipo_formacion") or r.get("tipo_programa"),
            "url_pdf": r.get("url_pdf"),
            "red_conocimiento": r.get("red_conocimiento"),
            "programa_especial": r.get("programa_especial")
        }
    except SQLAlchemyError as e:
        logger.error(f"Error obtener_programa_por_id: {e}")
        raise Exception("Error de base de datos al obtener programa")

def actualizar_programa(db: Session, cod_programa: int, data_update: EditarPrograma) -> bool:
    try:
        fields = data_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{k} = :{k}" for k in fields])
        fields["id"] = cod_programa
        query = text(f"UPDATE programas_formacion SET {set_clause} WHERE cod_programa = :id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error actualizar_programa: {e}")
        raise Exception("Error de base de datos al actualizar programa")

def eliminar_programa(db: Session, cod_programa: int) -> bool:
    try:
        query = text("DELETE FROM programas_formacion WHERE cod_programa = :id")
        db.execute(query, {"id": cod_programa})
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error eliminar_programa: {e}")
        raise Exception("Error de base de datos al eliminar programa")


# Funciones de solo lectura para consultas por campo y valores únicos
def get_programas_by_nivel(db: Session, nivel: str):
    try:
        # Determinar columnas existentes para nivel
        cols = _existing_columns(db, 'programas_formacion', ['nivel', 'nivel_formacion'])
        if not cols:
            return []
        if len(cols) == 1:
            col = cols[0]
            query = text(f"SELECT * FROM programas_formacion WHERE {col} = :nivel ORDER BY cod_programa ASC")
            rows = db.execute(query, {"nivel": nivel}).mappings().all()
        else:
            # Ambas existen
            query = text("SELECT * FROM programas_formacion WHERE nivel = :nivel OR nivel_formacion = :nivel ORDER BY cod_programa ASC")
            rows = db.execute(query, {"nivel": nivel}).mappings().all()
        mapped = []
        for r in rows:
            mapped.append({
                "cod_programa": str(r.get("cod_programa")) if r.get("cod_programa") is not None else None,
                "version": r.get("cod_version") or (str(r.get("PRF_version")) if r.get("PRF_version") is not None else None),
                "nombre": r.get("nombre_programa") or r.get("nombre"),
                "nivel": r.get("nivel_formacion") or r.get("nivel"),
                "meses_duracion": r.get("duracion_maxima"),
                "duracion_programa": r.get("dur_etapa_productiva") or r.get("duracion_maxima"),
                "unidad_medida": r.get("alamedida") or r.get("unidad_medida"),
                "estado": r.get("estado"),
                "tipo_programa": r.get("tipo_formacion") or r.get("tipo_programa"),
                "url_pdf": r.get("url_pdf"),
                "red_conocimiento": r.get("red_conocimiento"),
                "programa_especial": r.get("programa_especial")
            })
        return mapped
    except SQLAlchemyError as e:
        logger.error(f"Error get_programas_by_nivel: {e}")
        raise Exception("Error de base de datos al obtener programas por nivel")


def get_distinct_niveles(db: Session):
    try:
        cols = _existing_columns(db, 'programas_formacion', ['nivel', 'nivel_formacion'])
        if not cols:
            return []
        if len(cols) == 1:
            col = cols[0]
            query = text(f"SELECT DISTINCT {col} FROM programas_formacion WHERE {col} IS NOT NULL ORDER BY {col} ASC")
            rows = db.execute(query).scalars().all()
            return [r for r in rows]
        # Ambas existen
        query = text("SELECT DISTINCT COALESCE(nivel, nivel_formacion) AS nivel FROM programas_formacion WHERE COALESCE(nivel, nivel_formacion) IS NOT NULL ORDER BY nivel ASC")
        rows = db.execute(query).scalars().all()
        return [r for r in rows]
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_niveles: {e}")
        raise Exception("Error de base de datos al obtener niveles distintos")


def get_programas_by_tipo_programa(db: Session, tipo_programa: str):
    try:
        cols = _existing_columns(db, 'programas_formacion', ['tipo_programa', 'tipo_formacion'])
        if not cols:
            return []
        if len(cols) == 1:
            col = cols[0]
            query = text(f"SELECT * FROM programas_formacion WHERE {col} = :tipo_programa ORDER BY cod_programa ASC")
            rows = db.execute(query, {"tipo_programa": tipo_programa}).mappings().all()
        else:
            query = text("SELECT * FROM programas_formacion WHERE tipo_programa = :tipo_programa OR tipo_formacion = :tipo_programa ORDER BY cod_programa ASC")
            rows = db.execute(query, {"tipo_programa": tipo_programa}).mappings().all()
        mapped = []
        for r in rows:
            mapped.append({
                "cod_programa": str(r.get("cod_programa")) if r.get("cod_programa") is not None else None,
                "version": r.get("cod_version") or (str(r.get("PRF_version")) if r.get("PRF_version") is not None else None),
                "nombre": r.get("nombre_programa") or r.get("nombre"),
                "nivel": r.get("nivel_formacion") or r.get("nivel"),
                "meses_duracion": r.get("duracion_maxima"),
                "duracion_programa": r.get("dur_etapa_productiva") or r.get("duracion_maxima"),
                "unidad_medida": r.get("alamedida") or r.get("unidad_medida"),
                "estado": r.get("estado"),
                "tipo_programa": r.get("tipo_formacion") or r.get("tipo_programa"),
                "url_pdf": r.get("url_pdf"),
                "red_conocimiento": r.get("red_conocimiento"),
                "programa_especial": r.get("programa_especial")
            })
        return mapped
    except SQLAlchemyError as e:
        logger.error(f"Error get_programas_by_tipo_programa: {e}")
        raise Exception("Error de base de datos al obtener programas por tipo_programa")


def get_distinct_tipo_programa(db: Session):
    try:
        cols = _existing_columns(db, 'programas_formacion', ['tipo_programa', 'tipo_formacion'])
        if not cols:
            return []
        if len(cols) == 1:
            col = cols[0]
            query = text(f"SELECT DISTINCT {col} FROM programas_formacion WHERE {col} IS NOT NULL ORDER BY {col} ASC")
            return db.execute(query).scalars().all()
        # Ambas existen
        query = text("SELECT DISTINCT COALESCE(tipo_programa, tipo_formacion) AS tipo FROM programas_formacion WHERE COALESCE(tipo_programa, tipo_formacion) IS NOT NULL ORDER BY tipo ASC")
        return db.execute(query).scalars().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_tipo_programa: {e}")
        raise Exception("Error de base de datos al obtener tipos de programa distintos")


def get_programas_by_red_conocimiento(db: Session, red: str):
    try:
        query = text("SELECT * FROM programas_formacion WHERE red_conocimiento = :red ORDER BY cod_programa ASC")
        rows = db.execute(query, {"red": red}).mappings().all()
        mapped = []
        for r in rows:
            mapped.append({
                "cod_programa": str(r.get("cod_programa")) if r.get("cod_programa") is not None else None,
                "version": r.get("cod_version") or (str(r.get("PRF_version")) if r.get("PRF_version") is not None else None),
                "nombre": r.get("nombre_programa") or r.get("nombre"),
                "nivel": r.get("nivel_formacion") or r.get("nivel"),
                "meses_duracion": r.get("duracion_maxima"),
                "duracion_programa": r.get("dur_etapa_productiva") or r.get("duracion_maxima"),
                "unidad_medida": r.get("alamedida") or r.get("unidad_medida"),
                "estado": r.get("estado"),
                "tipo_programa": r.get("tipo_formacion") or r.get("tipo_programa"),
                "url_pdf": r.get("url_pdf"),
                "red_conocimiento": r.get("red_conocimiento"),
                "programa_especial": r.get("programa_especial")
            })
        return mapped
    except SQLAlchemyError as e:
        logger.error(f"Error get_programas_by_red_conocimiento: {e}")
        raise Exception("Error de base de datos al obtener programas por red_conocimiento")


def get_distinct_red_conocimiento(db: Session):
    try:
        query = text("SELECT DISTINCT red_conocimiento FROM programas_formacion WHERE red_conocimiento IS NOT NULL ORDER BY red_conocimiento ASC")
        return db.execute(query).scalars().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_red_conocimiento: {e}")
        raise Exception("Error de base de datos al obtener redes de conocimiento distintas")


def get_programas_by_estado(db: Session, estado: bool):
    try:
        query = text("SELECT * FROM programas_formacion WHERE estado = :estado ORDER BY cod_programa ASC")
        rows = db.execute(query, {"estado": estado}).mappings().all()
        mapped = []
        for r in rows:
            mapped.append({
                "cod_programa": str(r.get("cod_programa")) if r.get("cod_programa") is not None else None,
                "version": r.get("cod_version") or (str(r.get("PRF_version")) if r.get("PRF_version") is not None else None),
                "nombre": r.get("nombre_programa") or r.get("nombre"),
                "nivel": r.get("nivel_formacion") or r.get("nivel"),
                "meses_duracion": r.get("duracion_maxima"),
                "duracion_programa": r.get("dur_etapa_productiva") or r.get("duracion_maxima"),
                "unidad_medida": r.get("alamedida") or r.get("unidad_medida"),
                "estado": r.get("estado"),
                "tipo_programa": r.get("tipo_formacion") or r.get("tipo_programa"),
                "url_pdf": r.get("url_pdf"),
                "red_conocimiento": r.get("red_conocimiento"),
                "programa_especial": r.get("programa_especial")
            })
        return mapped
    except SQLAlchemyError as e:
        logger.error(f"Error get_programas_by_estado: {e}")
        raise Exception("Error de base de datos al obtener programas por estado")


def get_distinct_estados(db: Session):
    try:
        query = text("SELECT DISTINCT estado FROM programas_formacion ORDER BY estado DESC")
        return db.execute(query).scalars().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_estados: {e}")
        raise Exception("Error de base de datos al obtener estados distintos")
