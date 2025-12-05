from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging

from app.schemas.registro_calificado import CrearRegistroCalificado, EditarRegistroCalificado

logger = logging.getLogger(__name__)


def crear_registro(db: Session, registro: CrearRegistroCalificado) -> bool:
    try:
        data = registro.model_dump()
        query = text("""
            INSERT INTO registro_calificado (
                cod_programa, tipo_tramite, fecha_radicado,
                numero_resolucion, fecha_resolucion, fecha_vencimiento,
                vigencia, modalidad, clasificacion, estado_catalogo
            ) VALUES (
                :cod_programa, :tipo_tramite, :fecha_radicado,
                :numero_resolucion, :fecha_resolucion, :fecha_vencimiento,
                :vigencia, :modalidad, :clasificacion, :estado_catalogo
            )
        """)
        db.execute(query, data)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error crear_registro: {e}")
        raise Exception("Error de base de datos al crear registro calificado")


def listar_registros(db: Session):
    try:
        query = text("SELECT * FROM registro_calificado ORDER BY cod_programa ASC")
        return db.execute(query).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error listar_registros: {e}")
        raise Exception("Error de base de datos al listar registros calificados")


def obtener_registro_por_id(db: Session, cod_programa: str):
    try:
        query = text("SELECT * FROM registro_calificado WHERE cod_programa = :id")
        return db.execute(query, {"id": cod_programa}).mappings().first()
    except SQLAlchemyError as e:
        logger.error(f"Error obtener_registro_por_id: {e}")
        raise Exception("Error de base de datos al obtener registro calificado")


def actualizar_registro(db: Session, cod_programa: str, data_update: EditarRegistroCalificado) -> bool:
    try:
        fields = data_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{k} = :{k}" for k in fields])
        fields["id"] = cod_programa
        query = text(f"UPDATE registro_calificado SET {set_clause} WHERE cod_programa = :id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error actualizar_registro: {e}")
        raise Exception("Error de base de datos al actualizar registro calificado")


def eliminar_registro(db: Session, cod_programa: str) -> bool:
    try:
        query = text("DELETE FROM registro_calificado WHERE cod_programa = :id")
        db.execute(query, {"id": cod_programa})
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error eliminar_registro: {e}")
        raise Exception("Error de base de datos al eliminar registro calificado")


# Funciones de consulta por campos y para obtener valores únicos (para select)
def get_registros_by_modalidad(db: Session, modalidad: str):
    try:
        query = text("SELECT * FROM registro_calificado WHERE modalidad = :modalidad ORDER BY cod_programa ASC")
        return db.execute(query, {"modalidad": modalidad}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_registros_by_modalidad: {e}")
        raise Exception("Error de base de datos al obtener registros por modalidad")


def get_distinct_modalidades(db: Session):
    try:
        query = text("SELECT DISTINCT modalidad FROM registro_calificado WHERE modalidad IS NOT NULL ORDER BY modalidad ASC")
        result = db.execute(query).scalars().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_modalidades: {e}")
        raise Exception("Error de base de datos al obtener modalidades distintas")


def get_registros_by_clasificacion(db: Session, clasificacion: str):
    try:
        query = text("SELECT * FROM registro_calificado WHERE clasificacion = :clasificacion ORDER BY cod_programa ASC")
        return db.execute(query, {"clasificacion": clasificacion}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_registros_by_clasificacion: {e}")
        raise Exception("Error de base de datos al obtener registros por clasificacion")


def get_distinct_clasificaciones(db: Session):
    try:
        query = text("SELECT DISTINCT clasificacion FROM registro_calificado WHERE clasificacion IS NOT NULL ORDER BY clasificacion ASC")
        result = db.execute(query).scalars().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_clasificaciones: {e}")
        raise Exception("Error de base de datos al obtener clasificaciones distintas")


def get_registros_by_vigencia(db: Session, vigencia: str):
    try:
        query = text("SELECT * FROM registro_calificado WHERE vigencia = :vigencia ORDER BY cod_programa ASC")
        return db.execute(query, {"vigencia": vigencia}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_registros_by_vigencia: {e}")
        raise Exception("Error de base de datos al obtener registros por vigencia")


def get_distinct_vigencias(db: Session):
    try:
        query = text("SELECT DISTINCT vigencia FROM registro_calificado WHERE vigencia IS NOT NULL ORDER BY vigencia ASC")
        result = db.execute(query).scalars().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_vigencias: {e}")
        raise Exception("Error de base de datos al obtener vigencias distintas")


def get_registros_by_estado_catalogo(db: Session, estado_catalogo: str):
    try:
        query = text("SELECT * FROM registro_calificado WHERE estado_catalogo = :estado_catalogo ORDER BY cod_programa ASC")
        return db.execute(query, {"estado_catalogo": estado_catalogo}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_registros_by_estado_catalogo: {e}")
        raise Exception("Error de base de datos al obtener registros por estado_catalogo")


def get_distinct_estado_catalogo(db: Session):
    try:
        query = text("SELECT DISTINCT estado_catalogo FROM registro_calificado WHERE estado_catalogo IS NOT NULL ORDER BY estado_catalogo ASC")
        result = db.execute(query).scalars().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_estado_catalogo: {e}")
        raise Exception("Error de base de datos al obtener estados de catalogo distintos")


def get_registros_by_tipo_tramite(db: Session, tipo_tramite: str):
    try:
        query = text("SELECT * FROM registro_calificado WHERE tipo_tramite = :tipo_tramite ORDER BY cod_programa ASC")
        return db.execute(query, {"tipo_tramite": tipo_tramite}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_registros_by_tipo_tramite: {e}")
        raise Exception("Error de base de datos al obtener registros por tipo_tramite")


def get_distinct_tipo_tramite(db: Session):
    try:
        query = text("SELECT DISTINCT tipo_tramite FROM registro_calificado WHERE tipo_tramite IS NOT NULL ORDER BY tipo_tramite ASC")
        result = db.execute(query).scalars().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_tipo_tramite: {e}")
        raise Exception("Error de base de datos al obtener tipos de tramite distintos")
