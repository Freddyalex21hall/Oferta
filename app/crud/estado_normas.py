from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)


#   CREAR REGISTRO

def crear_estado_norma(db: Session, data: dict):
    try:
        query = text("""
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

        db.execute(query, data)
        db.commit()
        return {"mensaje": "Registro creado correctamente"}

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error crear_estado_norma: {e}")
        raise Exception(str(e))



#   LISTAR

def listar_estado_normas(db: Session):
    try:
        query = text("SELECT * FROM estado_de_normas ORDER BY id_estado_norma ASC")
        return db.execute(query).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error listar_estado_normas: {e}")
        raise Exception(str(e))



#   OBTENER POR ID

def obtener_estado_norma(db: Session, id_norma: int):
    try:
        query = text("""
            SELECT * FROM estado_de_normas
            WHERE id_estado_norma = :id
        """)
        return db.execute(query, {"id": id_norma}).mappings().first()
    except SQLAlchemyError as e:
        logger.error(f"Error obtener_estado_norma: {e}")
        raise Exception(str(e))



#   ACTUALIZAR

def actualizar_estado_norma(db: Session, id_norma: int, data_update: dict):
    try:
        if not data_update:
            return False

        set_clause = ", ".join([f"{k} = :{k}" for k in data_update])
        data_update["id"] = id_norma

        query = text(f"""
            UPDATE estado_de_normas
            SET {set_clause}
            WHERE id_estado_norma = :id
        """)

        db.execute(query, data_update)
        db.commit()
        return True

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error actualizar_estado_norma: {e}")
        raise Exception(str(e))



#   ELIMINAR

def eliminar_estado_norma(db: Session, id_norma: int):
    try:
        query = text("""
            DELETE FROM estado_de_normas
            WHERE id_estado_norma = :id
        """)

        db.execute(query, {"id": id_norma})
        db.commit()
        return True

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error eliminar_estado_norma: {e}")
        raise Exception(str(e))


# Funciones de solo lectura para consultas por campo y valores distintos (selects)
def get_estado_by_cod_programa(db: Session, cod_programa: int):
    try:
        query = text("SELECT * FROM estado_de_normas WHERE cod_programa = :cod_programa ORDER BY id_estado_norma ASC")
        return db.execute(query, {"cod_programa": cod_programa}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_estado_by_cod_programa: {e}")
        raise Exception(str(e))


def get_distinct_anios(db: Session):
    try:
        query = text("SELECT DISTINCT anio FROM estado_de_normas WHERE anio IS NOT NULL ORDER BY anio ASC")
        return db.execute(query).scalars().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_anios: {e}")
        raise Exception(str(e))


def get_estado_by_anio(db: Session, anio: int):
    try:
        query = text("SELECT * FROM estado_de_normas WHERE anio = :anio ORDER BY id_estado_norma ASC")
        return db.execute(query, {"anio": anio}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_estado_by_anio: {e}")
        raise Exception(str(e))


def get_distinct_vigencias(db: Session):
    try:
        query = text("SELECT DISTINCT vigencia FROM estado_de_normas WHERE vigencia IS NOT NULL ORDER BY vigencia ASC")
        return db.execute(query).scalars().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_vigencias: {e}")
        raise Exception(str(e))


def get_estado_by_vigencia(db: Session, vigencia: str):
    try:
        query = text("SELECT * FROM estado_de_normas WHERE vigencia = :vigencia ORDER BY id_estado_norma ASC")
        return db.execute(query, {"vigencia": vigencia}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_estado_by_vigencia: {e}")
        raise Exception(str(e))


def get_distinct_tipo_norma(db: Session):
    try:
        query = text("SELECT DISTINCT tipo_norma FROM estado_de_normas WHERE tipo_norma IS NOT NULL ORDER BY tipo_norma ASC")
        return db.execute(query).scalars().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_tipo_norma: {e}")
        raise Exception(str(e))


def get_estado_by_tipo_norma(db: Session, tipo_norma: str):
    try:
        query = text("SELECT * FROM estado_de_normas WHERE tipo_norma = :tipo_norma ORDER BY id_estado_norma ASC")
        return db.execute(query, {"tipo_norma": tipo_norma}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_estado_by_tipo_norma: {e}")
        raise Exception(str(e))


def get_distinct_mesa_sectorial(db: Session):
    try:
        query = text("SELECT DISTINCT mesa_sectorial FROM estado_de_normas WHERE mesa_sectorial IS NOT NULL ORDER BY mesa_sectorial ASC")
        return db.execute(query).scalars().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_distinct_mesa_sectorial: {e}")
        raise Exception(str(e))


def get_estado_by_mesa_sectorial(db: Session, mesa: str):
    try:
        query = text("SELECT * FROM estado_de_normas WHERE mesa_sectorial = :mesa ORDER BY id_estado_norma ASC")
        return db.execute(query, {"mesa": mesa}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error get_estado_by_mesa_sectorial: {e}")
        raise Exception(str(e))
