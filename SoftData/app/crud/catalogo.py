from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List
import logging

from app.schemas.catalogo import CatalogoBase, EditarCatalogo

logger = logging.getLogger(__name__)

def create_catalogo(db: Session, catalogo: CatalogoBase) -> Optional[bool]:
    try:
        dataCatalogo = catalogo.model_dump() # convierte el esquema en diccionario
        
        query = text("""
            INSERT INTO catalogo (
                nombre_catalogo, descripcion, cod_catalogo, estado
            ) VALUES (
                :nombre_catalogo, :descripcion, :cod_catalogo, :estado
            )
        """)
        db.execute(query, dataCatalogo)
        db.commit()

        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Error al crear catálogo: {e}")
        raise Exception("Error de base de datos al crear el catálogo")

def get_catalogo_by_id(db: Session, id_catalogo: int):
    try:
        query = text("""
            SELECT id_catalogo, nombre_catalogo, descripcion, 
                   cod_catalogo, estado
            FROM catalogo
            WHERE id_catalogo = :id_cat
        """)

        result = db.execute(query, {"id_cat": id_catalogo}).mappings().first()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al buscar catálogo por id: {e}")
        raise Exception("Error de base de datos al buscar el catálogo")

def get_all_catalogos(db: Session, skip: int = 0, limit: int = 100):
    try:
        query = text("""
            SELECT id_catalogo, nombre_catalogo, descripcion, 
                   cod_catalogo, estado
            FROM catalogo
            ORDER BY id_catalogo DESC
            LIMIT :limit_val OFFSET :skip_val
        """)

        result = db.execute(query, {"limit_val": limit, "skip_val": skip}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener todos los catálogos: {e}")
        raise Exception("Error de base de datos al obtener los catálogos")

def get_catalogo_by_codigo(db: Session, cod_catalogo: str):
    try:
        query = text("""
            SELECT id_catalogo, nombre_catalogo, descripcion, 
                   cod_catalogo, estado
            FROM catalogo
            WHERE cod_catalogo = :cod_cat
        """)

        result = db.execute(query, {"cod_cat": cod_catalogo}).mappings().first()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al buscar catálogo por código: {e}")
        raise Exception("Error de base de datos al buscar el catálogo por código")

def update_catalogo(db: Session, catalogo_id: int, catalogo_update: EditarCatalogo) -> bool:
    try:
        fields = catalogo_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{key} = :{key}" for key in fields])
        fields["catalogo_id"] = catalogo_id

        query = text(f"UPDATE catalogo SET {set_clause} WHERE id_catalogo = :catalogo_id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar catálogo: {e}")
        raise Exception("Error de base de datos al actualizar el catálogo")

def delete_catalogo(db: Session, id: int) -> bool:
    try:
        query = text("""
            DELETE FROM catalogo
            WHERE id_catalogo = :el_id
        """)

        db.execute(query, {"el_id": id})
        db.commit()
        
        return True
    
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar catálogo por id: {e}")
        raise Exception("Error de base de datos al eliminar el catálogo")

