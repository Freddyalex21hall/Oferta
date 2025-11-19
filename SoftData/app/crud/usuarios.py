from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional
import logging

from app.schemas.usuarios import CrearUsuario, EditarPass, EditarUsuario, RetornoUsuario
from core.security import get_hashed_password, verify_password

logger = logging.getLogger(__name__)

def create_user(db: Session, user: CrearUsuario) -> Optional[bool]:
    try:
        dataUser = user.model_dump() # convierte el esquema en diccionario
        contraOrigin = dataUser["contra_encript"] # saca la contra original
        contraEncript = get_hashed_password(contraOrigin) # envia la contra original a encriptar 
        dataUser["contra_encript"] = contraEncript # remplaza la contra original por la encriptada

        query = text("""
            INSERT INTO usuario (
                nombre_completo, num_documento, 
                correo, contra_encript, id_rol,
                estado
            ) VALUES (
                :nombre_completo, :num_documento,
                :correo, :contra_encript, :id_rol,
                :estado
            )
        """)
        db.execute(query, dataUser)
        db.commit()

        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Error al crear usuario: {e}")
        raise Exception("Error de base de datos al crear el usuario")

def get_user_by_id(db: Session, id_usuario:int):
    try:
        query = text("""
            SELECT usuario.id_usuario, usuario.nombre_completo, 
                   usuario.num_documento, usuario.correo, usuario.id_rol, 
                   usuario.estado, rol.nombre_rol
            FROM usuario
            INNER JOIN rol ON usuario.id_rol = rol.id_rol
            WHERE usuario.id_usuario = :id_user
        """)

        result = db.execute(query, {"id_user": id_usuario}).mappings().first()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al bucar usuario por id: {e}")
        raise Exception("Error de base de datos al buscar el usuario")


def get_user_by_email(db: Session, un_correo:str):
    try:
        query = text("""
            SELECT usuario.id_usuario, usuario.nombre_completo, 
                   usuario.num_documento, usuario.correo, usuario.id_rol, 
                   usuario.estado, rol.nombre_rol
            FROM usuario
            INNER JOIN rol ON usuario.id_rol = rol.id_rol
            WHERE usuario.correo = :email
        """)

        result = db.execute(query, {"email": un_correo}).mappings().first()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al bucar usuario por email: {e}")
        raise Exception("Error de base de datos al buscar el usuario por correo")


def get_user_by_email_security(db: Session, un_correo:str):
    try:
        query = text("""
            SELECT usuario.id_usuario, usuario.nombre_completo, 
                   usuario.num_documento, usuario.contra_encript, 
                   usuario.correo, usuario.id_rol, 
                   usuario.estado, rol.nombre_rol
            FROM usuario
            INNER JOIN rol ON usuario.id_rol = rol.id_rol
            WHERE usuario.correo = :email
        """)

        result = db.execute(query, {"email": un_correo}).mappings().first()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al bucar usuario por email: {e}")
        raise Exception("Error de base de datos al buscar el usuario por correo")



def user_delete(db: Session, id:int):
    try:
        query = text("""
            DELETE FROM usuario
            WHERE usuario.id_usuario = :el_id
        """)

        db.execute(query, {"el_id": id})
        db.commit()
        
        return True
    
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar usuario por id: {e}")
        raise Exception("Error de base de datos al eliminar el usuario")


def update_user(db: Session, user_id: int, user_update: EditarUsuario) -> bool:
    try:
        fields = user_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{key} = :{key}" for key in fields])
        fields["user_id"] = user_id

        query = text(f"UPDATE usuario SET {set_clause} WHERE id_usuario = :user_id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar usuario: {e}")
        raise Exception("Error de base de datos al actualizar el usuario")



def update_password(db: Session, user_data: EditarPass) -> bool:
    try:
        datos_usuario = user_data.model_dump()
        contra_encript = get_hashed_password(datos_usuario['contra_nueva'])
        datos_usuario['pass_encript'] = contra_encript

        query = text(f""" UPDATE usuario SET contra_encript = :pass_encript 
                        WHERE id_usuario = :id_usuario """)
        db.execute(query, datos_usuario)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar usuario: {e}")
        raise Exception("Error de base de datos al actualizar el usuario")


def verify_user_pass(db: Session, user_data: EditarPass) -> bool:
    try:
        query = text("""
            SELECT usuario.contra_encript
            FROM usuario
            WHERE usuario.id_usuario = :id_user
        """)

        result = db.execute(query, {"id_user": user_data.id_usuario }).mappings().first()
        contra_en_db = result.contra_encript
        contra_anterior = user_data.contra_anterior
        print(contra_en_db)
        print(contra_anterior)

        validated = verify_password(contra_anterior, contra_en_db)

        if not validated:
            return False
        else:
            return True
    
    except SQLAlchemyError as e:
        logger.error(f"Error al bucar validar la contraseña: {e}")
        raise Exception("Error de base de datos al validar la contraseña")