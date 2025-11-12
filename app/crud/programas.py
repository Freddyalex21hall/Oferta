from fastapi import HTTPException, logger
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional



def update_url_pdf(db: Session, cod: int, url: str) -> bool:
    try:

        query = text(f""" UPDATE programas_formacion SET url_pdf = :url_pdf 
                        WHERE cod_programa = :codigo """)
        db.execute(query, {"url_pdf": url, "codigo": cod})
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar usuario: {e}")
        raise Exception("Error de base de datos al actualizar el usuario")

def get_programa_by_cod(db: Session, cod: int):
    try:
        query = text("""SELECT * FROM programas_formacion
                        WHERE cod_programa = :codigo""")
        result = db.execute(query, {"codigo": cod}).mappings().first()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener programa: {e}")
        raise Exception("Error de base de datos al obtener el programa")