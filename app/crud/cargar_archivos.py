from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def insertar_estado_normas(db:Session, df):
    
    normas_insertadas = 0
    errores = []

    insert_normas_sql = text("""
        INSERT INTO estado_de_normas (
            COD PROGRAMA,VERSIÓN PROG,CODIGO VERSION,TIPO PROGRAMA,NIVEL DE FORMACIÓN,NOMBRE PROGRAMA,ESTADO PROGRAMA,Fecha Elaboracion,Año,RED CONOCIMIENTO,NOMBRE_NCL,NCL CODIGO,NCL VERSION,Norma corte a NOVIEMBRE,Versión,Norma - Versión,Mesa Sectorial,Tipo de Norma,Observación,Fecha de revisión,Tipo de competencia,Vigencia,Fecha de Elaboración
        ) VALUES (
            :COD PROGRAMA,:VERSIÓN PROG,:CODIGO VERSION,:TIPO PROGRAMA,:NIVEL DE FORMACIÓN,:NOMBRE PROGRAMA,:ESTADO PROGRAMA,:Fecha Elaboracion,:Año,:RED CONOCIMIENTO,:NOMBRE_NCL,:NCL CODIGO,:NCL VERSION,:Norma corte a NOVIEMBRE,:Versión,:Norma - Versión,:Mesa Sectorial,:Tipo de Norma,:Observación,:Fecha de revisión,:Tipo de competencia,:Vigencia,:Fecha de Elaboración
        )
    """)

    for idx, row in df.iterrows():
        try:
            result = db.execute(insert_normas_sql, row.to_dict())
            if result.rowcount == 1:
                normas += 1
        except SQLAlchemyError as e:
            msg = f"Error al insertar egresado (índice {idx}): {e}"
            errores.append(msg)
            logger.error(f"Error al insertar: {e}")

    db.commit()
    return {
        "normas_insertadas": normas_insertadas,
        "errores": errores,
        "mensaje": "Carga completada con errores" if errores else "Carga completada exitosamente"
    }