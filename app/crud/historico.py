from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List
import logging


logger = logging.getLogger(__name__)

def get_all_historicos(db: Session, skip: int = 0, limit: int = 100) -> List[dict]:
    try:
        query = text("""
            SELECT 
                grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
                grupos.jornada, grupos.etapa_ficha, grupos.estado_curso, grupos.fecha_inicio,
                grupos.fecha_fin, grupos.cod_municipio, grupos.cod_estrategia, grupos.cupo_asignado,
                grupos.num_aprendices_matriculados, grupos.num_aprendices_activos,
                historico.id_historico, historico.id_grupo,
                historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            INNER JOIN grupos ON historico.id_grupo = grupos.ficha
            LIMIT :limit OFFSET :skip
        """)

        result = db.execute(query, {"limit": limit, "skip": skip}).mappings().all()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historicos: {e}")
        raise Exception("Error de base de datos al obtener los historicos")

def get_historico_by_id(db: Session, id_historico: int):
    try:
        query = text("""
            SELECT 
                grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
                grupos.jornada, grupos.etapa_ficha, grupos.estado_curso, grupos.fecha_inicio,
                grupos.fecha_fin, grupos.cod_municipio, grupos.cod_estrategia, grupos.cupo_asignado,
                grupos.num_aprendices_matriculados, grupos.num_aprendices_activos,
                historico.id_historico, historico.id_grupo,
                historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            INNER JOIN grupos ON historico.id_grupo = grupos.ficha
            WHERE historico.id_historico = :id_historico
        """)

        result = db.execute(query, {"id_historico": id_historico}).mappings().first()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al buscar historico por id: {e}")
        raise Exception("Error de base de datos al buscar el historico")




def get_historicos_by_grupo(db: Session, id_grupo: int) -> List[dict]:
    try:
        query = text("""
            SELECT 
                grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
                grupos.jornada, grupos.etapa_ficha, grupos.estado_curso, grupos.fecha_inicio,
                grupos.fecha_fin, grupos.cod_municipio, grupos.cod_estrategia, grupos.cupo_asignado,
                grupos.num_aprendices_matriculados, grupos.num_aprendices_activos,
                historico.id_historico, historico.id_grupo,
                historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            INNER JOIN grupos ON historico.id_grupo = grupos.ficha
            WHERE historico.id_grupo = :id_grupo
        """)

        result = db.execute(query, {"id_grupo": id_grupo}).mappings().all()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historicos por grupo: {e}")
        raise Exception("Error de base de datos al obtener los historicos por grupo")


def get_historico_by_ficha(db: Session, ficha: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a una ficha específica.
    La ficha es única y corresponde al id_grupo en la tabla historico.
    """
    try:
        query = text("""
            SELECT 
                grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
                grupos.jornada, grupos.etapa_ficha, grupos.estado_curso, grupos.fecha_inicio,
                grupos.fecha_fin, grupos.cod_municipio, grupos.cod_estrategia, grupos.cupo_asignado,
                grupos.num_aprendices_matriculados, grupos.num_aprendices_activos,
                historico.id_historico, historico.id_grupo,
                historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            INNER JOIN grupos ON historico.id_grupo = grupos.ficha
            WHERE historico.id_grupo = :ficha
        """)

        result = db.execute(query, {"ficha": ficha}).mappings().first()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por ficha: {e}")
        raise Exception("Error de base de datos al obtener el historico por ficha")


def get_historico_by_cod_programa(db: Session, cod_programa: str) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un codigo de programa específico.
    El cod_programa es única y corresponde al cod_programa en la tabla grupos. Para poder consultar el programa que esta en el historico
    """
    try:
        query = text("""
            SELECT 
                grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
                grupos.jornada, grupos.etapa_ficha, grupos.estado_curso, grupos.fecha_inicio,
                grupos.fecha_fin, grupos.cod_municipio, grupos.cod_estrategia, grupos.cupo_asignado,
                grupos.num_aprendices_matriculados, grupos.num_aprendices_activos,
                historico.id_historico, historico.id_grupo,
                historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            INNER JOIN grupos ON historico.id_grupo = grupos.ficha
            WHERE grupos.cod_programa = :cod_programa
        """)

        result = db.execute(query, {"cod_programa": cod_programa}).mappings().first()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por codigo programa: {e}")
        raise Exception("Error de base de datos al obtener el historico por codigo de programa")

def get_historico_by_cod_centro(db: Session, cod_centro: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un codigo de centro en especifico.
    El codigo de centro corresponde al cod_centro en la tabla grupos para consultar el historico. Para poder consultar el centro que esta en el historico
    """
    try:
        query = text("""
            SELECT 
                grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
                grupos.jornada, grupos.etapa_ficha, grupos.estado_curso, grupos.fecha_inicio,
                grupos.fecha_fin, grupos.cod_municipio, grupos.cod_estrategia, grupos.cupo_asignado,
                grupos.num_aprendices_matriculados, grupos.num_aprendices_activos,
                historico.id_historico, historico.id_grupo,
                historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            INNER JOIN grupos ON historico.id_grupo = grupos.ficha
            WHERE grupos.cod_centro = :cod_centro        
        """)
        
        result = db.execute(query, {"cod_centro": cod_centro}).mappings().first()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por codigo centro: {e}")
        raise Exception("Error de base de datos al obtener el historico por codigo de centro")

def get_historico_by_jornada(db: Session, jornada: str) -> Optional[dict]:
    """
    Obtiene el histórico asociado a una jornada en especifico.
    La jornada corresponde al jornada en la tabla grupos para consultar el historico.
    """
    try:
        query = text("""
            SELECT 
                grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
                grupos.jornada, grupos.etapa_ficha, grupos.estado_curso, grupos.fecha_inicio,
                grupos.fecha_fin, grupos.cod_municipio, grupos.cod_estrategia, grupos.cupo_asignado,
                grupos.num_aprendices_matriculados, grupos.num_aprendices_activos,
                historico.id_historico, historico.id_grupo,
                historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            INNER JOIN grupos ON historico.id_grupo = grupos.ficha
            WHERE grupos.cod_centro = :cod_centro        
        """)
        
        result = db.execute(query, {"jornada": jornada}).mappings().first()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por jornada: {e}")
        raise Exception("Error de base de datos al obtener el historico por jornada")

# Consulta por ESTADO_CURSO
# Consulta por FECHA_INICIO
# Consulta por FECHA_FIN
# Consulta por COD_MUNICIPIO