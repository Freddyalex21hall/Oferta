from datetime import date
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
                grupos.codigo_regional ,grupos.ficha, grupos.cod_programa, grupos.cod_centro, grupos.modalidad,
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE grupos.cod_centro = :cod_centro        
        """)
        
        result = db.execute(query, {"cod_centro": cod_centro}).mappings().all()
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE grupos.jornada = :jornada           
        """)
        
        result = db.execute(query, {"jornada": jornada}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por jornada: {e}")
        raise Exception("Error de base de datos al obtener el historico por jornada")


# Consulta por ESTADO_CURSO
def get_historico_by_estado_curso(db: Session, estado_curso: str) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un estado de curso en especifico.
    El estado de curso corresponde al estado_curso en la tabla grupos para consultar el historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE grupos.estado_curso = :estado_curso        
        """)
        
        result = db.execute(query, {"estado_curso": estado_curso}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por estado curso: {e}")
        raise Exception("Error de base de datos al obtener el historico por estado curso")

# Consulta por FECHA_INICIO
def get_historico_by_fecha_inicio(db: Session, fecha_inicio: date) -> Optional[dict]:
    """
    Obtiene el histórico asociado a una fecha_inicio en especifico.
    La fecha_inicio corresponde a la fecha_inicio en la tabla grupos para consultar el historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE grupos.fecha_inicio = :fecha_inicio        
        """)
        
        result = db.execute(query, {"fecha_inicio": fecha_inicio}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por fecha_inicio: {e}")
        raise Exception("Error de base de datos al obtener el historico por fecha_inicio")

def get_historico_by_fecha_fin(db: Session, fecha_fin: date) -> Optional[dict]:
    """
    Obtiene el histórico asociado a una fecha_fin en especifico.
    La fecha_fin corresponde a la fecha_fin en la tabla grupos para consultar el historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE grupos.fecha_fin = :fecha_fin        
        """)
        
        result = db.execute(query, {"fecha_fin": fecha_fin}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por fecha_fin: {e}")
        raise Exception("Error de base de datos al obtener el historico por fecha_fin")


def get_historico_by_cod_municipio(db: Session, cod_municipio: str) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un cod_municipio en especifico.
    El cod_municipio corresponde al cod_municipio en la tabla grupos para consultar el historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE grupos.cod_municipio = :cod_municipio        
        """)
        
        result = db.execute(query, {"cod_municipio": cod_municipio}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por cod_municipio: {e}")
        raise Exception("Error de base de datos al obtener el historico por cod_municipio")


def get_historico_by_num_aprendices_inscritos(db: Session, num_aprendices_inscritos: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_inscritos en especifico.
    El num_aprendices_inscritos corresponde al num_aprendices_inscritos en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_inscritos = :num_aprendices_inscritos        
        """)
        
        result = db.execute(query, {"num_aprendices_inscritos": num_aprendices_inscritos}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_inscritos: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_inscritos")


def get_historico_by_num_aprendices_en_transito(db: Session, num_aprendices_en_transito: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_en_transito en especifico.
    El num_aprendices_en_transito corresponde al num_aprendices_en_transito en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_en_transito = :num_aprendices_en_transito        
        """)
        
        result = db.execute(query, {"num_aprendices_en_transito": num_aprendices_en_transito}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_en_transito: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_en_transito")


def get_historico_by_num_aprendices_formacion(db: Session, num_aprendices_formacion: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_formacion en especifico.
    El num_aprendices_formacion corresponde al num_aprendices_formacion en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_formacion = :num_aprendices_formacion        
        """)
        
        result = db.execute(query, {"num_aprendices_formacion": num_aprendices_formacion}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_formacion: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_formacion")


def get_historico_by_num_aprendices_induccion(db: Session, num_aprendices_induccion: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_induccion en especifico.
    El num_aprendices_induccion corresponde al num_aprendices_induccion en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_induccion = :num_aprendices_induccion        
        """)
        
        result = db.execute(query, {"num_aprendices_induccion": num_aprendices_induccion}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_induccion: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_induccion")


def get_historico_by_num_aprendices_condicionados(db: Session, num_aprendices_condicionados: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_condicionados en especifico.
    El num_aprendices_condicionados corresponde al num_aprendices_condicionados en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_condicionados = :num_aprendices_condicionados        
        """)
        
        result = db.execute(query, {"num_aprendices_condicionados": num_aprendices_condicionados}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_condicionados: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_condicionados")


def get_historico_by_num_aprendices_aplazados(db: Session, num_aprendices_aplazados: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_aplazados en especifico.
    El num_aprendices_aplazados corresponde al num_aprendices_aplazados en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_aplazados = :num_aprendices_aplazados        
        """)
        
        result = db.execute(query, {"num_aprendices_aplazados": num_aprendices_aplazados}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_aplazados: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_aplazados")


def get_historico_by_num_aprendices_retirado_voluntario(db: Session, num_aprendices_retirado_voluntario: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_retirado_voluntario en especifico.
    El num_aprendices_retirado_voluntario corresponde al num_aprendices_retirado_voluntario en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_retirado_voluntario = :num_aprendices_retirado_voluntario        
        """)
        
        result = db.execute(query, {"num_aprendices_retirado_voluntario": num_aprendices_retirado_voluntario}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_retirado_voluntario: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_retirado_voluntario")


def get_historico_by_num_aprendices_cancelados(db: Session, num_aprendices_cancelados: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_cancelados en especifico.
    El num_aprendices_cancelados corresponde al num_aprendices_cancelados en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_cancelados = :num_aprendices_cancelados        
        """)
        
        result = db.execute(query, {"num_aprendices_cancelados": num_aprendices_cancelados}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_cancelados: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_cancelados")


def get_historico_by_num_aprendices_reprobados(db: Session, num_aprendices_reprobados: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_reprobados en especifico.
    El num_aprendices_reprobados corresponde al num_aprendices_reprobados en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_reprobados = :num_aprendices_reprobados        
        """)
        
        result = db.execute(query, {"num_aprendices_reprobados": num_aprendices_reprobados}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_reprobados: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_reprobados")


def get_historico_by_num_aprendices_no_aptos(db: Session, num_aprendices_no_aptos: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_no_aptos en especifico.
    El num_aprendices_no_aptos corresponde al num_aprendices_no_aptos en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_no_aptos = :num_aprendices_no_aptos        
        """)
        
        result = db.execute(query, {"num_aprendices_no_aptos": num_aprendices_no_aptos}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_no_aptos: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_no_aptos")

def get_historico_by_num_aprendices_reingresados(db: Session, num_aprendices_reingresados: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_reingresados en especifico.
    El num_aprendices_reingresados corresponde al num_aprendices_reingresados en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_reingresados = :num_aprendices_reingresados       
        """)
        
        result = db.execute(query, {"num_aprendices_reingresados": num_aprendices_reingresados}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_reingresados: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_reingresados")


def get_historico_by_num_aprendices_por_certificar(db: Session, num_aprendices_por_certificar: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_por_certificar en especifico.
    El num_aprendices_por_certificar corresponde al num_aprendices_por_certificar en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_por_certificar = :num_aprendices_por_certificar        
        """)
        
        result = db.execute(query, {"num_aprendices_por_certificar": num_aprendices_por_certificar}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_por_certificar: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_por_certificar")


def get_historico_by_num_aprendices_certificados(db: Session, num_aprendices_certificados: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_certificados en especifico.
    El num_aprendices_certificados corresponde al num_aprendices_certificados en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_certificados = :num_aprendices_certificados        
        """)
        
        result = db.execute(query, {"num_aprendices_certificados": num_aprendices_certificados}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_certificados: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_certificados")


def get_historico_by_num_aprendices_trasladados(db: Session, num_aprendices_trasladados: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a un num_aprendices_trasladados en especifico.
    El num_aprendices_trasladados corresponde al num_aprendices_trasladados en la tabla historico.
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
            FROM grupos
            INNER JOIN historico ON historico.id_grupo = grupos.ficha
            WHERE historico.num_aprendices_trasladados = :num_aprendices_trasladados        
        """)
        
        result = db.execute(query, {"num_aprendices_trasladados": num_aprendices_trasladados}).mappings().all()
        return result
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por num_aprendices_trasladados: {e}")
        raise Exception("Error de base de datos al obtener el historico por num_aprendices_trasladados")

# Consulta por FECHA_FIN
# Consulta por COD_MUNICIPIO
# Consulta por num_aprendices_inscritos
# Consulta por num_aprendices_en_transito
# Consulta por num_aprendices_formacion
# Consulta por num_aprendices_induccion
# Consulta por num_aprendices_condicionados
# Consulta por num_aprendices_aplazados
# Consulta por num_aprendices_retirado_voluntario
# Consulta por num_aprendices_cancelados
# Consulta por num_aprendices_reprobados
# Consulta por num_aprendices_no_aptos
# Consulta por num_aprendices_reingresados
# Consulta por num_aprendices_por_certificar
# Consulta por num_aprendices_certificados
# Consulta por num_aprendices_trasladados