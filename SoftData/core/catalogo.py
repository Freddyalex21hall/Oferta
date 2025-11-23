"""
Utilidades y funciones auxiliares para el módulo de catálogo.
"""
import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def validate_catalogo_data(data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Valida los datos de un catálogo antes de procesarlos.
    
    Args:
        data: Diccionario con los datos del catálogo a validar
        
    Returns:
        tuple: (True, None) si es válido, (False, mensaje_error) si no es válido
    """
    try:
        # Validar nombre
        if 'nombre_catalogo' in data:
            nombre = data['nombre_catalogo']
            if not nombre or len(nombre.strip()) < 3:
                return False, "El nombre del catálogo debe tener al menos 3 caracteres"
            if len(nombre) > 100:
                return False, "El nombre del catálogo no puede exceder 100 caracteres"
        
        # Validar descripción
        if 'descripcion' in data and data['descripcion']:
            descripcion = data['descripcion']
            if len(descripcion) > 500:
                return False, "La descripción no puede exceder 500 caracteres"
        
        # Validar código
        if 'cod_catalogo' in data and data['cod_catalogo']:
            codigo = data['cod_catalogo']
            if len(codigo) > 50:
                return False, "El código del catálogo no puede exceder 50 caracteres"
        
        return True, None
    
    except Exception as e:
        logger.error(f"Error al validar datos del catálogo: {e}")
        return False, f"Error en la validación: {str(e)}"

def format_catalogo_response(catalogo: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formatea la respuesta de un catálogo para la API.
    
    Args:
        catalogo: Diccionario con los datos del catálogo
        
    Returns:
        Dict: Diccionario formateado con los datos del catálogo
    """
    try:
        formatted = {
            "id_catalogo": catalogo.get("id_catalogo"),
            "nombre_catalogo": catalogo.get("nombre_catalogo", "").strip(),
            "descripcion": catalogo.get("descripcion", "").strip() if catalogo.get("descripcion") else None,
            "cod_catalogo": catalogo.get("cod_catalogo", "").strip() if catalogo.get("cod_catalogo") else None,
            "estado": bool(catalogo.get("estado", True))
        }
        return formatted
    except Exception as e:
        logger.error(f"Error al formatear respuesta del catálogo: {e}")
        return catalogo

def generate_catalogo_code(nombre: str, existing_codes: list = None) -> str:
    """
    Genera un código único para un catálogo basado en su nombre.
    
    Args:
        nombre: Nombre del catálogo
        existing_codes: Lista de códigos existentes para evitar duplicados
        
    Returns:
        str: Código generado para el catálogo
    """
    try:
        # Convertir nombre a código (mayúsculas, sin espacios, sin acentos)
        import unicodedata
        import re
        
        # Normalizar y convertir a mayúsculas
        nombre_normalizado = unicodedata.normalize('NFD', nombre.upper())
        nombre_sin_acentos = ''.join(c for c in nombre_normalizado if unicodedata.category(c) != 'Mn')
        
        # Remover caracteres especiales y espacios
        codigo = re.sub(r'[^A-Z0-9]', '', nombre_sin_acentos)
        
        # Limitar a 50 caracteres
        codigo = codigo[:50]
        
        # Si el código está vacío, usar un prefijo
        if not codigo:
            codigo = "CAT"
        
        # Verificar duplicados y agregar número si es necesario
        if existing_codes:
            codigo_base = codigo
            contador = 1
            while codigo in existing_codes:
                codigo = f"{codigo_base}{contador}"
                contador += 1
                if len(codigo) > 50:
                    codigo = codigo[:47] + str(contador)
        
        return codigo
    
    except Exception as e:
        logger.error(f"Error al generar código de catálogo: {e}")
        # Retornar código por defecto basado en timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"CAT{timestamp[:10]}"

