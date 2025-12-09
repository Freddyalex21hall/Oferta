from typing import List, Dict, Any
from sqlalchemy import text
import pandas as pd


def _rows_to_dicts(result_proxy) -> List[Dict[str, Any]]:
    try:
        return [dict(r) for r in result_proxy.mappings().all()]
    except Exception:
        cols = result_proxy.keys()
        return [dict(zip(cols, row)) for row in result_proxy.fetchall()]


def get_unified_rows(db) -> pd.DataFrame:
    """Construye un DataFrame con las columnas solicitadas a partir de tablas existentes.

    Tablas usadas: `programas_formacion`, `registro_calificado`, `estado_de_normas`,
    `grupos`, `historico`, `centros_formacion`.
    """
    # Consulta base de programas con datos de registro y normas
    sql_programas = text("""
        SELECT p.cod_programa, p.cod_version, p.PRF_version, p.tipo_formacion, p.nombre_programa,
            p.nivel_formacion, p.duracion_maxima, p.resolucion, p.fecha_resolucion as prf_fecha_resolucion,
            p.modalidad, p.apuestas_prioritarias, p.red_conocimiento,
            r.tipo_tramite AS registro_tipo_tramite,
            r.numero_resolucion AS registro_num_resolucion,
            r.fecha_resolucion AS registro_fecha_resolucion,
            e.nombre_ncl AS norma_nombre_ncl, e.version AS norma_version
        FROM programas_formacion p
        LEFT JOIN registro_calificado r ON p.cod_programa = r.cod_programa
        LEFT JOIN estado_de_normas e ON p.cod_programa = e.cod_programa
    """)

    programas = db.execute(sql_programas)
    programas_rows = _rows_to_dicts(programas)

    # Agregaciones por programa desde grupos
    sql_grupos = text("""
        SELECT cod_programa,
            COUNT(*) AS grupos_count,
            SUM(COALESCE(cupo_asignado,0)) AS cupos_sum,
            MIN(fecha_inicio) AS primera_fecha_inicio,
            MAX(fecha_fin) AS ultima_fecha_fin,
            GROUP_CONCAT(DISTINCT jornada) AS jornadas,
            GROUP_CONCAT(DISTINCT cod_municipio) AS municipios,
            GROUP_CONCAT(DISTINCT cod_centro) AS cod_centros
        FROM grupos
        GROUP BY cod_programa
    """)
    grupos = db.execute(sql_grupos)
    grupos_map = {r['cod_programa']: r for r in _rows_to_dicts(grupos)}

    # Agregación historico por programa (uniendo por grupo -> programa)
    sql_historico = text("""
        SELECT g.cod_programa,
            SUM(COALESCE(h.num_aprendices_inscritos,0)) AS inscritos_sum,
            SUM(COALESCE(h.num_aprendices_en_transito,0)) AS inscritos_segunda,
            SUM(COALESCE(h.num_aprendices_certificados,0)) AS certificados_sum
        FROM historico h
        JOIN grupos g ON g.ficha = h.id_grupo
        GROUP BY g.cod_programa
    """)
    historico = db.execute(sql_historico)
    historico_map = {r['cod_programa']: r for r in _rows_to_dicts(historico)}

    # Mapa rápido de centros_formacion por codigo (para nombre y regional)
    sql_centros = text("SELECT cod_centro, nombre_centro, nombre_regional FROM centros_formacion")
    centros = db.execute(sql_centros)
    centros_map = {str(r['cod_centro']): r for r in _rows_to_dicts(centros)}

    rows = []
    for p in programas_rows:
        cod = p.get('cod_programa')
        g = grupos_map.get(cod, {})
        h = historico_map.get(cod, {})

        # Tomar primer centro si existe (lista en cod_centros separada por commas)
        centro_codigo = None
        if g and g.get('cod_centros'):
            centro_codigo = str(g['cod_centros']).split(',')[0]

        centro_info = centros_map.get(centro_codigo) if centro_codigo else None

        fila = {
            'OFERTA': p.get('tipo_formacion') or '',
            'CÓDIGO CENTRO': centro_codigo or '',
            'CENTRO DE FORMACIÓN': centro_info.get('nombre_centro') if centro_info else '',
            'DENOMINACIÓN': p.get('nombre_programa') or '',
            'TIPO OFERTA': p.get('tipo_formacion') or '',
            'NIVEL': p.get('nivel_formacion') or '',
            '1. DENOMINACIÓN DE LA FORMACIÓN': p.get('nombre_programa') or '',
            '2. MODALIDAD': p.get('modalidad') or '',
            '3. CÓDIGO PROGRAMA': p.get('cod_programa') or '',
            '4. VERSIÓN DEL PROGRAMA': p.get('cod_version') or p.get('PRF_version') or '',
            'CÓDIGO-VERSIÓN': f"{p.get('cod_programa','')}-{p.get('cod_version','')}",
            'NOMBRE DENOMINACIÓN DEL PROGRAMA EN EL CATALOGO': p.get('nombre_programa') or '',
            'VALIDACIÓN': p.get('registro_tipo_tramite') or '',
            'RESOLUCIÓN': p.get('resolucion') or p.get('registro_num_resolucion') or '',
            'FECHA DE RESOLUCIÓN': p.get('prf_fecha_resolucion') or p.get('registro_fecha_resolucion') or None,
            'CODIGO SNIES': '',
            '5. NO. RESOLUCIÓN, FECHA Y CÓDIGO SNIES': '',
            'ACTA COMITÉ PRIMARIO CENTRO DE FORMACION': '',
            '6. JUSTIFICACIÓN DE LA OFERTA EDUCATIVA': '',
            '7. GRUPOS': int(g.get('grupos_count') or 0),
            '8. CUPOS': int(g.get('cupos_sum') or 0),
            'DURACIÓN DEL PROGRAMA HORAS': p.get('duracion_maxima') or '',
            'DURACIÓN EN CATALOGO  22/04/2024': p.get('duracion_maxima') or '',
            'VALIDACIÓN COMPARACIÓN CON CATALOGO  22/04/2024': '',
            '9. DURACIÓN DEL PROGRAMA (MESES)': '',
            '10. MUNICIPIO': g.get('municipios') if g else '',
            '11. SEDE': centro_info.get('nombre_centro') if centro_info else '',
            'CÓDIGO INDICATIVA': '',
            'HORARIO FORMACIÓN': g.get('jornadas') if g else '',
            'Jornada': g.get('jornadas') if g else '',
            'Apuesta prioritaria': p.get('apuestas_prioritarias') or '',
            'ESTRATEGIA': '',
            'FECHA INICIO': g.get('primera_fecha_inicio') if g else None,
            'FECHA FINALIZACIÓN': g.get('ultima_fecha_fin') if g else None,
            'ESTADO EN ACTA': '',
            'ESTADO EN SOFIA PLUS': '',
            'CONCEPTO GRUPO': '',
            'COORDINACIÓN DE FPI (REGIONAL)': centro_info.get('nombre_regional') if centro_info else '',
            'INSCRITOS PRIMERA OPCIÓN': int(h.get('inscritos_sum') or 0),
            'INSCRITOS SEGUNDA OPCIÓN': int(h.get('inscritos_segunda') or 0),
            'CERTIFICADOS': int(h.get('certificados_sum') or 0),
            # porcentaje como float 0-100 (más útil para cálculos), se puede formatear luego
            'PORCENTAJE_CERTIFICADOS': (
                (int(h.get('certificados_sum') or 0) / int(h.get('inscritos_sum') or 1)) * 100
                if (int(h.get('inscritos_sum') or 0) > 0) else 0
            ),
            'RED DE CONOCIMIENTO': p.get('red_conocimiento') or ''
        }

        rows.append(fila)

    # Crear filas adicionales de alerta cuando certificados > 30% de inscritos
    alert_rows = []
    for r in rows:
        try:
            pct = float(r.get('PORCENTAJE_CERTIFICADOS') or 0)
        except Exception:
            pct = 0
        if pct > 30:
            alert = {
                'OFERTA': 'ALERTA: CERTIFICADOS > 30%',
                '3. CÓDIGO PROGRAMA': r.get('3. CÓDIGO PROGRAMA'),
                'DENOMINACIÓN': r.get('DENOMINACIÓN'),
                'INSCRITOS PRIMERA OPCIÓN': r.get('INSCRITOS PRIMERA OPCIÓN'),
                'CERTIFICADOS': r.get('CERTIFICADOS'),
                'PORCENTAJE_CERTIFICADOS': r.get('PORCENTAJE_CERTIFICADOS'),
                'CONCEPTO GRUPO': 'Alerta generada automáticamente: más del 30% certificados'
            }
            alert_rows.append(alert)

    # Añadir alertas al final
    rows.extend(alert_rows)

    df = pd.DataFrame(rows)

    desired_order = [
        'OFERTA','CÓDIGO CENTRO','CENTRO DE FORMACIÓN','DENOMINACIÓN','TIPO OFERTA','NIVEL',
        '1. DENOMINACIÓN DE LA FORMACIÓN','2. MODALIDAD','3. CÓDIGO PROGRAMA','4. VERSIÓN DEL PROGRAMA',
        'CÓDIGO-VERSIÓN','NOMBRE DENOMINACIÓN DEL PROGRAMA EN EL CATALOGO','VALIDACIÓN','RESOLUCIÓN',
        'FECHA DE RESOLUCIÓN','CODIGO SNIES','5. NO. RESOLUCIÓN, FECHA Y CÓDIGO SNIES',
        'ACTA COMITÉ PRIMARIO CENTRO DE FORMACION','6. JUSTIFICACIÓN DE LA OFERTA EDUCATIVA','7. GRUPOS',
        '8. CUPOS','DURACIÓN DEL PROGRAMA HORAS','DURACIÓN EN CATALOGO  22/04/2024',
        'VALIDACIÓN COMPARACIÓN CON CATALOGO  22/04/2024','9. DURACIÓN DEL PROGRAMA (MESES)','10. MUNICIPIO',
        '11. SEDE','CÓDIGO INDICATIVA','HORARIO FORMACIÓN','Jornada','Apuesta prioritaria','ESTRATEGIA',
        'FECHA INICIO','FECHA FINALIZACIÓN','ESTADO EN ACTA','ESTADO EN SOFIA PLUS','CONCEPTO GRUPO',
        'COORDINACIÓN DE FPI (REGIONAL)','INSCRITOS PRIMERA OPCIÓN','INSCRITOS SEGUNDA OPCIÓN','RED DE CONOCIMIENTO'
    ]

    # Añadimos las nuevas columnas a desired_order si no existen
    if 'CERTIFICADOS' not in desired_order:
        desired_order.append('CERTIFICADOS')
    if 'PORCENTAJE_CERTIFICADOS' not in desired_order:
        desired_order.append('PORCENTAJE_CERTIFICADOS')

    existing_cols = [c for c in desired_order if c in df.columns]
    # Si no hay columnas existentes (p. ej. tablas vacías), crear un DataFrame
    # con las columnas esperadas y una fila informativa para que el Excel todavía
    # se genere y pueda descargarse desde la API.
    if not existing_cols:
        # Crear DataFrame con las columnas deseadas y una fila explicativa
        df = pd.DataFrame(columns=desired_order)
        info_row = {c: '' for c in desired_order}
        # Mensaje claro en una columna relevante
        if 'DENOMINACIÓN' in desired_order:
            info_row['DENOMINACIÓN'] = 'No hay registros en programas_formacion'
        else:
            # si por alguna razón no existe esa columna, usar la primera
            info_row[desired_order[0]] = 'No hay registros en programas_formacion'
        df = pd.DataFrame([info_row])
    else:
        df = df[existing_cols]

    return df
