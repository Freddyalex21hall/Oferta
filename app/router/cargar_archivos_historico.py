from fastapi import APIRouter, UploadFile, File, Depends
import pandas as pd
from sqlalchemy.orm import Session
from io import BytesIO
from app.crud.cargar_archivos_historico import insertar_historico_en_bd
from core.database import get_db

router = APIRouter()

@router.post("/upload-excel-historico/")
async def upload_excel_historico(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Endpoint para cargar datos históricos de aprendices por grupo desde un archivo Excel.
    Espera un archivo con columnas de estados de aprendices por grupo.
    """
    
    # Leer archivo Excel
    contents = await file.read()
    df = pd.read_excel(
        BytesIO(contents),
        engine="openpyxl",
        skiprows=4,  # Ajustar según tu formato
        usecols=[
            "IDENTIFICADOR_FICHA",  # O el nombre que uses para identificar el grupo
            "INSCRITOS",
            "EN_TRANSITO",
            "FORMACION",
            "INDUCCION",
            "CONDICIONADOS",
            "APLAZADOS",
            "RETIRADO_VOLUNTARIO",
            "CANCELADOS",
            "REPROBADOS",
            "NO_APTOS",
            "REINGRESADOS",
            "POR_CERTIFICAR",
            "CERTIFICADOS",
            "TRASLADADOS"
        ],
        dtype=str
    )
    
    print("DataFrame original:")
    print(df.head())
    print(df.columns)
    print(df.dtypes)

    # Renombrar columnas al formato de la BD
    df = df.rename(columns={
        "IDENTIFICADOR_FICHA": "ficha",
        "INSCRITOS": "num_aprendices_inscritos",
        "EN_TRANSITO": "num_aprendices_en_transito",
        "FORMACION": "num_aprendices_formacion",
        "INDUCCION": "num_aprendices_induccion",
        "CONDICIONADOS": "num_aprendices_condicionados",
        "APLAZADOS": "num_aprendices_aplazados",
        "RETIRADO_VOLUNTARIO": "num_aprendices_retirado_voluntario",
        "CANCELADOS": "num_aprendices_cancelados",
        "REPROBADOS": "num_aprendices_reprobados",
        "NO_APTOS": "num_aprendices_no_aptos",
        "REINGRESADOS": "num_aprendices_reingresados",
        "POR_CERTIFICAR": "num_aprendices_por_certificar",
        "CERTIFICADOS": "num_aprendices_certificados",
        "TRASLADADOS": "num_aprendices_trasladados"
    })

    print("\nDataFrame renombrado:")
    print(df.head())
    print(df.columns)

    # Eliminar filas con ficha faltante (campo obligatorio)
    df = df.dropna(subset=["ficha"])

    # Convertir ficha a numérico
    df["ficha"] = pd.to_numeric(df["ficha"], errors="coerce").astype("Int64")

    # Convertir todas las columnas numéricas de aprendices
    columnas_numericas = [
        "num_aprendices_inscritos",
        "num_aprendices_en_transito",
        "num_aprendices_formacion",
        "num_aprendices_induccion",
        "num_aprendices_condicionados",
        "num_aprendices_aplazados",
        "num_aprendices_retirado_voluntario",
        "num_aprendices_cancelados",
        "num_aprendices_reprobados",
        "num_aprendices_no_aptos",
        "num_aprendices_reingresados",
        "num_aprendices_por_certificar",
        "num_aprendices_certificados",
        "num_aprendices_trasladados"
    ]

    for col in columnas_numericas:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int16")

    print("\nDataFrame procesado:")
    print(df.head())
    print(df.dtypes)

    # Necesitamos obtener el id_grupo desde la ficha
    # Asumiendo que tienes una consulta para obtener id_grupo desde la ficha
    from sqlalchemy import text
    
    # Obtener mapeo de ficha -> id_grupo
    fichas = df["ficha"].unique().tolist()
    
    query = text("""
        SELECT ficha, id_grupo 
        FROM grupos 
        WHERE ficha IN :fichas
    """)
    
    result = db.execute(query, {"fichas": tuple(fichas)})
    ficha_to_id = {row.ficha: row.id_grupo for row in result}
    
    # Agregar id_grupo al DataFrame
    df["id_grupo"] = df["ficha"].map(ficha_to_id)
    
    # Filtrar registros sin id_grupo válido
    df_validos = df[df["id_grupo"].notna()].copy()
    registros_sin_grupo = len(df) - len(df_validos)
    
    if registros_sin_grupo > 0:
        print(f"⚠️ Advertencia: {registros_sin_grupo} registros no tienen grupo asociado")

    # Seleccionar solo las columnas necesarias para la BD
    df_historico = df_validos[[
        "id_grupo",
        "num_aprendices_inscritos",
        "num_aprendices_en_transito",
        "num_aprendices_formacion",
        "num_aprendices_induccion",
        "num_aprendices_condicionados",
        "num_aprendices_aplazados",
        "num_aprendices_retirado_voluntario",
        "num_aprendices_cancelados",
        "num_aprendices_reprobados",
        "num_aprendices_no_aptos",
        "num_aprendices_reingresados",
        "num_aprendices_por_certificar",
        "num_aprendices_certificados",
        "num_aprendices_trasladados"
    ]]

    print("\nDataFrame final para BD:")
    print(df_historico.head())

    # Insertar en la base de datos
    resultados = insertar_historico_en_bd(db, df_historico)
    
    # Agregar información adicional sobre registros no procesados
    resultados["registros_sin_grupo"] = registros_sin_grupo
    
    return resultados