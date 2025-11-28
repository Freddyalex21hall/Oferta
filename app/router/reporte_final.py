from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from io import BytesIO
import pandas as pd

from core.database import get_db
from app.crud.reporte_final import get_unified_rows

router = APIRouter()


@router.get('/reporte/final', tags=["Reporte Final"], summary="Exportar reporte final a Excel")
def reporte_final(db=Depends(get_db)):
    """Genera un Excel con la unión de estado de normas, histórico, programas y registro calificado.

    Retorna un `StreamingResponse` con el archivo Excel en memoria.
    """
    try:
        df = get_unified_rows(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generando reporte: {str(e)}")

    output = BytesIO()
    # Escribir Excel en memoria
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Reporte_Final')
    output.seek(0)

    headers = {
        'Content-Disposition': 'attachment; filename="reporte_final.xlsx"'
    }

    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)
