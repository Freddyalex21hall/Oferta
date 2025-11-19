from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.schemas.centros import CentroBase
from core.database import get_db
from app.crud import centros as crud_centros


router = APIRouter()

@router.post("/registrar", status_code=status.HTTP_201_CREATED)
def create_user(centro: CentroBase, db: Session = Depends(get_db)):
    try:
        crear = crud_centros.create_centro(db, centro)
        if crear:
            return {"message": "Centro creado correctamente"}
        else:
            return {"message": "El Centro no pudo ser creado correctamente"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

