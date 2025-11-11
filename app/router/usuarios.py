from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.router.dependencias import get_current_user
from app.schemas.usuarios import CrearUsuario, EditarPass, EditarUsuario, RetornoUsuario
from core.database import get_db
from app.crud import usuarios as crud_users
 

router = APIRouter()

@router.post("/registrar", status_code=status.HTTP_201_CREATED)
def create_user(
    user: CrearUsuario, 
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        if user_token.id_rol !=1 :
            raise HTTPException(status_code=401, detail="no autorizado para crear usuarios")
        crud_users.create_user(db, user)
        return {"message": "Usuario creado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/obtener-por-id/{id_usuario}", status_code=status.HTTP_200_OK, response_model=RetornoUsuario)
def get_by_id(
    id_usuario:int, 
    db:Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    
    try:
        if user_token.id_rol !=1 :
            raise HTTPException(status_code=401, detail="no autorizado para crear usuarios")
        result = crud_users.get_user_by_id(db, id_usuario)
    
        if result is None:
            HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")

        return result
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/obtener-por-correo/{correo}", status_code=status.HTTP_200_OK, response_model=RetornoUsuario)
def get_by_email(
    correo:str, 
    db:Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        if user_token.id_rol !=1 :
            raise HTTPException(status_code=401, detail="no autorizado para crear usuarios")
        result = crud_users.get_user_by_email(db, correo)
    
        if result is None:
            HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")

        return result
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.delete("/eliminar-por-id/{id}", status_code=status.HTTP_200_OK)
def delete_by_id(
    id_usuario:int, 
    db:Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        if user_token.id_rol !=1 :
            raise HTTPException(status_code=401, detail="no autorizado para crear usuarios")
        user = crud_users.user_delete(db, id_usuario)
    
        if user:
            return {"message": "Usuario eliminado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.put("/editar/{user_id}") # el tipo put es para editar
def update_user(
    user_id: int, 
    user: EditarUsuario, 
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        if user_token.id_rol !=1 :
            raise HTTPException(status_code=401, detail="no autorizado para crear usuarios")
        success = crud_users.update_user(db, user_id, user)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar el usuario")
        return {"message": "Usuario actualizado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.put("/editar-contrasenia/{user_id}") # el tipo put es para editar
def update_password(
    user: EditarPass, 
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        if user_token.id_rol !=1 :
            raise HTTPException(status_code=401, detail="no autorizado para crear usuarios")
        verificar= crud_users.verify_user_pass(db, user)
        if not verificar:
            raise HTTPException(status_code=400, detail="las contraseñas no coinciden")
        
        success = crud_users.update_user(db, user)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar la contraseña de el usuario")
        return {"message": "contraseña actualizada correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))