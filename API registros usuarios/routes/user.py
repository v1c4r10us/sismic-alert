from fastapi import APIRouter
from config.db import collection
from schemas.user import userEntity,usersEntity
from models.user import User

user = APIRouter()

@user.get('/users')
def find_all_users():
    return usersEntity(collection.find())

@user.post('/users')
def create_user(user: User):
    new_user = dict(user)

    collection.insert_one(new_user).inserted_id

    return {"message": "Datos guardados exitosamente"}


@user.get('/users/{id}')
def find_user():
    return "hello world"

@user.put('/users/{id}')
def update_user():
    return "hello world"

@user.delete('/users/{id}')
def delete_user():
    return "hello world"