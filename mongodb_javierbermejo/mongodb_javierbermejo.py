# Aplicación báscia para la activdad de MongoDB
from pymongo import MongoClient
from decouple import config
from urllib.parse import quote_plus
import sys
import os
import datetime
import json


class Actividad:
    def __init__(self):
        # Leemos los datos del fichero .env
        DB_HOST = config('DB_HOST')
        DB_DATABASE = config('DB_DATABASE')
        DB_USERNAME = config('DB_USERNAME')
        DB_PASSWORD = config('DB_PASSWORD')

        # Intentamos la conexión y usamos
        # quote_plus por si hay caracteres especiales en la clave
        # También fijamos el timeout para no esperar 20 seg en caso de error
        try:
            uri = "mongodb://%s:%s@%s:27017" % (quote_plus(
                DB_USERNAME), quote_plus(DB_PASSWORD), DB_HOST)
            self.conn = MongoClient(uri, serverSelectionTimeoutMS=3000)
            self.mydb = self.conn[DB_DATABASE]
            self.mycol = self.mydb["notas"]
        except:
            print('Error al conectar con la DB')
            sys.exit(1)

    # Preparamos las fechas en formato
    def preparar_fechas(self,datos):
        for dato in datos:
            dato['fecha'] = datetime.datetime.strptime(
                dato['fecha'], "%d-%m-%Y %H: %M: %S")
        return datos

    # Insertar los datos requeridos
    def insertar(self, datos):
        self.mycol.insert_many(datos)

    # Actualizar los datos requeridos
    def actualizar_nota(self, nombre,nota):
        self.mycol.update_one(nombre,nota)

    # Lectura de datos
    def lectura(self,consulta={}):
        registros = self.mycol.find(consulta)

        print('{:<25}\t{:<20}\t{}\t{:<20}\t{}\t{}\t'.format(
                'ID', 'NOMBRE', 'EDAD', 'EMAIL', 'NOTA', 'FECHA'))
        for registro in registros:
            print('{}\t{:<20}\t{}\t{:<20}\t{}\t{}\t'.format(
                registro['_id'], registro['nombre'], registro['edad'], registro['email'], registro['nota'],registro['fecha']))

    # Eliminar los datos requeridos
    def eliminar(self, consulta):
        self.mycol.delete_one(consulta)


if __name__ == '__main__':


    db = Actividad()
    input("Presiona enter para iniciar las tareas...")

    datos = [{"nombre": "Pedro López", "edad": 25, "email": "pedro@eip.com", "nota": 5.2, "fecha": "12-07-2021 12: 45: 26"},
             {"nombre": "Julia García", "edad": 22, "email": "julia@eip.com",
                 "nota": 7.3, "fecha": "12-07-2021 12: 45: 26"},
             {"nombre": "Amparo Mayoral", "edad": 28, "email": "amparo@eip.com",
                 "nota": 8.4, "fecha": "12-07-2021 12: 45: 26"},
             {"nombre": "Juan Martínez", "edad": 30, "email": "juan@eip.com", "nota": 6.8, "fecha": "12-07-2021 12: 45: 26"}]

    # datos_preparados = db.preparar_fechas(datos)

    # db.insertar(datos_preparados)

    # print('Tarea 1 insertar datos realizada correctamente ....')
    # input('\nPulsa enter para iniciar la segunda Tarea ...')


    # # Actualizar nota de Amparo
    # nombre =  {"nombre": "Amparo Mayoral"}
    # nota = { "$set": { "nota": 9.3 } }

    # db.actualizar_nota(nombre,nota)

    # # Actualizar nota de Juan
    # nombre =  {"nombre": "Juan Martínez"}
    # nota = { "$set": { "nota": 7.2 } }

    # db.actualizar_nota(nombre,nota)

    print('Tarea 2 actualizar datos realizada correctamente ....')
    input('\nPulsa enter para iniciar la tercera Tarea ...')

    db.lectura()

    print('Tarea 3 listado de datos realizada correctamente ....')
    input('\nPulsa enter para iniciar la cuarta Tarea ...')

    consulta = {'nota': { '$gt': 7, '$lt': 7.5}}
    db.lectura(consulta)

    print('Tarea 4 filtrar notas de 7 a 7.5 realizada correctamente ....')
    input('\nPulsa enter para iniciar la quinta Tarea ...')

    consulta = {'nombre': 'Pedro López'}
    db.eliminar(consulta)

    print('Tarea 5 eliminar a Pedro López realizada correctamente ....')
    print('Los datos han quedado así')

    db.lectura()
    print('Pedro López eliminado ... Hasta la vista baby!! ....')


