import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from random import *
from faker import *
from datetime import date
fake = Faker()
from sqlalchemy import create_engine
import os
class Integracion_multifuente:
    def __init__(self):
        self.conexion = None

        if self.validar_lectura():
            print("""Conexion a SQL exitosa
✅ Datos cargados. Iniciando menú....""")
            self.pantalla()
        else:
            print("❌ La aplicación no pudo iniciar.")


    def validar_lectura(self):
        load_dotenv()
        productos = os.getenv('productos')
        ventas = os.getenv('ventas')
        url = os.getenv('DATABASE_URL')
        if not Path(productos).exists():
            print(f"El enlace del archivo {productos} no existe o esta mal escrita la ruta.")
            return False
        if not Path(ventas).exists():
            print(f"El enlace del archivo {ventas} no existe o esta mal escrita la ruta.")
            return False
        try:
            self.inventario = pd.read_excel(productos,engine='openpyxl')
            self.metas_ventas = pd.read_csv(ventas)
            self.engine = create_engine(url)
            if self.conexion is None:
                with self.engine.connect():
                    pass
            return True
        except FileNotFoundError:
            print(f"❌ Error de Seguridad: Verifica el .env.")
            return False
        except Exception as e:
            print(f"⚠️ Error inesperado: {e}")
            return False
    def pantalla(self):
        print("1. Inventario (Excel)")
        print("2. Metas (CSV)")
        print("3. Mandar: Inventario vs Ventas (SQL + Excel)")
        print("4. Salir")

        while True:
            try:
                opcion= int(input("Ingresa una opcion valida"))
                if opcion in range(1, 5):
                    return opcion
            except ValueError:
                print("Opcion incorrecta")


    def lectura_excel(self):
        return self.inventario
    def lectura_csv(self):
        return self.metas_ventas
    def tabla_ventas(self):
        id_producto = self.inventario['ID_Producto'].unique().tolist()
        id = []
        cantidad = []
        fecha = []
        for n in range(10000):
            random_id = choice(id_producto)
            Cantidad = randint(1, 5)
            id.append(random_id)
            cantidad.append(Cantidad)
            fechas = fake.date_between(start_date=date(2025, 1, 1), end_date='today')
            fecha.append(fechas)
        fecha =pd.to_datetime(fecha)
        self.mi_tabla = pd.DataFrame({'Producto_ventas': id,'Cantidad': cantidad,'Fecha': fecha})
        self.mi_tabla = self.mi_tabla.sort_values(by = 'Fecha')
        self.mi_tabla.to_sql('ventas', con=self.engine, if_exists='replace', index=False)

        return "Transfiriendo ventas..."

    def tabla_excel(self):


        inventario_limpio =self.inventario.map(lambda x:x.strip().upper() if isinstance(x, str) else x)
        contador_errores = inventario_limpio.isnull().sum()
        contador_errores.name = 'Valores NaN'
        limpiar=inventario_limpio.dropna().copy()
        limpiar['Precio_Unitario'].str.strip()
        limpiar['Precio_Unitario'] = pd.to_numeric(limpiar['Precio_Unitario'], errors='coerce')
        limpiar =limpiar.loc[limpiar['Stock_Actual']>=0]
        self.datos_productos = limpiar

        self.datos_productos.to_sql('productos', con=self.engine, if_exists='replace', index=False)
        return "Transfiriendo productos..."

    def cruzar_ventas_productos(self):
        query = """
                SELECT v.Producto_ventas, \
                       v.Cantidad, \
                       v.Fecha,
                       p.Nombre, \
                       p.Categoria, \
                       p.Precio_Unitario,
                       (v.Cantidad * p.Precio_Unitario) AS Total_Venta
                FROM ventas v
                         JOIN productos p ON v.Producto_ventas = p.ID_Producto \
                """
        resultado = pd.read_sql(query, con=self.engine)
        resultado.to_sql('ventas_detalle', con=self.engine,
                         if_exists='replace', index=False)
        return resultado

def menu():

    prueba = Integracion_multifuente()
    while True:
        opcion = prueba.pantalla()
        if opcion == 1:
            print(prueba.lectura_excel())

        elif opcion == 2:
            print(prueba.lectura_csv())
        elif opcion == 3:
            print(prueba.tabla_excel())
            print(prueba.tabla_ventas())
            prueba.cruzar_ventas_productos()
        else:
            print("Saliendo...")
            break

menu()

