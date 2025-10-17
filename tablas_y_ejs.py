import pandas as pd
import numpy as np
import duckdb as dd
#%%
# Cargar los 4 archivos directamente
carpeta = 'C:/Users/Reni/Desktop/Labo/tp1'
df_actividades = pd.read_csv(carpeta + '/actividades_establecimientos.csv')

df_educativos = pd.read_excel(carpeta + '/2022_padron_oficial_establecimientos_educativos.xlsx', skiprows=6)

print(df_educativos)
# renombramos las columnas porque hay nombres repetidos y se dificulta referenciarlas bien para armar las tablas correctamente
df_educativos.columns = [f"col{i}" for i in range(len(df_educativos.columns))]

df_productivos = pd.read_csv(carpeta + '/Datos_por_departamento_actividad_y_sexo.csv')
#%%
df_padron_poblacion = pd.read_excel(carpeta + '/padron_poblacion.xlsX',skiprows=12)

#%%
# tabla establecimientos educativos
query_establecimientos_educativos = """
SELECT col1 AS cueanexo, col20 AS jardin_maternal, col21 AS jardin_infantil, 
       col22 AS primario, col23 AS secundario, col24 AS secundario_tecnico, 
       col25 AS terciario, col26 AS terciario_tecnico
FROM df_educativos
"""
df_establecimientos_educativos = dd.query(query_establecimientos_educativos).df()
#%%
# tabla departamento
query_departamento = """
SELECT DISTINCT in_departamentos AS id_depto, departamento AS nombre_depto
FROM df_productivos
"""
df_departamento = dd.query(query_departamento).df()
print(df_departamento)
#%%
# tabla provincia
# corregimos nombres y formatos asi luego no tenemos problemas en los JOIN.
query_provincia = """
SELECT DISTINCT
    provincia_id AS id_provincia,
    CASE
        WHEN LOWER(TRIM(provincia)) IN ('caba', 'ciudad autonoma de buenos aires', 'ciudad autónoma de buenos aires') THEN 'Ciudad Autónoma de Buenos Aires'
        WHEN LOWER(TRIM(provincia)) = 'cordoba' THEN 'Córdoba'
        WHEN LOWER(TRIM(provincia)) = 'entre rios' THEN 'Entre Ríos'
        WHEN LOWER(TRIM(provincia)) = 'tucuman' THEN 'Tucumán'
        WHEN LOWER(TRIM(provincia)) = 'rio negro' THEN 'Río Negro'
        WHEN LOWER(TRIM(provincia)) = 'neuquen' THEN 'Neuquén'
        ELSE provincia
    END AS nombre_provincia
FROM df_productivos
"""
df_provincia = dd.query(query_provincia).df()
print(df_provincia)
#%%
# tabla actividad
query_actividad = """
        SELECT clae6,clae6_desc
        FROM df_actividades
"""
df_actividad = dd.query(query_actividad)
#%%
# tabla establecimientos productivos
query_establecimientos_productivos = """
    SELECT provincia_id AS id_provincia, in_departamentos AS id_depto, clae6, genero, Empleo, Establecimientos, empresas_exportadoras
    FROM df_productivos
    WHERE anio == 2022
"""
df_establecimientos_productivos = dd.query(query_establecimientos_productivos).df()

#%%
# reformateo de padron_poblacion ya que esta muy feo. 

# 1️⃣ Creamos una columna auxiliar para identificar las filas con "AREA # ..."
df_padron_poblacion['id_depto'] = df_padron_poblacion['Unnamed: 1'].where(df_padron_poblacion['Unnamed: 1'].astype(str).str.startswith('AREA'))

# 2️⃣ Propagamos el área hacia abajo para rellenar las filas de la subtabla
df_padron_poblacion['id_depto'] = df_padron_poblacion['id_depto'].ffill()

# 3️⃣ Nos quedamos solo con las filas que tienen datos de la tabla (Edad no nula)
df_padron_aux = df_padron_poblacion[df_padron_poblacion['Unnamed: 1'].notna()].copy()

# 4️⃣ Renombramos columnas a algo legible
df_padron_limpio = df_padron_aux.rename(columns={
    'Unnamed: 1': 'Edad',
    'Unnamed: 2': 'Casos',
    'Unnamed: 3': 'Porcentaje',
    'Unnamed: 4': 'Porcentaje Acumulado',
})
# 5️⃣ Limpiamos el campo del área (sacamos el texto y nos quedamos con el número)
df_padron_limpio['id_depto'] = df_padron_aux['id_depto'].str.replace('AREA #', '').str.strip()
df_padron_limpio = df_padron_limpio.drop(columns=['area'], errors='ignore')
df_padron_limpio = df_padron_limpio.drop(columns=['Unnamed: 0'], errors='ignore')

# 6️⃣ Nos quedamos solo con filas donde las columnas numéricas son efectivamente números
cols_numericas = ['Edad', 'Casos', 'Porcentaje', 'Porcentaje Acumulado', 'id_depto']
df_padron_limpio = df_padron_limpio.dropna(subset=cols_numericas)  # eliminamos filas con NaN
df_padron_limpio = df_padron_limpio[df_padron_limpio[cols_numericas].applymap(lambda x: str(x).replace('.', '', 1).isdigit()).all(axis=1)]

# 7️⃣ Convertimos las columnas a numérico para asegurarnos
for col in cols_numericas:
    df_padron_limpio[col] = pd.to_numeric(df_padron_limpio[col])


#%% 
#ahora si, tabla poblacion

# diccionario de rangos educativos con edades mínimas y máximas
rangos_educativos = {
    'edad_jardin_maternal': (0, 2),
    'edad_jardin_infantil': (3, 5),
    'edad_primaria': (6, 13),
    'edad_secundaria': (12, 17),
    'edad_secundaria_tecnica': (12, 19),
    'edad_terciario': (18, 120),
    'edad_terciario_tecnico': (18, 120)
}

# función que devuelve la lista de rangos educativos para una edad dada
def asignar_rangos_educativos(edad):
    return [rango for rango, (min_edad, max_edad) in rangos_educativos.items() if min_edad <= edad <= max_edad]

# expandimos el DataFrame para manejar solapamientos, ya que las personas que pueden ir a la secundaria normal, pueden ir a la tecnica, lo mismo con el terciario.
df_expanded = df_padron_limpio.assign(rangos=df_padron_limpio['Edad'].apply(asignar_rangos_educativos)).explode('rangos')
df_expanded = df_expanded.rename(columns={'rangos': 'rango_educativo'})

# sumamos los casos por departamento y rango educativo
query = """
SELECT
    id_depto,
    rango_educativo,
    SUM(casos) AS total_casos
FROM df_expanded
GROUP BY id_depto, rango_educativo
"""
df_suma = dd.query(query).df()

# pivotamos para que cada rango educativo sea una columna
df_poblacion = df_suma.pivot(index='id_depto', columns='rango_educativo', values='total_casos').fillna(0).reset_index()

# Aseguramos que todas las columnas existan aunque no haya datos
columnas = ['id_depto'] + list(rangos_educativos.keys())
for col in columnas:
    if col not in df_poblacion.columns:
        df_poblacion[col] = 0

df_poblacion = df_poblacion[columnas]
print(df_poblacion)
#%%
# tabla ubicación
query_ubicacion = """
SELECT provincia_id AS id_provincia, in_departamentos AS id_depto
FROM df_productivos
"""
df_ubicacion = dd.query(query_ubicacion).df()
#%%

#inner join poblacion con ubicacion TENGO EDADES DE POBLACION CON ID PROVINCIA Y DEPTO
query_poblacion_ubicacion = """
SELECT p.id_depto,id_provincia, edad_jardin_maternal, edad_jardin_infantil, edad_primaria, edad_secundaria, edad_secundaria_tecnica, edad_terciario, edad_terciario_tecnico
FROM df_poblacion p 
INNER JOIN df_ubicacion u
    ON p.id_depto = u.id_depto
"""
df_poblacion_ubicacion = dd.query(query_poblacion_ubicacion).df()
print(df_poblacion_ubicacion)

#agrego Provincia con nombre_provincia y Departamento con nombre_depto
query_pob_ubi_con_nombres = """
SELECT DISTINCT
        nombre_depto AS Departamento,
        nombre_provincia AS Provincia,
        edad_jardin_maternal, 
        edad_jardin_infantil, 
        edad_primaria, 
        edad_secundaria, 
        edad_secundaria_tecnica, 
        edad_terciario, 
        edad_terciario_tecnico
FROM df_poblacion_ubicacion pu
INNER JOIN df_departamento d
    ON pu.id_depto = d.id_depto
INNER JOIN df_provincia p 
    ON pu.id_provincia = p.id_provincia
"""
df_pob_ubi_con_nombres = dd.query(query_pob_ubi_con_nombres).df()

query_ee_con_prov = """
SELECT DISTINCT e.col0 AS Provincia, e.col11 AS Departamento, cueanexo, jardin_maternal, jardin_infantil, primario, secundario, secundario_tecnico, terciario, terciario_tecnico
FROM  df_establecimientos_educativos ee 
INNER JOIN df_educativos e
    ON ee.cueanexo = e.col1
"""
df_ee_con_prov = dd.query(query_ee_con_prov).df()

query_pob_ubi_prov_depto = """
SELECT DISTINCT 
            p.Provincia,
            p.Departamento,
            cueanexo AS Cueanexo,
            jardin_maternal, 
            jardin_infantil, 
            primario, 
            secundario, 
            secundario_tecnico, 
            terciario, 
            terciario_tecnico,
            edad_jardin_maternal, 
            edad_jardin_infantil, 
            edad_primaria, 
            edad_secundaria, 
            edad_secundaria_tecnica, 
            edad_terciario, 
            edad_terciario_tecnico
FROM df_pob_ubi_con_nombres p 
INNER JOIN df_ee_con_prov e
ON (
    p.Provincia  = e.Provincia 
    )
"""
df_pob_ubi_prov_depto = dd.query(query_pob_ubi_prov_depto).df()

#CUENTO EE POR DEPTO Y LUEGO JOIN PARA TERMINAR 
#vemos cantidad de jardines por provincia y departamento
#############################DEBERIA ANDAR ME TIRA ERROR VER!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
query_cant_jardines = """
SELECT
    provincia,
    departamento,
    SUM(jardin_maternal + jardin_infantil) AS Jardines,
    SUM(edad_jardin_maternal + edad_jardin_infantil) AS "Población Jardín"
FROM df_pob_ubi_prov_depto
GROUP BY provincia, departamento;
"""
df_cant_jardines_depto = dd.query(query_cant_jardines).df()

#vemos cantidad de primarios por provincia y departamento
query_cant_primarios = """
SELECT 
    provincia AS Provincia, departamento AS Departamento, Primarias
    COUNT (primario) AS Primarias  
    COUNT (edad_primaria) AS Población Primaria,
FROM df_poblacion_ee_esta_en
GROUP BY provincia AND departamento 
"""
df_cant_primarios_depto = dd.query(query_cant_primarios).df()


#vemos cantidad de secundarios por provincia y departamento
query_cant_secundarios = """
SELECT 
    provincia AS Provincia, departamento AS Departamento, Secundarios
    COUNT (secundario 
           AND 
           secundario_tecnico) AS Secundarios  
    COUNT (edad_secundario 
           AND 
           edad_secundario_tecnico) AS Población Secundario,
FROM df_poblacion_ee_esta_en
GROUP BY provincia AND departamento 
"""
df_cant_secundarios_depto = dd.query(query_cant_secundarios).df()


#vemos cantidad de secundarios por provincia y departamento
query_cant_terciarios = """
SELECT 
    provincia AS Provincia, departamento AS Departamento, Terciarios
    COUNT (terciario 
           AND 
           terciario_tecnico) AS Terciarios  
    COUNT (edad_terciario 
           AND 
           edad_terciario_tecnico) AS Población Terciario,
FROM df_poblacion_ee_esta_en
GROUP BY provincia AND departamento 
"""
df_cant_terciarios_depto = dd.query(query_cant_terciarios).df()

#HAY QUE HACERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR

#unimos cantidades de cada nivel educativo con su provincia y depto y sus rrespectivas poblaciones. Ademas, ordenamos
#las provincias alfabéticamente y las primarias por cantidad de forma descendiente.
query_cant_ee_depto = """
SELECT *
FROM df_cant_jardines_depto j
INNER JOIN  df_cant_primarios_depto p
    ON (j.Provincia = p.Provincia 
        AND
        j.Departamento = p.Departamento)
INNER JOIN df_cant_secundarios_depto s 
    ON (j.Provincia = s.Provincia 
        AND
        j.Departamento = s.Departamento)

INNER JOIN df_cant_terciarios_depto t 
    ON (j.Provincia = p.Provincia 
        AND
        j.Departamento = p.Departamento)
    
ORDER BY Provincia ASC, Primarias DESC
"""
df_cant_ee_depto = dd.query(query_cant_ee_depto).df()
#%%
#A la tabla de Establecimientos Productivos le agrego el nombre de la provincia y del departamento
#haciendo join con df_provincia y df_departamento.
query_ep_en_ubicación = """
SELECT *
FROM df_establecimientos_productivos ep
INNER JOIN df_departamento d
    ON d.id_depto = ep.id_depto
INNER JOIN df_provincia p
    ON p.id_provincia = ep.id_provincia
"""
df_ep_en_ubicación = dd.query(query_ep_en_ubicación).df()

#De la tabla anterior me quedo solo con Nombre Provincia, Nombre Departamento, y cuento cuantos empleados
#hay por cada Provincia y Departamento. Luego ordeno descendientemente por Provincia, y por cantidad de empleados.
query_empleados_por_ubicación = """
SELECT 
    nombre_provincia AS Provincia, nombre_departamento AS Departamento, Cantidad de empleados en 2022
    COUNT(Empleo) AS Cantidad de empleados en 2022
FROM df_ep_en_ubicación 
GROUP BY Provincia AND Departamento
ORDER BY Provincia ASC, Cantidad de empleados en 2022 ASC
"""

df_empleados_por_ubicación = dd.query(query_empleados_por_ubicación).df()

#%%
#tenemos poblacion por depto, con el nombre del depto, y provincia, con el nombre de la provincia 
query_poblacion_por_depto = """
SELECT nombre_depto AS Departamento, Población, nombre_provincia AS Provincia
    (edad_jardin_maternal + edad_jardin_infantil + edad_primaria + edad_secundaria + edad_secundaria_tecnica + edad_terciario + edad_terciario_tecnico) AS Población
FROM df_poblacion p
INNER JOIN df_departamento d
    ON d.nombre_depto = p.id_depto
INNER JOIN df_ubicacion u
ON p.id_depto = u.id_depto
INNER JOIN df_provincia pr
    ON u.id_provincia = pr.id_provincia
"""
df_poblacion_por_depto = dd.query(query_poblacion_por_depto).df()

query_mujeres = """
SELECT COUNT(empresas_exportadoras) AS cant_expo_mujeres, id_depto, id_provincia
FROM df_establecimientos_productivos
WHERE genero = mujeres
GROUP BY id_depto AND id_provincia 
"""
df_mujeres = dd.query(query_mujeres).df()

###################FALTA CANTIDAD DE ESTABLECIMIENTOS EDUCATIVOS (HAY QUE HACERLO POR ID DEPTO Y ESTA EN)
query_prod_pobl_depto = """
SELECT Provincia, Departamento, Población, cantidad_expo_mujeres 
FROM df_poblacion_por_depto p
"""




    

