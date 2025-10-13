import pandas as pd
import numpy as np
import duckdb as dd

#%%
# Cargar los 4 archivos directamente
carpeta = "C:/Users/Manuel/Desktop/tp_labo"
df_actividades = pd.read_csv(carpeta + '/actividades_establecimientos.csv')

df_educativos = pd.read_excel(carpeta + '/2022_padron_oficial_establecimientos_educativos.xlsx', skiprows=6)

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
SELECT in_departamentos AS id_depto, departamento AS nombre_depto
FROM df_productivos
"""
df_departamento = dd.query(query_departamento).df()

#%%
# tabla provincia
# corregimos nombres y formatos asi luego no tenemos problemas en los JOIN.
query_provincia = """
SELECT
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
    'edad_jardin_infantil': (0, 2),
    'edad_jardin_maternal': (3, 5),
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



#%%
# tabla ubicación
query_ubicacion = """
SELECT provincia_id AS id_provincia, in_departamentos AS id_depto
FROM df_productivos
"""
df_ubicacion = dd.query(query_ubicacion).df()
#%%
"""
#creeria que esta bien pero no me corre asi que  A VERIFICAR.
query_esta_en = 
SELECT
    e.cueanexo,
    d.id_depto,
    p.id_provincia
FROM (
    SELECT
        col1 AS cueanexo,
        col0 AS provincia,
        col11 AS departamento
    FROM df_educativos
) AS e
LEFT JOIN df_departamento d
    ON e.departamento = d.nombre_depto
LEFT JOIN df_provincia p
    ON e.provincia = p.nombre_provincia

df_esta_en=dd.query(query_esta_en).df()
"""