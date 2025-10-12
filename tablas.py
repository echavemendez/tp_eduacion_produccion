import pandas as pd
import numpy as np
import duckdb as dd

# Cargar los 4 archivos directamente
"""
df_actividades = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/actividades_establecimientos.csv')
"""
df_educativos = pd.read_excel('/content/drive/MyDrive/Colab Notebooks/2022_padron_oficial_establecimientos_educativos.xlsx', skiprows=6)

#renombro las columnas porque hay nombres repetidos y se dificulta referenciarlas bien para armar las tablas correctamente
df_educativos.columns = [f"col{i}" for i in range(len(df_educativos.columns))]

df_productivos = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/Datos_por_departamento_actividad_y_sexo.csv')

"""
df_padron_poblacion = pd.read_excel('/content/drive/MyDrive/Colab Notebooks/padron_poblacion.xlsX')
"""


# tabla establecimientos educativos
# renombramos las columnas con nombres declarativos correspondientes
query_establecimientos_educativos="""
                SELECT col1 AS cueanexo, col20 AS jardin_maternal, col21 AS jardin_infantil, col22 AS primario, col23 AS secundario, col24 AS secundario_tecnico, col25 AS terciario, col26 AS terciario_tecnico
                FROM df_educativos
"""

df_establecimientos_educativos=dd.query(query_establecimientos_educativos).df()


# tabla realiza HACER

df_realiza=

#tabla esta_en

query_esta_en = """
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
    ON CAST(e.provincia AS VARCHAR) = CAST(p.nombre_provincia AS VARCHAR)
"""
df_esta_en = dd.query(query_esta_en).df()
print(df_esta_en)



# tabla establecimientos productivos
# por ahora puse como identificador de la actividad solo al clae6

query_establecimientos_productivos="""
                SELECT provincia_id AS id_provincia, in_departamentos AS id_depto, anio, clae6
                FROM df_productivos
"""

df_establecimientos_productivos=n=dd.query(query_establecimientos_productivos).df()


# tabla poblacion
# deberia ser id_depto, edad_primaria, edad_secundaria, edad_terciario, edad_jardin_maternal.. etc

query_poblacion="""
                SELECT provincia_id AS id_provincia, in_departamentos AS id_depto, anio, clae6
                FROM df_padron_poblacion
"""

df_poblacion=dd.query(query_poblacion).df()



# tabla ubicacion
# ver como hacer para unificar tablas con mujeres y hombres asociados a una misma empresa, a si mismo hay un ejercicio que pide cosas con el clae6 asociado
# tal vez necesitamos la tabla completa y es al pedo subseccionarla la verdad.
query_ubicacion="""
                SELECT provincia_id AS id_provincia, in_departamentos AS id_depto,
                FROM df_productivos
"""

df_ubicacion=dd.query(query_ubicacion).df()



# tabla departamento
query_departamento="""
                    SELECT in_departamentos AS id_depto,  departamento AS nombre_depto
                    FROM df_productivos
"""
df_departamento=dd.query(query_departamento).df()



# tabla provincia
# armamos CASE WHEN para poder corregir las provincias sin tilde, y luego no tener problema al hacer joins
query_provincia = """
SELECT
    provincia_id AS id_provincia,
    CASE
        WHEN LOWER(TRIM(provincia)) IN ('caba', 'ciudad autonoma de buenos aires', 'ciudad autónoma de buenos aires') THEN 'Ciudad Autónoma de Buenos Aires'
        WHEN LOWER(TRIM(provincia)) = 'Cordoba' THEN 'Córdoba'
        WHEN LOWER(TRIM(provincia)) = 'Entre Rios' THEN 'Entre Ríos'
        WHEN LOWER(TRIM(provincia)) = 'Tucuman' THEN 'Tucumán'
        WHEN LOWER(TRIM(provincia)) = 'Rio Negro' THEN 'Río Negro'
        WHEN LOWER(TRIM(provincia)) = 'Neuquen' THEN 'Neuquén'
        ELSE provincia
    END AS nombre_provincia
FROM df_productivos
"""
df_provincia = dd.query(query_provincia).df()



# tabla actividad HACER
# tambien ver que poner aca
query_actividad="""
                SELECT clae2, clae6, letra,
"""
df_actividad=