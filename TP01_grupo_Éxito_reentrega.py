
#Grupo= Éxito
#Integrantes:
#Nombre= Barello, Renata LU=1060/24
#Nombre= Echave Méndez, Manuel LU=1333/23
#Nombre= González Frey, Paloma LU=1030/24
#------------------------------------------------------------------------------------------------------------
# Trabajo Práctico de Laboratorio de Datos
# Descripción:
# El código realiza la carga, limpieza y normalización de datos provenientes de distintos archivos 
# (educativos, productivos y poblacionales) para construir tablas unificadas y responder consultas analíticas.
# Se desarrollan los siguientes ítems:
#   i) Relación entre establecimientos educativos y población en edad escolar por departamento.
#   ii) Cantidad total de empleados por provincia y departamento.
#   iii) Participación laboral de mujeres en empresas exportadoras en relación con la población total.
#   iv) Identificación de los rubros con mayor empleo en departamentos que superan el promedio provincial.
# Finalmente, se generan visualizaciones comparativas y de proporción EE/población para distintos niveles educativos.
#------------------------------------------------------------------------------------------------------------

#%%
import pandas as pd
import duckdb as dd
import unicodedata #para corregir formato de los departamentos
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
#%%
# carga de los 4 archivos dados 


# path
#ruta_actual = os.path.dirname(os.path.abspath(__file__))

# ruta a las tablas
#carpeta = os.path.join(ruta_actual, "TablasOriginales")
# lectura de archivos
carpeta = ("C:/Users/Reni/Desktop/Labo/tp1/")
df_actividades = pd.read_csv(carpeta + 'actividades_establecimientos.csv')
df_educativos = pd.read_excel((carpeta + '2022_padron_oficial_establecimientos_educativos.xlsx'), skiprows=6)
#renombramos las columnas porque hay nombres repetidos y se dificulta referenciarlas bien para armar las tablas correctamente
df_educativos.columns = [f"col{i}" for i in range(len(df_educativos.columns))]
df_productivos = pd.read_csv(carpeta + 'Datos_por_departamento_actividad_y_sexo.csv')
df_padron_poblacion = pd.read_excel((carpeta + 'padron_poblacion.xlsX'), skiprows=12)

#%%
#Código de verificación de calidad de los datos. Para Goal-Question-Metric.

#GQM del cuadro 2022 _ padrón_oficial_establecimientos _ educativos (Establecimientos Educativos)
query_gqm1 = """
SELECT
    COUNT(*) AS filas_totales,
    SUM(
        CASE 
            WHEN col12 LIKE '%/%' 
              OR col12 LIKE '%,%' 
              OR col12 LIKE '% %' 
            THEN 1 
            ELSE 0 
        END
    ) AS filas_con_mas_de_un_mail
FROM df_educativos;
"""
df_gqm1 = dd.query(query_gqm1).df()
#GQM del cuadro Datos_por_departamento_actividad_y_sexo (Establecimientos Productivos)
#Problema: los establecimientos que tienen sólo un género, no aparecen devuelta con el género que no tienen 
#con un 0 en la columna"Empleo".

#Me fijo cuántas filas sin contar las columnas Genero y Empleo, hay distintas. Osea cuantos establecimientos 
#productivos hay en total.
query_gqm2 = """
SELECT
  (SELECT COUNT(*) FROM df_productivos) AS total_filas,
  (SELECT COUNT(*) 
   FROM (
     SELECT DISTINCT 
       anio,
       in_departamentos,
       departamento,
       provincia_id,
       provincia,
       clae6,
       clae2,
       letra,
       Establecimientos,
       empresas_exportadoras
     FROM df_productivos
   ) AS unicas
  ) AS total_unicas;

"""
df_gqm2 = dd.query(query_gqm2).df()

#Acá me aseguro que no haya espacions Null en Empleo:
query_empleo = """
SELECT COUNT(Empleo) AS total_no_nulos
FROM df_productivos
"""
df_empleo = dd.query(query_empleo).df()

establecimientos_con_ambos_generos = (df_gqm2['total_filas'] - df_gqm2['total_unicas'])

establecimientos_con_un_genero = df_gqm2['total_filas'] - (2*(establecimientos_con_ambos_generos))
#%%
# Comenzamos a armar las tablas de nuestro modelo relacional
# tabla establecimientos educativos

query_establecimientos_educativos = """
SELECT col1 AS cueanexo, col20 AS jardin_maternal, col21 AS jardin_infantil, 
       col22 AS primario, col23 AS secundario, col24 AS secundario_tecnico, 
       col25 AS terciario, col26 AS terciario_tecnico
FROM df_educativos
"""
df_establecimientos_educativos = dd.query(query_establecimientos_educativos).df()

#generamos .csv para la carpeta
df_establecimientos_educativos.to_csv('df_establecimientos_educativos')
       

#%%
# tabla departamento
query_departamento = """
SELECT DISTINCT in_departamentos AS id_depto, departamento AS nombre_depto
FROM df_productivos
ORDER BY id_depto DESC
"""
df_departamento = dd.query(query_departamento).df()
def quitar_tildes(texto):
    if isinstance(texto, str):
        # normaliza el texto y elimina los acentos
        texto_sin_tilde = unicodedata.normalize('NFKD', texto)
        return ''.join(c for c in texto_sin_tilde if not unicodedata.combining(c))
    return texto

# aplicamos la función a la columna
df_departamento["nombre_depto"] = df_departamento["nombre_depto"].apply(quitar_tildes)

#generamos .csv para la carpeta

df_departamento.to_csv('df_departamento')

#%%
# tabla provincia
# corregimos nombres y formatos asi luego no tenemos problemas en los JOIN.
query_provincia = """
SELECT DISTINCT
    provincia_id AS id_provincia,
    CASE
        WHEN LOWER(TRIM(provincia)) IN ('caba', 'ciudad autonoma de buenos aires', 'ciudad autónoma de buenos aires') THEN 'Ciudad de Buenos Aires'
        WHEN LOWER(TRIM(provincia)) = 'cordoba' THEN 'Córdoba'
        WHEN LOWER(TRIM(provincia)) = 'entre rios' THEN 'Entre Ríos'
        WHEN LOWER(TRIM(provincia)) = 'tucuman' THEN 'Tucumán'
        WHEN LOWER(TRIM(provincia)) = 'rio negro' THEN 'Río Negro'
        WHEN LOWER(TRIM(provincia)) = 'neuquen' THEN 'Neuquén'
        WHEN LOWER(TRIM(provincia)) = 'santiago del estero' THEN 'Santiago del Estero'
        WHEN LOWER(TRIM(provincia)) = 'tierra del fuego' THEN 'Tierra del Fuego'
        ELSE provincia
    END AS nombre_provincia
FROM df_productivos
"""
df_provincia = dd.query(query_provincia).df()

#generamos .csv para la carpeta

df_provincia.to_csv('df_provincia ')

#%%
# tabla actividad
query_actividad = """
        SELECT clae6,clae6_desc
        FROM df_actividades
"""
df_actividad = dd.query(query_actividad)

#generamos .csv para la carpeta

df_actividad.to_csv('df_actividad')
#%%
# tabla establecimientos productivos
query_establecimientos_productivos = """
    SELECT  provincia_id AS id_provincia, in_departamentos AS id_depto, clae6, genero, Empleo, Establecimientos, empresas_exportadoras
    FROM df_productivos
    WHERE anio == 2022
"""
df_establecimientos_productivos = dd.query(query_establecimientos_productivos).df()

#generamos .csv para la carpeta

df_establecimientos_productivos.to_csv('df_establecimientos_productivos')

#%%
# reformateo de padron_poblacion ya que esta muy feo. 

# renombramos las columnas ya que no son declarativos (se perdieron los nombres originales por el formato del excel dado)

# creamos una columna auxiliar para identificar las filas con "AREA # ..."
df_padron_poblacion['id_depto'] = df_padron_poblacion['Unnamed: 1'].where(df_padron_poblacion['Unnamed: 1'].astype(str).str.startswith('AREA'))

# propagamos el área hacia abajo para rellenar las filas de la subtabla
df_padron_poblacion['id_depto'] = df_padron_poblacion['id_depto'].ffill()

# nos quedamos solo con las filas que tienen datos de la tabla (Edad no nula)
df_padron_aux = df_padron_poblacion[df_padron_poblacion['Unnamed: 1'].notna()].copy()

# renombramos columnas a algo legible
df_padron_limpio = df_padron_aux.rename(columns={
    'Unnamed: 1': 'Edad',
    'Unnamed: 2': 'Casos',
    'Unnamed: 3': 'Porcentaje',
    'Unnamed: 4': 'Porcentaje Acumulado',
})
# limpiamos el campo del área (sacamos el texto y nos quedamos con el número)
df_padron_limpio['id_depto'] = df_padron_aux['id_depto'].str.replace('AREA #', '').str.strip()
df_padron_limpio = df_padron_limpio.drop(columns=['area'], errors='ignore')
df_padron_limpio = df_padron_limpio.drop(columns=['Unnamed: 0'], errors='ignore')

# nos quedamos solo con filas donde las columnas numéricas son efectivamente números
cols_numericas = ['Edad', 'Casos', 'Porcentaje', 'Porcentaje Acumulado', 'id_depto']
df_padron_limpio = df_padron_limpio.dropna(subset=cols_numericas)  # eliminamos filas con NaN
df_padron_limpio = df_padron_limpio[df_padron_limpio[cols_numericas].applymap(lambda x: str(x).replace('.', '', 1).isdigit()).all(axis=1)] #reemplazamos para trabajo mas comodo a posteriori

# convertimos las columnas a numérico para asegurarnos de poder joinear luego sin tener que castear como otro tipo de dato
for col in cols_numericas:
    df_padron_limpio[col] = pd.to_numeric(df_padron_limpio[col])

#%% 
# ahora si, tabla poblacion

# diccionario de rangos educativos con edades mínimas y máximas
rangos_educativos = {
    'edad_jardin_maternal': (0, 2),
    'edad_jardin_infantil': (3, 5),
    'edad_primaria': (6, 12),
    'edad_secundaria': (13, 17),
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

# con esto nos aseguramos que todas las columnas existan aunque no haya datos, con ayuda del diccionario que creamos antes

columnas = ['id_depto'] + list(rangos_educativos.keys())
for col in columnas:
    if col not in df_poblacion.columns:
        df_poblacion[col] = 0

df_poblacion = df_poblacion[columnas]

# generamos .csv para la carpeta

df_poblacion.to_csv('df_poblacion')


#%%
# tabla ubicación
query_ubicacion = """
SELECT DISTINCT provincia_id AS id_provincia, in_departamentos AS id_depto
FROM df_productivos
"""
df_ubicacion = dd.query(query_ubicacion).df()

# generamos .csv para la carpeta

df_ubicacion.to_csv('df_ubicacion')

#%%

#-----------------------------------------------------------------------------------------------------------------------------------------------------------

# comienzo de las actividades y ejercicios solicitados por consigna

#%%
# tabla i)
# tabla cantidad de establecimientos por nivel, en cada departamento y cantidad de poblacion en edades educativas asociadas a los mismos
# inner join poblacion con ubicacion

query_poblacion_ubicacion = """
SELECT DISTINCT p.id_depto,id_provincia, edad_jardin_maternal, edad_jardin_infantil, edad_primaria, edad_secundaria, edad_secundaria_tecnica, edad_terciario, edad_terciario_tecnico
FROM df_poblacion p 
LEFT JOIN df_ubicacion u
    ON p.id_depto = u.id_depto
"""
df_poblacion_ubicacion = dd.query(query_poblacion_ubicacion).df()

#%%
# agregamos Provincia con nombre_provincia y Departamento con nombre_depto
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

#%%
# generacion del df que contiene establecimientos educativos segun provincia
query_ee_con_nombres_prov_y_depto = """
SELECT DISTINCT e.col0 AS Provincia, e.col11 AS Departamento, cueanexo, jardin_maternal, jardin_infantil, primario, secundario, secundario_tecnico, terciario, terciario_tecnico
FROM  df_establecimientos_educativos ee 
INNER JOIN df_educativos e
    ON ee.cueanexo = e.col1
"""
df_ee_con_nombres_prov_y_depto = dd.query(query_ee_con_nombres_prov_y_depto).df()

#%%
# generacion del df que contiene establecimientos educativos segun departamento y joineamos con provincia para tenerlo completo
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
INNER JOIN df_ee_con_nombres_prov_y_depto e
ON (
    LOWER(p.Provincia)  = LOWER(e.Provincia) AND LOWER(p.Departamento) = LOWER(e.Departamento)
    )
"""
df_pob_ubi_prov_depto = dd.query(query_pob_ubi_prov_depto).df()

#%%
# lista con modalidades
cols_a_numericas = [ 
    'jardin_maternal', 'jardin_infantil',
    'primario', 'secundario', 'secundario_tecnico',
    'terciario', 'terciario_tecnico'
]

# convertimos las columnas a numéricas y reemplazamos NaN por 0 para evitar errores
# podriamos hacerlo con CASE WHEN pero son excepciones por cada una y de esta forma chequeamos y corregimos todo junto
for c in cols_a_numericas:
    df_pob_ubi_prov_depto[c] = (
        pd.to_numeric(df_pob_ubi_prov_depto[c], errors='coerce')
        .fillna(0)
    )

#query final unificador

query_tabla_EE_poblacion = """
SELECT
    Provincia,
    Departamento,

    -- cantidad de jardines (maternal + infantil)
    SUM(jardin_maternal + jardin_infantil) AS Jardines,

    -- población total en jardín (ya viene agregada)
    edad_jardin_maternal + edad_jardin_infantil AS "Población Jardín",

    -- cantidad de primarias
    SUM(primario) AS Primarias,

    -- población total primaria (ya viene agregada)
    edad_primaria AS "Población Primaria",

    -- cantidad de secundarios (común + técnica)
    SUM(secundario + secundario_tecnico) AS Secundarios,

    -- población total secundaria (ya viene agregada)
    edad_secundaria_tecnica AS "Población Secundaria",
    

FROM df_pob_ubi_prov_depto
GROUP BY Provincia, Departamento,
         edad_jardin_maternal, edad_jardin_infantil,
         edad_primaria, edad_secundaria_tecnica         
ORDER BY Provincia, Departamento
"""

# tabla final del ejercicio 1
df_tabla_EE_poblacion = dd.query(query_tabla_EE_poblacion).df()

#%%
# tabla cantidad total de empleados por departamento item ii) 

# a la tabla de Establecimientos Productivos le agregamos el nombre de la provincia y del departamento
# haciendo join con df_provincia y df_departamento

query_ep_en_ubicación = """
SELECT *
FROM df_establecimientos_productivos ep
INNER JOIN df_departamento d
    ON d.id_depto = ep.id_depto
INNER JOIN df_provincia p
    ON p.id_provincia = ep.id_provincia
"""
df_ep_en_ubicación = dd.query(query_ep_en_ubicación).df()

#%%

# de la tabla anterior nos quedamos solo con Nombre Provincia, Nombre Departamento, y contamos cuantos empleados
# hay por cada Provincia y Departamento, luego ordenamos descendientemente por Provincia, y por cantidad de empleados

query_empleados_por_ubicacion = """
SELECT 
    nombre_provincia AS Provincia,
    nombre_depto AS Departamento,
    SUM(Empleo) AS Cantidad_de_empleados_en_2022
FROM df_ep_en_ubicación
GROUP BY nombre_provincia, nombre_depto
ORDER BY Provincia ASC, Cantidad_de_empleados_en_2022 DESC
"""
# tabla final del ejercicio 2
df_empleados_por_ubicacion = dd.query(query_empleados_por_ubicacion).df()

#%%
# Tabla item iii)
# tenemos poblacion por depto, con el nombre del depto, y provincia, con el nombre de la provincia
# usamos la tabla df_pob_ubi_con_nombres que tienen la poblacion separada por depto y provincia. Sumo la cantidad total.

query_poblacion_total_por_ubi = """
SELECT 
    Provincia, Departamento, 
    (edad_jardin_maternal + edad_jardin_infantil + edad_primaria + edad_secundaria + edad_terciario) AS Población
    
FROM df_pob_ubi_con_nombres
"""

df_poblacion_total_por_ubi = dd.query(query_poblacion_total_por_ubi).df()

#%%
# tenemos la población total de cada depto.

# ahora, queremos la cantidad de mujeres que trabajan en empresas exportadoras, ponemos un cero en la columna 'Cant_Expo_Mujeres' cuando no cumple así no perdemos informacón
query_mujeres = """
SELECT 
    id_depto,
    id_provincia,
    SUM(
        CASE 
            WHEN genero = 'Mujeres' AND empresas_exportadoras >= 1 THEN empleo 
            ELSE 0 
        END
    ) AS Cant_Expo_Mujeres
FROM df_establecimientos_productivos
GROUP BY id_depto, id_provincia;
"""
df_mujeres = dd.query(query_mujeres).df()

#%%
# tenemos las empleadas mujeres por id_depto, queremos por sus nombres
query_mujeres_nombres = """
SELECT nombre_depto AS Departamento, nombre_provincia AS Provincia, Cant_Expo_Mujeres
FROM df_mujeres m
INNER JOIN df_departamento d 
    ON m.id_depto = d.id_depto 
INNER JOIN df_provincia p 
    ON m.id_provincia = p.id_provincia 
"""
df_mujeres_nombres = dd.query(query_mujeres_nombres).df()

#%%
# tenemos las mujeres que trabajan em empresas exportadoras 

# acá vemos cuantas empresas exportadoras, donde trabajan mujeres, hay por depto

query_ee_mujeres = """
SELECT 
    id_depto,
    id_provincia,
    SUM(
        CASE 
            WHEN genero = 'Mujeres' THEN empresas_exportadoras
            ELSE 0
        END
    ) AS Cant_EE
FROM df_establecimientos_productivos
GROUP BY id_depto, id_provincia;
"""
df_ee_mujeres = dd.query(query_ee_mujeres).df()

#%%

# tenemos las empresas exportadoras de mujeres por id_depto, las queremos por sus nombres
query_ee_mujeres_nombres = """
SELECT nombre_depto AS Departamento, nombre_provincia AS Provincia, Cant_EE
FROM df_ee_mujeres m
INNER JOIN df_departamento d 
    ON m.id_depto = d.id_depto 
INNER JOIN df_provincia p 
    ON m.id_provincia = p.id_provincia 
"""
df_ee_mujeres_nombres = dd.query(query_ee_mujeres_nombres).df()
# armamos la tabla final, juntamos todo con un JOIN por Provincia y Departamento 

#%%

# consulta final, hacemos JOIN por provincia y depto
query_mujeres_en_eexp = """
SELECT p.Provincia, p.Departamento, Cant_Expo_Mujeres, Cant_EE, Población
FROM df_poblacion_total_por_ubi p
INNER JOIN df_mujeres_nombres m 
    ON (p.Provincia = m.Provincia 
        AND 
        p.Departamento = m.Departamento)
INNER JOIN df_ee_mujeres_nombres ee
    ON (p.Provincia = ee.Provincia 
        AND 
        p.Departamento = ee.Departamento)
ORDER BY Cant_EE DESC, Cant_Expo_Mujeres DESC, p.Provincia ASC, p.Departamento ASC
"""
df_mujeres_en_eexp = dd.query(query_mujeres_en_eexp).df()

# %% Tabla Item IV
# buscamos los departamentos con más empleo que el promedio provincial y
# dentro de ellos, el rubro (primeros 3 dígitos del CLAE6) con más trabajadores

# promedio de empleos por provincia
query_promedio_provincia = """
    SELECT 
        u.id_provincia,
        AVG(e.Empleo) AS promedio_empleo_provincia
    FROM df_establecimientos_productivos AS e
    INNER JOIN df_ubicacion AS u
        ON e.id_depto = u.id_depto
    GROUP BY u.id_provincia
"""
df_promedio_provincia = dd.query(query_promedio_provincia).df()

#%% total de empleos por departamento
query_empleo_departamento = """
    SELECT 
        u.id_provincia,
        u.id_depto,
        SUM(e.Empleo) AS empleo_total_departamento
    FROM df_establecimientos_productivos AS e
    INNER JOIN df_ubicacion AS u
        ON e.id_depto = u.id_depto
    GROUP BY u.id_provincia, u.id_depto
"""
df_empleo_departamento = dd.query(query_empleo_departamento).df()

#%% departamentos cuyo empleo total supera el promedio provincial
query_departamentos_sobre_promedio = """
    SELECT 
        d.id_provincia,
        d.id_depto,
        d.empleo_total_departamento
    FROM df_empleo_departamento AS d
    INNER JOIN df_promedio_provincia AS p
        ON d.id_provincia = p.id_provincia
    WHERE d.empleo_total_departamento > p.promedio_empleo_provincia
"""
df_departamentos_sobre_promedio = dd.query(query_departamentos_sobre_promedio).df()

#%% empleo por los primeros tres dígitos del CLAE6 en cada departamento
# usamos LPAD para agregar el 0 si es necesario (el CLAE6 a veces no llega a 6 digitos), y luego tomamos los 3 primeros
query_empleo_por_clae6 = """
    SELECT 
        u.id_provincia,
        u.id_depto,
        SUBSTR(LPAD(CAST(e.clae6 AS VARCHAR), 6, '0'), 1, 3) AS clae6_3digitos,
        SUM(e.Empleo) AS empleo_en_clae6
    FROM df_establecimientos_productivos AS e
    INNER JOIN df_ubicacion AS u
        ON e.id_depto = u.id_depto
    GROUP BY u.id_provincia, u.id_depto, clae6_3digitos
"""
df_empleo_por_clae6 = dd.query(query_empleo_por_clae6).df()

#%% rubro que más empleo genera en cada departamento
query_clae6_top_por_departamento = """
    SELECT 
        e1.id_provincia,
        e1.id_depto,
        e1.clae6_3digitos,
        e1.empleo_en_clae6
    FROM df_empleo_por_clae6 AS e1
    INNER JOIN (
        SELECT 
            id_provincia, 
            id_depto, 
            MAX(empleo_en_clae6) AS max_empleo
        FROM df_empleo_por_clae6
        GROUP BY id_provincia, id_depto
    ) AS e2
        ON e1.id_provincia = e2.id_provincia
        AND e1.id_depto = e2.id_depto
        AND e1.empleo_en_clae6 = e2.max_empleo
"""
df_clae6_top_por_departamento = dd.query(query_clae6_top_por_departamento).df()

#%% resultado final tabla 4:  rubro más fuerte, con cantidad de empleados asociados de los departamentos con empleo mayor al promedio provincial

query_rubro_empleo_max = """
    SELECT 
        p.nombre_provincia,
        dpto.nombre_depto,
        c.clae6_3digitos AS clae6_mas_empleo,
        c.empleo_en_clae6 AS empleo_en_rubro
    FROM df_departamentos_sobre_promedio AS dep
    INNER JOIN df_clae6_top_por_departamento AS c
        ON dep.id_provincia = c.id_provincia
        AND dep.id_depto = c.id_depto
    INNER JOIN df_provincia AS p
        ON dep.id_provincia = p.id_provincia
    INNER JOIN df_departamento AS dpto
        ON dep.id_depto = dpto.id_depto
    ORDER BY p.nombre_provincia, dpto.nombre_depto
"""

df_rubro_empleo_max = dd.query(query_rubro_empleo_max).df()

# finaliza la parte de tablas del tp.

#%%
# comenzamos la parte de visualizacion
# visualizacion 1)
query_empleados_por_prov = """
SELECT 
    Provincia,
    SUM(Cantidad_de_empleados_en_2022) AS cant_por_prov
FROM df_empleados_por_ubicacion
GROUP BY Provincia
ORDER BY cant_por_prov DESC
"""

df_empleados_por_prov = dd.query(query_empleados_por_prov).df()


# configuramos el estilo general
plt.style.use('seaborn-v0_8-darkgrid')

# creamos el objeto
fig, ax = plt.subplots(figsize=(12, 6))

# elegimos grafico de barras ya que es muy facil notar las diferencias entre cantidades y la cantidad de provincias no es tan grande que lo hace engorroso.
ax.bar(df_empleados_por_prov['Provincia'], df_empleados_por_prov['cant_por_prov'], 
       color='#4A90E2', edgecolor='black', alpha=0.85)

# título y etiquetas
ax.set_title('Cantidad de empleados por provincia (2022)', fontsize=16, fontweight='bold', pad=15)
ax.set_xlabel('Provincia', fontsize=12)
ax.set_ylabel('Cantidad de empleados (en millones)', fontsize=12)

# rotamos etiquetas del eje X para facilitar la lectura
plt.xticks(rotation=45, ha='right')

# agregamos valores encima de las barras para mayor presicion 
for i, val in enumerate(df_empleados_por_prov['cant_por_prov']):
    ax.text(i, val + val*0.01, f"{int(val):,}", ha='center', va='bottom', fontsize=9)

# ajustamos márgenes
plt.tight_layout()

# grafico en si
plt.show()

#%%
# visualizacion 2)

# para poder sacar conclusiones mas directas y con mas fundamentos decidimos cocientar a los EE por la poblacion para tener una metrica comparable
# luego, tendremos un grafico por cada nivel, jardin, primaria, y secundaria. 
# decidimos usar scatter plot, con el eje y condicionado por el cociente, y la posicion en el eje x que sea simplemente el departamento asi evitamos solapamiento.

# copiamos el df original
df = df_tabla_EE_poblacion.copy()

# calculamos los cocientes por nivel educativo
df['ratio_jardin'] = df['Jardines'] / df['Población Jardín']
df['ratio_primaria'] = df['Primarias'] / df['Población Primaria']
df['ratio_secundaria'] = df['Secundarios'] / df['Población Secundaria']

# definimos grupos etarios
grupos_etarios = {
    'Jardín': '0-6 años',
    'Primaria': '6-13 años',
    'Secundaria': '12-17 años'
}

# comenzamos con el nivel de primaria
nivel = 'Primaria'
col_pob = f'Población {nivel}'
col_ratio = f'ratio_{nivel.lower()}'

# ordenamos para identificar extremos
df_sorted = df.sort_values(col_ratio, ascending=False)
top5 = df_sorted.head(5)
bottom5 = df_sorted.tail(5)

# coloreo
palette_top = sns.color_palette("tab10", 5)      # 5 colores para top
palette_bottom = sns.color_palette("Set2", 5)    # 5 colores para bottom
color_neutro = "lightgrey"

# creamos el objeto

plt.figure(figsize=(10,6))
sns.scatterplot(
    data=df, 
    x=col_pob, 
    y=col_ratio, 
    color=color_neutro, 
    s=60, 
    alpha=0.7,
    label="Otros departamentos"
)

# agregamos top5
for i, (_, row) in enumerate(top5.iterrows()):
    plt.scatter(row[col_pob], row[col_ratio], color=palette_top[i], s=80, label=f"Top {i+1}: {row['Departamento']}")
    

# agregamos bottom5 
for i, (_, row) in enumerate(bottom5.iterrows()):
    plt.scatter(row[col_pob], row[col_ratio], color=palette_bottom[i], s=80, label=f"Bottom {i+1}: {row['Departamento']}")
   
# ajustes finales y grafico

plt.title(f'Relación EE / Población - Nivel {nivel} ({grupos_etarios[nivel]})', fontsize=13)
plt.xlabel(f'Población total del nivel {nivel.lower()}', fontsize=11)
plt.ylabel('Proporción EE por habitante', fontsize=11)
plt.grid(True, alpha=0.3)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
plt.tight_layout()
plt.show()

#%%
# visualizacion 2
# nivel jardín
nivel = 'Jardín'
col_pob = f'Población {nivel}'
col_ratio = 'ratio_jardin'

# ordenamos para identificar extremos
df_sorted = df.sort_values(col_ratio, ascending=False)
top5 = df_sorted.head(5)
bottom5 = df_sorted.tail(5)

# coloreo
palette_top = sns.color_palette("tab10", 5)
palette_bottom = sns.color_palette("Set2", 5)
color_neutro = "lightgrey"

# gráfico
plt.figure(figsize=(10,6))
sns.scatterplot(
    data=df,
    x=col_pob,
    y=col_ratio,
    color=color_neutro,
    s=60,
    alpha=0.7,
    label="Otros departamentos"
)

# top5
for i, (_, row) in enumerate(top5.iterrows()):
    plt.scatter(row[col_pob], row[col_ratio], color=palette_top[i], s=80, label=f"Top {i+1}: {row['Departamento']}")

# bottom5
for i, (_, row) in enumerate(bottom5.iterrows()):
    plt.scatter(row[col_pob], row[col_ratio], color=palette_bottom[i], s=80, label=f"Bottom {i+1}: {row['Departamento']}")

# estilo
plt.title(f'Relación EE / Población - Nivel {nivel} ({grupos_etarios[nivel]})', fontsize=13)
plt.xlabel(f'Población total del nivel {nivel.lower()}', fontsize=11)
plt.ylabel('Proporción EE por habitante', fontsize=11)
plt.grid(True, alpha=0.3)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
plt.tight_layout()
plt.show()


#%%
# visualizacion 2
# nivel secundaria
nivel = 'Secundaria'
col_pob = f'Población {nivel}'
col_ratio = f'ratio_{nivel.lower()}'

# ordenamos para identificar extremos
df_sorted = df.sort_values(col_ratio, ascending=False)
top5 = df_sorted.head(5)
bottom5 = df_sorted.tail(5)

# coloreo
palette_top = sns.color_palette("tab10", 5)
palette_bottom = sns.color_palette("Set2", 5)
color_neutro = "lightgrey"

# gráfico
plt.figure(figsize=(10,6))
sns.scatterplot(
    data=df,
    x=col_pob,
    y=col_ratio,
    color=color_neutro,
    s=60,
    alpha=0.7,
    label="Otros departamentos"
)

# top5
for i, (_, row) in enumerate(top5.iterrows()):
    plt.scatter(row[col_pob], row[col_ratio], color=palette_top[i], s=80, label=f"Top {i+1}: {row['Departamento']}")

# bottom5
for i, (_, row) in enumerate(bottom5.iterrows()):
    plt.scatter(row[col_pob], row[col_ratio], color=palette_bottom[i], s=80, label=f"Bottom {i+1}: {row['Departamento']}")

# estilo
plt.title(f'Relación EE / Población - Nivel {nivel} ({grupos_etarios[nivel]})', fontsize=13)
plt.xlabel(f'Población total del nivel {nivel.lower()}', fontsize=11)
plt.ylabel('Proporción EE por habitante', fontsize=11)
plt.grid(True, alpha=0.3)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
plt.tight_layout()
plt.show()

#%%
# visualizacion 3
# necesitamos saber la cantidad de Establecimientos Educativos por provincia. creamos tabla df_esta_en para tener los Establecimientos Educativos por id_provincia e id_depto. 
# hacemos una consulta SQL para quedarnos con los Establecimientos Educativos por provinica, con sus nombres (de las provincias), en un mismo df.

# asociamos ubicacion con nombre
query_ubicacion_con_nombres = """
SELECT 
    u.id_provincia,p.nombre_provincia,
    u.id_depto, d.nombre_depto
FROM df_ubicacion u
INNER JOIN df_provincia p
    ON u.id_provincia = p.id_provincia
INNER JOIN df_departamento d
    ON u.id_depto = d.id_depto
ORDER BY u.id_depto DESC
"""

df_ubicacion_con_nombres = dd.query(query_ubicacion_con_nombres).df()

query_esta_en="""
    SELECT e.col1, a.id_provincia, a.id_depto 
    FROM df_ubicacion_con_nombres a
    RIGHT JOIN df_educativos e
    ON ((LOWER(e.col0) = LOWER(a.nombre_provincia)) AND (LOWER(e.col11) = LOWER(a.nombre_depto)))
"""

df_esta_en=dd.query(query_esta_en).df()


query_ee_por_prov_depto = """
SELECT DISTINCT id_provincia, id_depto, COUNT(col1) AS EE
FROM df_esta_en 
GROUP BY id_provincia, id_depto 
"""

df_ee_por_prov_depto = dd.query(query_ee_por_prov_depto).df()

query_ee_prov_depto_nombres = """
SELECT nombre_provincia AS Provincia, nombre_depto AS Departamento, EE AS "Establecimientos Educativos"
FROM df_ee_por_prov_depto ee
INNER JOIN df_provincia p 
    ON ee.id_provincia = p.id_provincia
INNER JOIN df_departamento d 
    ON ee.id_depto = d.id_depto
"""

df_ee_prov_depto_nombres = dd.query(query_ee_prov_depto_nombres).df()


ee_prov_depto_nombres_sin_outliers = """
SELECT * 
FROM df_ee_prov_depto_nombres 
WHERE "Establecimientos Educativos" < 500
"""
df_ee_prov_depto_nombres_sin_outliers = dd.query(ee_prov_depto_nombres_sin_outliers).df()

outliers = """
SELECT Departamento, Provincia, "Establecimientos Educativos"
FROM df_ee_prov_depto_nombres AS df
WHERE "Establecimientos Educativos" >= 500
"""
df_outliers = dd.query(outliers).df()

# gráfico
df = df_ee_prov_depto_nombres_sin_outliers.copy()
col = "Establecimientos Educativos"

# ordenamos las provincias por la MEDIANA de EE (depto) dentro de cada provincia
orden = (df.groupby("Provincia")[col]
           .median()
           .sort_values()           # de menor a mayor
           .index.tolist())

# preparamos los arrays (uno por provincia)
data = [df.loc[df["Provincia"] == prov, col].values for prov in orden]

# cambiamos los colores del gráfico para que quede más lindo
COLOR_BOX_FILL  = "#BFDBFE"   # color de las cajitas (relleno)
COLOR_EDGE      = "#2F4F4F"  # color de bordes de cajas, bigotes y topes
COLOR_MEDIAN    = "#1E40AF"  # color de la línea de la mediana
COLOR_OUTLIERS  = "#BFDBFE"  # color de los puntos outliers
COLOR_AX_BG     = "#FFFFFF"  # color de fondo del área del gráfico (axes)
COLOR_FIG_BG    = "#F7F7F7"  # color de fondo de toda la figura (figure)

# creamos, el gráfico, con los boxplots en una misma figura con colores personalizados
fig, ax = plt.subplots(figsize=(18, 6))  # creamos figura y ejes

bp = ax.boxplot(
    data, labels=orden, showfliers=True, patch_artist=True,   # patch_artist=True permite colorear las cajitas
    boxprops     = dict(facecolor=COLOR_BOX_FILL, edgecolor=COLOR_EDGE),   # color de cajitas (relleno) y bordes
    medianprops  = dict(color=COLOR_MEDIAN, linewidth=2),                  # color de la línea de la mediana
    whiskerprops = dict(color=COLOR_EDGE),                                 # color de los bigotes
    capprops     = dict(color=COLOR_EDGE),                                 # color de los topes de los bigotes
    flierprops   = dict(marker='o', markerfacecolor=COLOR_OUTLIERS,
                        markeredgecolor=COLOR_OUTLIERS, alpha=0.6)         # color de los outliers (puntitos)
)

ax.set_facecolor(COLOR_AX_BG)   # color de fondo del área del gráfico
fig.patch.set_facecolor(COLOR_FIG_BG)  # color de fondo de toda la figura

ax.set_ylabel("Cantidad de Establecimientos Educativos por Departamento")
ax.set_xlabel("Provincias (ordenadas por mediana)")
ax.set_title("Establecimientos Educativos por Departamento por Provincia (ordenados por mediana)")
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()
#%%
# visualizacion 4
#%%

# vamos a usar las tablas df_poblacion_total_por_ubi y df_empleados_por_ubcacion. Nos queda relacionarlas 
# y df_ee_prov_depto_nombres. 

# primero buscamos la relación entre la cantidad de empleados cada mil habitantes.
query_relacion_empleados_poblacion = """
SELECT 
    pu.Provincia,
    pu.Departamento,
   
    ROUND(
        1000.0 * COALESCE(CAST(epu.Cantidad_de_empleados_en_2022 AS DOUBLE), 0)
        / NULLIF(CAST(pu.Población AS DOUBLE), 0),
        2
    ) AS "Empleados c/1000"
FROM df_poblacion_total_por_ubi AS pu
LEFT JOIN df_empleados_por_ubicacion AS epu
  ON pu.Provincia = epu.Provincia
 AND pu.Departamento = epu.Departamento
"""
df_relacion_empleados_poblacion = dd.query(query_relacion_empleados_poblacion).df()

# luego, buscamos la relación de la cantidad de EE cada 1000 habitantes por departamento.
query_relacion_ee_poblacion = """
SELECT 
    ee.Provincia,
    ee.Departamento,
    ROUND(
        1000.0 * ee."Establecimientos Educativos" / NULLIF(p.Población, 0), 3
    ) AS "EE por 1000 hab"
FROM df_ee_prov_depto_nombres ee
INNER JOIN df_poblacion_total_por_ubi p 
  ON ee.Provincia = p.Provincia
 AND ee.Departamento = p.Departamento
"""
df_relacion_ee_poblacion = dd.query(query_relacion_ee_poblacion).df()

query_relacion = """
SELECT reu.Provincia, reu.Departamento, "EE por 1000 hab" AS "EE c/1000", "Empleados c/1000"
FROM df_relacion_empleados_poblacion reu
INNER JOIN df_relacion_ee_poblacion reep
    ON (reu.Provincia = reep.Provincia
        AND 
        reu.Departamento = reep.Departamento)
ORDER BY "EE c/1000" ASC
"""
df_relacion = dd.query(query_relacion).df()

# consultamos cual es el departamento con casi 30 empleados cada mil habitantes para referenciar luego en el informe

query_consulta = """
SELECT DISTINCT *
FROM df_relacion r 
WHERE "Empleados c/1000" > 10
"""
df_consulta = dd.query(query_consulta).df()

# limpieza mínima
dfp = df_relacion.dropna(subset=["Empleados c/1000", "EE c/1000"]).copy()

# subconjunto para ajuste dentro del rango visible (0–10)
dfp_fit = dfp.query('`Empleados c/1000` >= 0 and `Empleados c/1000` <= 10 and `EE c/1000` >= 0 and `EE c/1000` <= 10').copy()
if len(dfp_fit) < 2:
    dfp_fit = dfp

# regresión ortogonal via PCA, la tendencia basicamente (tls). la generamos para tener una linea general de referencia contra la que comparar
def tls_fit(x, y):
    x = np.asarray(x); y = np.asarray(y)
    xbar, ybar = x.mean(), y.mean()
    C = np.cov(x, y, ddof=0)
    vals, vecs = np.linalg.eigh(C)
    v = vecs[:, -1]           # autovector principal
    a = v[1] / v[0]           # pendiente
    b = ybar - a * xbar       # intercepto
    return a, b

# generacion
m_tls, b_tls = tls_fit(dfp_fit["EE c/1000"].values, dfp_fit["Empleados c/1000"].values)
xline = np.linspace(0, 10, 200)

# scatter básico EE contra poblacion
fig, ax = plt.subplots(figsize=(8,6))
sns.scatterplot(
    data=dfp,
    x="EE c/1000",              # X = EE
    y="Empleados c/1000",       # Y = Empleados
    s=120, alpha=0.85, edgecolor="white", linewidth=0.5, ax=ax
)
ax.plot(xline, m_tls*xline + b_tls, linestyle="-", linewidth=2.5, color="black", label="Tendencia (TLS)")

# correlación
r = dfp_fit["EE c/1000"].corr(dfp_fit["Empleados c/1000"])
ax.text(0.02, 0.98, f"r = {r:.2f}", transform=ax.transAxes, ha="left", va="top")

# límites y etiquetas
ax.set_xlim(0, 10); ax.set_ylim(0, 10)
ax.set_title("EE c/1000 (X) vs Empleados c/1000 (Y) por departamento (2022)")
ax.set_xlabel("Establecimientos Educativos por 1000 hab")
ax.set_ylabel("Empleados por 1000 hab")
ax.grid(True, alpha=0.3)
ax.legend(loc="lower right")
plt.tight_layout()
plt.show()

# scatter por provincia con diferencia de colores
provincias = sorted(dfp["Provincia"].dropna().unique())
pal_big = (
    sns.color_palette("tab20", 20)
    + sns.color_palette("tab20b", 20)
    + sns.color_palette("tab20c", 20)
    + sns.color_palette("Set3", 12)
    + sns.color_palette("Dark2", 8)
)
# paleta a utilizar (gradiente)
palette = {prov: pal_big[i % len(pal_big)] for i, prov in enumerate(provincias)}

# nombres y paleta
plt.figure(figsize=(10,7))
ax2 = sns.scatterplot(
    data=dfp,
    x="EE c/1000",
    y="Empleados c/1000",
    hue="Provincia",
    palette=palette,
    s=120, alpha=0.85, edgecolor="white", linewidth=0.5
)
ax2.plot(xline, m_tls*xline + b_tls, linestyle="-", linewidth=2.5, color="black", label="Tendencia (TLS)")

ax2.set_xlim(0, 10); ax2.set_ylim(0, 10)
plt.title("Relación por Deptos: EE (X) vs Empleados (Y)")
plt.xlabel("Establecimientos Educativos cada 1000 habitantes")
plt.ylabel("Empleados cada 1000 habitantes")

# leyenda (provincias + línea)
handles, labels = ax2.get_legend_handles_labels()
ax2.legend(handles, labels, bbox_to_anchor=(1.02,1), loc="upper left",
           borderaxespad=0., fontsize=8, ncol=1, title="Provincia", markerscale=1.2)

# corregimos el layout
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

#%%
# visualizacion 5

# las 5 actividades (CLAE6) con mayor y menor proporción (respectivamente)
# de empleadas mujeres, para 2022. Incluir en el gráfico la proporción
# promedio de empleo femenino.

# primero hacemos una tabla a partir de la tabla df_establecimientos_productivos,
# obteniendo clae6, y sumando la cantidad de empleos distinguiendo entre varones y mujeres, y 
# haciendo una nueva columna que me diga el porcentaje de mujeres que hay en cada clae6.

query_empleo_por_act = """
SELECT 
    clae6,
    SUM(CASE WHEN genero = 'Mujeres' THEN Empleo ELSE 0 END) AS Total_Empleo_Mujeres_Por_Act,
    SUM(CASE WHEN genero = 'Varones' THEN Empleo ELSE 0 END) AS Total_Empleo_Varones_Por_Act,
    ROUND(
        1.0 * SUM(CASE WHEN genero = 'Mujeres' THEN Empleo ELSE 0 END)
        / NULLIF(SUM(Empleo), 0), 4
    ) AS prop_mujeres

FROM df_establecimientos_productivos
GROUP BY clae6
ORDER BY prop_mujeres DESC
"""

df_empleo_por_act = dd.query(query_empleo_por_act).df()

# filtramos actividades con al menos alguna mujer empleada
df_filtrado = df_empleo_por_act[df_empleo_por_act["prop_mujeres"] > 0].copy()

# calculamos promedio total
promedio_total = df_filtrado["prop_mujeres"].mean()

# seleccionamos las 5 mayores y 5 menores proporciones (mayores a 0)
mayor_proporcion = df_filtrado.nlargest(5, "prop_mujeres")
menor_proporcion = df_filtrado.nsmallest(5, "prop_mujeres")

# unimos ambas para graficar
df_grafico1 = pd.concat([mayor_proporcion, menor_proporcion])

query_grafico = """
SELECT 
    g.clae6,
    g.Total_Empleo_Mujeres_Por_Act,
    g.Total_Empleo_Varones_Por_Act,
    g.prop_mujeres,
    a.clae6_desc
FROM df_grafico1 g
INNER JOIN df_actividades a
ON g.clae6 = a.clae6
"""
df_grafico = dd.query(query_grafico).df()

# ordenar de mayor a menor según prop_mujeres
df_sorted = df_grafico.sort_values(by='prop_mujeres', ascending=False)

fig, ax = plt.subplots(figsize=(10,6))

# barras
bars = ax.bar(df_sorted['clae6'].astype(str),
              df_sorted['prop_mujeres'],
              color="#60A5FA",
              alpha=0.8)

# línea del promedio
ax.axhline(promedio_total, color="#2563EB", linestyle='--', linewidth=2,
           label=f'Promedio total: {promedio_total:.2%}')

# etiquetas de porcentaje
for bar, prop in zip(bars, df_sorted['prop_mujeres']):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height(),
        f"{prop:.2%}",
        ha='center', va='bottom',
        fontsize=10, fontweight='bold'
    )

# creamos una leyenda textual combinando clae6 y descripción
legend_text = [f"{c} – {d}" for c, d in zip(df_sorted['clae6'], df_sorted['clae6_desc'])]

# agregamos la leyenda textual fuera del gráfico
ax.legend(
    [plt.Line2D([0], [0], color="#60A5FA", lw=6)] * len(df_sorted),
    legend_text,
    title="CLAE6 – Descripción",
    bbox_to_anchor=(1.05, 1),
    loc='upper left',
    frameon=False
)

# estética y grafico final
ax.set_xlabel('CLAE6', fontsize=12)
ax.set_ylabel('Proporción de mujeres', fontsize=12)
ax.set_title('Proporción de mujeres por actividad (CLAE6)', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()