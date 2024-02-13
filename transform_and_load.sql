-- Active: 1707571777599@@127.0.0.1@5432@postgres
-- Creamos nuestra tabla dentro de la base "alquileres". Por defecto, decidí
-- que todas las variables sean del tipo VARCHAR ya que, nuestro dataset original
-- cuenta con columnas con tipos de valores mixtos. El objetivo es normalizar
-- los datos y su tipo para culminar en un dataset limpio y correctamente estructurado*/
CREATE TABLE
    alquileres.publicaciones (
        id_publicacion VARCHAR(255) NULL,
        tipo VARCHAR(255) NULL,
        titulo VARCHAR(255) NULL,
        direccion VARCHAR(255) NULL,
        m2_totales VARCHAR(255) NULL,
        m2_cubiertos VARCHAR(255) NULL,
        m2_balcon VARCHAR(255) NULL,
        ambientes VARCHAR(255) NULL,
        dormitorios VARCHAR(255) NULL,
        banios VARCHAR(255) NULL,
        cocheras VARCHAR(255) NULL,
        admite_mascotas VARCHAR(255) NULL,
        piso VARCHAR(255) NULL,
        antiguedad VARCHAR(255) NULL,
        disposicion VARCHAR(255) NULL,
        orientacion VARCHAR(255) NULL,
        amoblado VARCHAR(255) NULL,
        moneda_precio VARCHAR(255) NULL,
        precio VARCHAR(255) NULL,
        expensas VARCHAR(255) NULL
    );

-- Introducimos los datos a nuestra tabla creada desde el archivo csv
COPY alquileres.publicaciones 
    FROM 'C:\Users\Usuario\OneDrive\Documentos\GaboData\Alquileres-CABA\dfs\df_alquileres.csv'
WITH (FORMAT CSV, HEADER, ENCODING 'UTF-8');

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

-- En esta sección, identificaremos los registros duplicados.
-- Luego de una exhaustiva inspección he dado con que el dataset
-- cuenta con dos tipos de registros duplicados. Una tanda de duplicados (Grupo A)
-- tienen todas sus columnas repetidas excepto los valores de la categoría "id_publicacion".
-- Otro conjunto de registros (Grupo B) duplican todos sus valores en todas sus categorías
-- excepto en la columna "precio". Estas incongruencias son producto de errores cometidos
-- durante el proceso de recolección de datos con Selenium.
-- Es por esto, que he decidio darle un tratamiento distinto a cada grupo para eliminar aquellos
-- registros que se encuentran duplicados.

-- Tratamiento de valores duplicados Grupo A:
ALTER TABLE alquileres.publicaciones
ADD COLUMN temp_id SERIAL;

SELECT (alquileres.publicaciones.tipo,
        alquileres.publicaciones.titulo,
        alquileres.publicaciones.direccion,
        alquileres.publicaciones.m2_totales,
        alquileres.publicaciones.m2_cubiertos,
        alquileres.publicaciones.m2_balcon,
        alquileres.publicaciones.ambientes,
        alquileres.publicaciones.dormitorios,
        alquileres.publicaciones.banios,
        alquileres.publicaciones.cocheras,
        alquileres.publicaciones.admite_mascotas,
        alquileres.publicaciones.piso,
        alquileres.publicaciones.antiguedad,
        alquileres.publicaciones.disposicion,
        alquileres.publicaciones.orientacion,
        alquileres.publicaciones.amoblado,
        alquileres.publicaciones.moneda_precio,
        alquileres.publicaciones.precio,
        alquileres.publicaciones.expensas)::text, COUNT(*) AS freq
FROM alquileres.publicaciones
GROUP BY (
        alquileres.publicaciones.tipo,
        alquileres.publicaciones.titulo,
        alquileres.publicaciones.direccion,
        alquileres.publicaciones.m2_totales,
        alquileres.publicaciones.m2_cubiertos,
        alquileres.publicaciones.m2_balcon,
        alquileres.publicaciones.ambientes,
        alquileres.publicaciones.dormitorios,
        alquileres.publicaciones.banios,
        alquileres.publicaciones.cocheras,
        alquileres.publicaciones.admite_mascotas,
        alquileres.publicaciones.piso,
        alquileres.publicaciones.antiguedad,
        alquileres.publicaciones.disposicion,
        alquileres.publicaciones.orientacion,
        alquileres.publicaciones.amoblado,
        alquileres.publicaciones.moneda_precio,
        alquileres.publicaciones.precio,
        alquileres.publicaciones.expensas)
HAVING COUNT(*) > 1
ORDER BY freq DESC;

DELETE FROM alquileres.publicaciones
WHERE temp_id IN(
    SELECT temp_id
    FROM(
        SELECT temp_id,
            ROW_NUMBER() OVER(
                PARTITION BY
                tipo,
                titulo,
                direccion,
                m2_totales,
                m2_cubiertos,
                m2_balcon,
                ambientes,
                dormitorios,
                banios,
                cocheras,
                admite_mascotas,
                piso,
                antiguedad,
                disposicion,
                orientacion,
                amoblado,
                moneda_precio,
                precio,
                expensas
            ORDER BY temp_id
            ) AS row_number
        FROM alquileres.publicaciones
    ) sub
    WHERE sub.row_number > 1
);


-- Tratamiento de valores duplicados Grupo B:

SELECT id_publicacion, COUNT(*) AS FREQ
FROM alquileres.publicaciones
GROUP BY id_publicacion
HAVING COUNT(*) > 1;


DELETE FROM alquileres.publicaciones
WHERE temp_id IN (
    SELECT temp_id
    FROM (
        SELECT temp_id,
               MIN(temp_id) OVER (PARTITION BY id_publicacion) AS min_temp_id
        FROM alquileres.publicaciones
        WHERE id_publicacion IN (
            SELECT id_publicacion
            FROM alquileres.publicaciones
            GROUP BY id_publicacion
            HAVING COUNT(*) > 1
        )
    ) sub
    WHERE temp_id = min_temp_id
);


-- Analizando y normalizando la columna "direccion"

ALTER TABLE alquileres.publicaciones
ADD COLUMN barrio VARCHAR(255);

UPDATE alquileres.publicaciones
SET barrio =
    CASE
        WHEN direccion LIKE '%Agronomía%' THEN 'Agronomía'
        WHEN direccion LIKE '%Almagro%' THEN 'Almagro'
        WHEN direccion LIKE '%Balvanera%' THEN 'Balvanera'
        WHEN direccion LIKE '%Barracas%' THEN 'Barracas'
        WHEN direccion LIKE '%Barrio Norte%' THEN 'Barrio Norte'
        WHEN direccion LIKE '%Belgrano%' THEN 'Belgrano'
        WHEN direccion LIKE '%Boedo%' THEN 'Boedo'
        WHEN direccion LIKE '%Botánico%' THEN 'Palermo'
        WHEN direccion LIKE '%Caballito%' THEN 'Caballito'
        WHEN direccion LIKE '%Chacarita%' THEN 'Chacarita'
        WHEN direccion LIKE '%Coghlan%' THEN 'Coghlan'
        WHEN direccion LIKE '%Colegiales%' THEN 'Colegiales'
        WHEN direccion LIKE '%Congreso%' THEN 'Congreso'
        WHEN direccion LIKE '%Constitución%' THEN 'Constitución'
        WHEN direccion LIKE '%Flores%' THEN 'Flores'
        WHEN direccion LIKE '%Floresta%' THEN 'Floresta'
        WHEN direccion LIKE '%La Boca%' THEN 'La Boca'
        WHEN direccion LIKE '%La Paternal%' THEN 'La Paternal'
        WHEN direccion LIKE '%Las Cañitas%' THEN 'Las Cañitas'
        WHEN direccion LIKE '%Liniers%' THEN 'Liniers'
        WHEN direccion LIKE '%Mataderos%' THEN 'Mataderos'
        WHEN direccion LIKE '%Monserrat%' THEN 'Monserrat'
        WHEN direccion LIKE '%Monte Castro%' THEN 'Monte Castro'
        WHEN direccion LIKE '%Nueva Pompeya%' THEN 'Nueva Pompeya'
        WHEN direccion LIKE '%Núñez%' THEN 'Núñez'
        WHEN direccion LIKE '%Palermo%' THEN 'Palermo'
        WHEN direccion LIKE '%Parana Al 1000%' THEN 'Recoleta'
        WHEN direccion LIKE '%Parque Avellaneda%' THEN 'Parque Avellaneda'
        WHEN direccion LIKE '%Parque Chacabuco%' THEN 'Parque Chacabuco'
        WHEN direccion LIKE '%Parque Chas%' THEN 'Parque Chas'
        WHEN direccion LIKE '%Parque Patricios%' THEN 'Parque Patricios'
        WHEN direccion LIKE '%Puerto Madero%' THEN 'Puerto Madero'
        WHEN direccion LIKE '%Recoleta%' THEN 'Recoleta'
        WHEN direccion LIKE '%Retiro%' THEN 'Retiro'
        WHEN direccion LIKE '%Once%' THEN 'Once'
        WHEN direccion LIKE '%Saavedra%' THEN 'Saavedra'
        WHEN direccion LIKE '%San Cristóbal%' THEN 'San Cristóbal'
        WHEN direccion LIKE '%San Nicolás%' THEN 'San Nicolás'
        WHEN direccion LIKE '%San Telmo%' THEN 'San Telmo'
        WHEN direccion LIKE '%Santa Rita%' THEN 'Santa Rita'
        WHEN direccion LIKE '%Velez Sársfield%' OR direccion LIKE '%Velez Sarsfield%' THEN 'Velez Sársfield'
        WHEN direccion LIKE '%Versalles%' THEN 'Versalles'
        WHEN direccion LIKE '%Villa Crespo%' THEN 'Villa Crespo'
        WHEN direccion LIKE '%Villa Devoto%' THEN 'Villa Devoto'
        WHEN direccion LIKE '%Villa del Parque%' THEN 'Villa del Parque'
        WHEN direccion LIKE '%Villa General Mitre%' OR direccion LIKE '%Villa Gral. Mitre%' THEN 'Villa General Mitre'
        WHEN direccion LIKE '%Villa Lugano%' THEN 'Villa Lugano'
        WHEN direccion LIKE '%Villa Luro%' THEN 'Villa Luro'
        WHEN direccion LIKE '%Villa Ortúzar%' THEN 'Villa Ortúzar'
        WHEN direccion LIKE '%Paternal%' THEN 'Paternal'
        WHEN direccion LIKE '%Villa Pueyrredón%' THEN 'Villa Pueyrredón'
        WHEN direccion LIKE '%Villa Real%' THEN 'Villa Real'
        WHEN direccion LIKE '%Villa Riachuelo%' THEN 'Villa Riachuelo'
        WHEN direccion LIKE '%Villa Santa Rita%' THEN 'Villa Santa Rita'
        WHEN direccion LIKE '%Villa Soldati%' THEN 'Villa Soldati'
        WHEN direccion LIKE '%Villa Urquiza%' THEN 'Villa Urquiza'
        WHEN direccion LIKE '%Sarmiento 1400%' THEN 'San Nicolas'
        WHEN direccion LIKE '%Reconquista Al 700%' THEN 'San Nicolas'
        WHEN direccion LIKE '%Maipú Al 800%' THEN 'San Nicolas'
        WHEN direccion LIKE '%Avenida Lope De Vega Al 1000%' THEN 'Villa Luro'
        WHEN direccion LIKE '%Bahia Blanca 1756%' THEN 'Floresta'
        ELSE 'Sin barrio'
    END;

-- Eliminamos la columna "direccion" ya que nuestro análisis se basará
-- en la distribución geográfica de los alquileres por barrio.
ALTER TABLE alquileres.publicaciones
DROP COLUMN direccion;

SELECT barrio, COUNT(*)
FROM alquileres.publicaciones
GROUP BY barrio
ORDER BY COUNT(*) DESC;

SELECT *
FROM alquileres.publicaciones
WHERE barrio = 'Sin barrio';

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

-- Examinamos la columna tipo de nuestro dataset para denotar
-- que tenemos dos registros con la categoría "Departamento en vent". Procedemos a eliminarlos.

SELECT tipo, COUNT(*)
FROM alquileres.publicaciones
GROUP BY tipo;

DELETE FROM
alquileres.publicaciones
WHERE tipo = 'Departamento en Venta';

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

-- En nuestro dataset tenemos valores del tipo string "NULL".
-- Estos valores no son valores null como PostgreSQL lo entiende, son producto del código generado en python
-- al momento de scrappear el sitio web para que aquellos valores en donde no existía. Lo que logramos con 
-- la siguiente consulta es convertirlos estrictamente a valores null.

UPDATE alquileres.publicaciones
SET
    id_publicacion = NULLIF(id_publicacion, 'NULL'),
    tipo = NULLIF(tipo, 'NULL'),
    titulo = NULLIF(titulo, 'NULL'),
    m2_totales = NULLIF(m2_totales, 'NULL'),
    m2_cubiertos = NULLIF(m2_cubiertos, 'NULL'),
    m2_balcon = NULLIF(m2_balcon, 'NULL'),
    ambientes = NULLIF(ambientes, 'NULL'),
    dormitorios = NULLIF(dormitorios, 'NULL'),
    banios = NULLIF(banios, 'NULL'),
    cocheras = NULLIF(cocheras, 'NULL'),
    admite_mascotas = NULLIF(admite_mascotas, 'NULL'),
    piso = NULLIF(piso, 'NULL'),
    antiguedad = NULLIF(antiguedad, 'NULL'),
    disposicion = NULLIF(disposicion, 'NULL'),
    orientacion = NULLIF(orientacion, 'NULL'),
    amoblado = NULLIF(amoblado, 'NULL'),
    moneda_precio = NULLIF(moneda_precio, 'NULL'),
    precio = NULLIF(precio, 'NULL'),
    expensas = NULLIF(expensas, 'NULL')
WHERE
    id_publicacion = 'NULL' OR
    tipo = 'NULL' OR
    titulo = 'NULL' OR
    m2_totales = 'NULL' OR
    m2_cubiertos = 'NULL' OR
    m2_balcon = 'NULL' OR
    ambientes = 'NULL' OR
    dormitorios = 'NULL' OR
    banios = 'NULL' OR
    cocheras = 'NULL' OR
    admite_mascotas = 'NULL' OR
    piso = 'NULL' OR
    antiguedad = 'NULL' OR
    disposicion = 'NULL' OR
    orientacion = 'NULL' OR
    amoblado = 'NULL' OR
    moneda_precio = 'NULL' OR
    precio = 'NULL' OR
    expensas = 'NULL';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Normalizemos los valores de las categorias m2_totales, m2_cubiertos, m2_balcon. Vamos a eliminar la unidad de medida "m2" y a
-- convertirlos en valores de tipo integer.

-- Creamos una nueva columna de tipo integer
ALTER TABLE alquileres.publicaciones
ADD COLUMN m2_totales_temp INTEGER;

-- Limpiamos los valores de la unidad de medida "m2"
UPDATE alquileres.publicaciones
SET m2_totales = TRIM(split_part(m2_totales, ' ', 1));

-- Seteamos los nuevos valores transformándolos en enteros a la nueva columna creada
UPDATE alquileres.publicaciones
SET m2_totales_temp =
CASE 
    WHEN m2_totales ~ '\.' THEN CAST(TRIM(split_part(m2_totales, '.', 1)) AS INTEGER)
    ELSE CAST(m2_totales AS INTEGER)
END;

-- Borramos la columna original
ALTER TABLE alquileres.publicaciones
DROP COLUMN m2_totales;

-- Renombramos la columna última creada
ALTER TABLE alquileres.publicaciones
RENAME COLUMN m2_totales_temp TO m2_totales;

-- Visualizamos la frecuencia de los datos del campo m2_totales
SELECT m2_totales, COUNT(*)
FROM alquileres.publicaciones
GROUP BY m2_totales;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Realizamos las mismas operaciones para la columna m2_cubiertos
ALTER TABLE alquileres.publicaciones
ADD COLUMN m2_cubiertos_temp INTEGER;


-- Eliminamos la unidad "m2" de los valores
UPDATE alquileres.publicaciones
SET m2_cubiertos = TRIM(split_part(m2_cubiertos, ' ', 1));


-- Tratamos los valores que contengan un punto, un cero o sean nulos.
UPDATE alquileres.publicaciones
SET m2_cubiertos_temp =
CASE 
    WHEN  m2_cubiertos ~ '\.' THEN CAST(TRIM(split_part(m2_cubiertos, '.', 1)) AS INTEGER) 
    WHEN m2_cubiertos = NULL THEN NULL
    WHEN m2_cubiertos = '0' THEN NULL
    ELSE
        CAST(m2_cubiertos AS INTEGER) 
END


-- Eliminamos la columna "m2_cubiertos"
ALTER TABLE alquileres.publicaciones
DROP COLUMN m2_cubiertos;


-- Y por último, renombramos la columna "m2_cubiertos_temp"
ALTER TABLE alquileres.publicaciones
RENAME COLUMN m2_cubiertos_temp TO m2_cubiertos;


-- Analizamos la columna final
SELECT m2_cubiertos, COUNT(*)
FROM alquileres.publicaciones
GROUP BY m2_cubiertos;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Vamos a normalizar los valores de la columna "m2_balcon"
ALTER TABLE alquileres.publicaciones
ADD COLUMN m2_balcon_temp INTEGER;


-- Eliminamos la unidad "m2" de los valores
UPDATE alquileres.publicaciones
SET m2_balcon = TRIM(split_part(m2_balcon, ' ', 1));


-- Tratamos los valores que contengan un punto, un cero o sean nulos.
UPDATE alquileres.publicaciones
SET m2_balcon_temp =
CASE 
    WHEN  m2_balcon ~ '\.' THEN CAST(TRIM(split_part(m2_balcon, '.', 1)) AS INTEGER) 
    WHEN m2_balcon = NULL THEN NULL
    WHEN m2_balcon = '0' THEN NULL
    ELSE
        CAST(m2_balcon AS INTEGER) 
END


-- Eliminamos la columna "m2_balcon"
ALTER TABLE alquileres.publicaciones
DROP COLUMN m2_balcon;


-- Renombramos la columna "m2_balcon_temp"
ALTER TABLE alquileres.publicaciones
RENAME COLUMN m2_balcon_temp TO m2_balcon;


-- Examinamos el resultado final
SELECT m2_balcon, COUNT(*)
FROM alquileres.publicaciones
GROUP BY m2_balcon;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Vayamos por la columna "ambientes"

-- A simple vista podemos observar que no tenemos valores de tipo flotante
-- por lo que no será necesario realizar ningún operación de split sobre
-- los valores de tipo string
SELECT ambientes, COUNT(*)
FROM alquileres.publicaciones
GROUP BY ambientes;


-- Procedemos a reconvertir los valores, en valores de tipo INTEGER
ALTER TABLE alquileres.publicaciones
ADD COLUMN ambientes_temp INTEGER;


-- Traspasamos los valores a la nueva columna creada
UPDATE alquileres.publicaciones
SET ambientes_temp = CAST(ambientes AS INTEGER);


-- Eliminamos y renombramos columnas
ALTER TABLE alquileres.publicaciones
DROP COLUMN ambientes;


ALTER TABLE alquileres.publicaciones
RENAME COLUMN ambientes_temp TO ambientes;


-- Analizemos el resultado final
SELECT ambientes, COUNT(*)
FROM alquileres.publicaciones
GROUP BY ambientes
ORDER BY COUNT(*) DESC;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Trabajemos la columna "dormitorios". Veamos que hay!
SELECT dormitorios, COUNT(*)
FROM alquileres.publicaciones
GROUP BY dormitorios;


-- Creamos una nueva columna
ALTER TABLE alquileres.publicaciones
ADD COLUMN dormitorios_temp INTEGER;


-- Seteamos los valores a la columna recientemente creada
UPDATE alquileres.publicaciones
SET dormitorios_temp = CAST(dormitorios AS INTEGER);


-- Eliminamos y renombramos las columnas que hemos operado
ALTER TABLE alquileres.publicaciones
DROP COLUMN dormitorios;


ALTER TABLE alquileres.publicaciones
RENAME COLUMN dormitorios_temp TO dormitorios;


-- Veamos el resultado final
SELECT dormitorios, COUNT(*)
FROM alquileres.publicaciones
GROUP BY dormitorios;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Miremos la columna "banios"
SELECT banios, COUNT(*)
FROM alquileres.publicaciones
GROUP BY banios;


-- Creemos una nueva columna
ALTER TABLE alquileres.publicaciones
ADD COLUMN banios_temp INTEGER;


-- Seteemos la nueva columna con los valores convertidos a enteros
UPDATE alquileres.publicaciones
SET banios_temp = CAST(banios AS INTEGER);


-- Eliminamos, renombramos y analizamos la columna resultante
ALTER TABLE alquileres.publicaciones
DROP COLUMN banios;


ALTER TABLE alquileres.publicaciones
RENAME COLUMN banios_temp TO banios;


SELECT banios, COUNT(*)
FROM alquileres.publicaciones
GROUP BY banios;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Transformemos y analizemos la columna "cocheras"
SELECT cocheras, COUNT(*)
FROM alquileres.publicaciones
GROUP BY cocheras;


-- Creamos una nueva columna
ALTER TABLE alquileres.publicaciones
ADD COLUMN cocheras_temp INTEGER;


-- Agregamos los valores reconvertidos a la nueva columna
UPDATE alquileres.publicaciones
SET cocheras_temp = CAST(cocheras AS INTEGER);


-- Eliminamos, renombramos y analizamos el resultado
ALTER TABLE alquileres.publicaciones
DROP COLUMN cocheras;


ALTER TABLE alquileres.publicaciones
RENAME COLUMN cocheras_temp TO cocheras;


SELECT cocheras, COUNT(*)
FROM alquileres.publicaciones
GROUP BY cocheras;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Veamos que trae la columna "admite_mascotas"
SELECT admite_mascotas, COUNT(*)
FROM alquileres.publicaciones
GROUP BY admite_mascotas;


-- Eliminamos el acento a la palabra "Sí" para evitar futuros
-- problemas que pueda generar con el tipo de ENCODING
UPDATE alquileres.publicaciones
SET admite_mascotas = 'Si'
WHERE admite_mascotas = 'Sí';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Avancemos con la columna piso
SELECT piso, COUNT(*)
FROM alquileres.publicaciones
GROUP BY piso;


-- Creamos una nueva columna
ALTER TABLE alquileres.publicaciones
ADD COLUMN ubicacion_piso INTEGER


-- Seteamos la columna reciente
UPDATE alquileres.publicaciones
SET ubicacion_piso = CAST(piso AS INTEGER);


-- Eliminamos la columna "piso" y revisamos el resultado
ALTER TABLE alquileres.publicaciones
DROP COLUMN piso;


SELECT ubicacion_piso, COUNT(*)
FROM alquileres.publicaciones
GROUP BY ubicacion_piso;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Avancemos con la columna antiguedad
SELECT antiguedad, COUNT(*)
FROM alquileres.publicaciones
GROUP BY antiguedad
ORDER BY COUNT(*) DESC;


-- Creamos una nueva columna
ALTER TABLE alquileres.publicaciones
ADD COLUMN antiguedad_en_anios INTEGER;


-- Eliminamos la palabra "años" de los valores de la categoría antiguedad
UPDATE alquileres.publicaciones
SET antiguedad_en_anios = 
    CASE 
        WHEN antiguedad = '2015 años' THEN 9
        WHEN antiguedad = '2024 años' THEN 0
        WHEN antiguedad = '-2 años' THEN 2
        ELSE
            CAST(split_part(antiguedad, ' ', 1) AS INTEGER)
    END;


-- Eliminamos la columna original y corroboramos el resultado
ALTER TABLE alquileres.publicaciones
DROP COLUMN antiguedad;


SELECT antiguedad_en_anios, COUNT(*)
FROM alquileres.publicaciones
GROUP BY antiguedad_en_anios
ORDER BY COUNT(*) DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Avancemos con la columna amoblado
SELECT amoblado, COUNT(*)
FROM alquileres.publicaciones
GROUP BY amoblado;


-- Eliminamos la tilde de la categoría "Si"
UPDATE alquileres.publicaciones
SET amoblado = 'Si'
WHERE amoblado = 'Sí';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Avancemos con la columna moneda_precio para el tipo de moneda dólar

-- En principio vamos a tratar aquellos valores en dolares que contienen un punto.
SELECT precio, moneda_precio, COUNT(*)
FROM alquileres.publicaciones
WHERE moneda_precio = 'U$S'
AND
precio ~ '\.'
GROUP BY precio, moneda_precio
ORDER BY precio DESC;


-- En este punto, mirando registro por registro con la intención de validar sus precios me he dado cuenta que el dataset contiene publicaciones mal catalogadas. Es decir, el propietario subió alquileres temporales en la categoría alquileres ordinarios. Es por esto, que he decidido eliminar todos los registros que en su titulo contenga la palabra "temporal" ó "Temporal"
DELETE FROM alquileres.publicaciones
WHERE id_publicacion IN(
    SELECT id_publicacion
    FROM alquileres.publicaciones
    WHERE titulo LIKE '%Temporal%'
    OR
    titulo LIKE '%temporal%'
) RETURNING *;


-- Normalización de los precios en dólares para los valores del tipo  600.0
UPDATE alquileres.publicaciones
SET precio = LEFT(precio, 3)
WHERE moneda_precio = 'U$S' AND precio ~ '^\d{3}\.';


-- Normalización de los precios en dólares para los valores del tipo  60.0
UPDATE alquileres.publicaciones
SET precio = LEFT(precio, 2)
WHERE moneda_precio = 'U$S' AND precio ~ '^\d{2}\.';


-- Normalización de los precios en dólares para los valores del tipo  6.0
UPDATE alquileres.publicaciones
SET precio = CONCAT(REPLACE(precio, '.', ''), '00')
WHERE precio ~ '^\d\.\d$'
AND moneda_precio = 'U$S';


-- Normalización de los precios en dólares para los valores del tipo  6.55
UPDATE alquileres.publicaciones
SET precio = CONCAT(REPLACE(precio, '.', ''), '0')
WHERE precio ~ '^\d\.\d{2}'
AND moneda_precio = 'U$S';



-- Realizamos las mismas operaciones para los valores en pesos argentinos
SELECT precio
FROM alquileres.publicaciones
WHERE moneda_precio = '$'
AND
precio ~ '\.'
GROUP BY precio
ORDER BY precio;


-- Normalizamos los valores en pesos
UPDATE alquileres.publicaciones
SET precio = CONCAT(REPLACE(precio, '.', ''), '000')
WHERE precio ~ '^\d{3}\.\d{1}'
AND moneda_precio = '$';


-- Corroboramos que nuestra columna precio haya sido normalizada en su totalidad
SELECT precio, moneda_precio
FROM alquileres.publicaciones
GROUP BY precio, moneda_precio
ORDER BY moneda_precio DESC;


-- Por último crearemos una nueva columna que soporte valores de tipo entero y migraremos nuestros datos actuales de la columna precio.
-- Luego, haremos la conversión de los precios en dólares a pesos según el tipo de cambio vigente al momento del scrapping (5/02/2024 - dálar blue $1140)
ALTER TABLE alquileres.publicaciones
ADD COLUMN precio_temp INTEGER;


-- Creamos una nueva columna
UPDATE alquileres.publicaciones
SET precio_temp =
    CAST(precio AS INTEGER);


-- Visualizamos el formato de los valores en dólares
SELECT precio_temp, moneda_precio
FROM alquileres.publicaciones
WHERE moneda_precio = 'U$S'
GROUP BY precio_temp, moneda_precio
ORDER BY precio_temp ASC;


-- Realizamos la conversión a pesos
UPDATE alquileres.publicaciones
SET precio_temp =
    precio_temp * 1140
WHERE moneda_precio = 'U$S';


-- Establecemos que todos los valores son en pesos
UPDATE alquileres.publicaciones
SET moneda_precio = '$'
WHERE moneda_precio = 'U$S';


-- Eliminamos la columna orginal de precio
ALTER TABLE alquileres.publicaciones
DROP COLUMN precio;


-- Renombramos precio_temp
ALTER TABLE alquileres.publicaciones
RENAME COLUMN precio_temp TO precio;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Trabajemos con la columna expensas. Veamos que hay:
SELECT expensas, COUNT(*) AS freq
FROM alquileres.publicaciones
GROUP BY expensas;


-- Los valores vienen acompañados de 'ARS', 'AR', o 'A'.
UPDATE alquileres.publicaciones
SET expensas = TRIM(split_part(expensas, ' ', 1))
WHERE expensas LIKE '% %';


-- Hemos detectado que un valor contiene un punto
SELECT expensas
FROM alquileres.publicaciones
WHERE expensas ~ '\.';


-- Tratemos ese valor
UPDATE alquileres.publicaciones
SET expensas = 
    TRIM(split_part(expensas, '.', 1))
WHERE expensas ~ '\.';


-- Creamos una nueva columna
ALTER TABLE alquileres.publicaciones
ADD COLUMN expensas_temp INTEGER;


-- Transferimos nuestros valores a la nueva columna
UPDATE alquileres.publicaciones
SET expensas_temp =
    CAST(expensas AS INTEGER);


-- Eliminamos, renombramos y analizamos el resultado
ALTER TABLE alquileres.publicaciones
DROP COLUMN expensas;


ALTER TABLE alquileres.publicaciones
RENAME COLUMN expensas_temp TO expensas;


SELECT expensas, moneda_precio
FROM alquileres.publicaciones
GROUP BY expensas, moneda_precio
ORDER BY expensas;



SELECT * FROM alquileres.publicaciones;








