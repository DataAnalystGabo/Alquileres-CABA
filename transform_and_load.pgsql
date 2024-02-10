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
ADD COLUMN Barrio VARCHAR(255);

UPDATE alquileres.publicaciones
SET Barrio =
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
    direccion = NULLIF(direccion, 'NULL'),
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
    direccion = 'NULL' OR
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


SELECT * FROM alquileres.publicaciones;
