/*
-----------------------------------------
--    ANALIZAR TASA DE VALORES NULOS   --
--            @AmadeusCelta            --
-----------------------------------------

Evaluar el porcentaje de valores nulos en una base de datos es una práctica fundamental en el análisis de datos y 
la gestión de bases de datos. Esta evaluación proporciona una visión clara de la calidad de los datos y puede ayudar 
a identificar problemas potenciales en la integridad y confiabilidad de la información.

A continuación, te presento algunas de las razones más comunes por las que es necesario evaluar el porcentaje de nulos:

Calidad de los datos:
- Identificar datos faltantes: Los valores nulos indican que hay información faltante, lo que puede afectar la 
  precisión de los análisis y las decisiones basadas en esos datos.
- Evaluar la integridad de los datos: Un alto porcentaje de valores nulos en una columna o tabla puede indicar 
  problemas en el proceso de recopilación o carga de datos, como errores en los formularios, fallas en la integración 
  de datos o pérdida de información.

Preparación de datos para análisis:
- Imputación de valores: Antes de realizar análisis estadísticos o de machine learning, es necesario decidir cómo 
  tratar los valores nulos (imputar, eliminar registros, etc.).
- Transformación de datos: En algunos casos, los valores nulos pueden convertirse en una categoría adicional (por ejemplo, 
  "desconocido") o utilizarse para crear nuevas variables indicadoras.

Optimización de consultas:
- Identificar columnas con muchos valores nulos: Al conocer las columnas con un alto porcentaje de valores nulos, se 
  pueden optimizar las consultas SQL, evitando operaciones innecesarias sobre datos faltantes.
- Crear índices: Los índices pueden mejorar el rendimiento de las consultas, pero no son eficientes para columnas con 
  muchos valores nulos.

Diseño de la base de datos:
- Validación del diseño: Un alto porcentaje de valores nulos en una columna puede indicar que el diseño de la base de 
  datos no es adecuado o que los requisitos de negocio han cambiado.
- Modificación del esquema: En algunos casos, puede ser necesario modificar el esquema de la base de datos para manejar 
  mejor los valores nulos, por ejemplo, agregando una columna adicional para indicar si un valor es nulo o no.

Detección de anomalías:
- Errores en la carga de datos: Un número inusualmente alto de valores nulos en una columna puede indicar un error en 
  el proceso de carga de datos.
- Cambios en los procesos de negocio: Un aumento repentino en el porcentaje de valores nulos puede señalar cambios en 
  los procesos de negocio que afectan la captura de datos.


En resumen, evaluar el porcentaje de valores nulos es una tarea esencial para garantizar la calidad y confiabilidad de 
los datos en una base de datos. Al identificar y comprender los patrones de valores nulos, se pueden tomar medidas para 
mejorar la calidad de los datos, optimizar las consultas y garantizar que los análisis sean precisos y confiables.

Para la generación de este script se hace uso de MySQL versión 8.2

-----------------------------------------
*/


-----------------------------------------
--         ANALIZAR UNA TABLA          -- 
-----------------------------------------

DELIMITER $$

CREATE PROCEDURE obtener_porcentaje_nulos_Xtabla(IN nombre_tabla VARCHAR(100))
BEGIN
  -- Declaracion de variables
  DECLARE done INT DEFAULT FALSE;
  DECLARE nombre_columna VARCHAR(100);
  DECLARE total_registros INT;
  DECLARE nulos INT;
  DECLARE porcentaje_nulos DECIMAL(5,2);

  -- Declaramos un cursor para almacenar las columnas de la tabla, con esta se hara un loop para obtener la informacion
  DECLARE cur CURSOR FOR
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = DATABASE()
    AND table_name = nombre_tabla;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  DROP TABLE IF EXISTS tabla_resultados;
  CREATE TEMPORARY TABLE tabla_resultados (
    columna VARCHAR(100),
    total_registros INT,
    nulos INT,
    porcentaje_nulos DECIMAL(5,2)
  );

  OPEN cur;

  read_loop: LOOP
    FETCH cur INTO nombre_columna;
    IF done THEN
      LEAVE read_loop;
    END IF;

    SET @sql = CONCAT('SELECT COUNT(*) INTO @total_registros FROM ', nombre_tabla, ' WHERE ', nombre_columna, ' IS NOT NULL');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('SELECT COUNT(*) INTO @nulos FROM ', nombre_tabla, ' WHERE ', nombre_columna, ' IS NULL');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET porcentaje_nulos = IF(@total_registros = 0, 0, (@nulos / (@total_registros + @nulos)) * 100);

    INSERT INTO tabla_resultados (columna, total_registros, nulos, porcentaje_nulos)
    VALUES (nombre_columna, @total_registros, @nulos, porcentaje_nulos);

  END LOOP;

  CLOSE cur;

  SELECT * FROM tabla_resultados;

END $$

DELIMITER ;


-----------------------------------------
-- ANALIZAR TODAS LAS TABLAS DE UNA BD --
-----------------------------------------

DELIMITER $$

CREATE PROCEDURE obtener_porcentaje_nulos()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE nombre_tabla VARCHAR(100);
  DECLARE nombre_columna VARCHAR(100);
  DECLARE total_registros INT;
  DECLARE nulos INT;

  DECLARE cur CURSOR FOR
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE table_schema = DATABASE();

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  DROP TABLE IF EXISTS tabla_resultados;
  CREATE TEMPORARY TABLE tabla_resultados (
    tabla VARCHAR(100),
    columna VARCHAR(100),
    total_registros INT,
    nulos INT,
    porcentaje_nulos DECIMAL(5,2)
  );

  OPEN cur;

  read_loop: LOOP
    FETCH cur INTO nombre_tabla, nombre_columna;
    IF done THEN
      LEAVE read_loop;
    END IF;

    SET @sql = CONCAT('SELECT COUNT(*) INTO @total_registros FROM ', nombre_tabla);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('SELECT COUNT(*) INTO @nulos FROM ', nombre_tabla, ' WHERE ', nombre_columna, ' IS NULL');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @porcentaje_nulos = IF(@total_registros = 0, 0, (@nulos / @total_registros) * 100);

    INSERT INTO tabla_resultados (tabla, columna, total_registros, nulos, porcentaje_nulos)
    VALUES (nombre_tabla, nombre_columna, @total_registros, @nulos, @porcentaje_nulos);

  END LOOP;

  CLOSE cur;

  SELECT * FROM tabla_resultados;

END $$

DELIMITER ;

CALL obtener_porcentaje_nulos();


/*
-----------------------------------------
--        EJEMPLO DE APLICACION        --
-----------------------------------------

Presumamos que tenemos una BD llamada MiPrueba que contiene dos tablas ejemplo01 y ejemplo02, con la siguiente
información de prueba

-- ejemplo01
|id |nombre|apellido|
|---|------|--------|
|1  |Prueba|Prueba  |
|2  |Prueba|Prueba  |
|3  |Prueba|Prueba  |


-- ejemplo02
|id |nombre|apellido|
|---|------|--------|
|1  |Prueba|        |
|2  |      |Prueba  |
|3  |Prueba|        |


-----------------------------------------
*/


/*
-----------------------------------------
--     LLAMAR A LOS PROCEDIMIENTOS     --
-----------------------------------------

Al igual que cualquier SP en My SQL se utiliza CALL.

-----------------------------------------
*/

CALL obtener_porcentaje_nulos_Xtabla('ejemplo01')
/*
|columna |total_registros|nulos|porcentaje_nulos|
|--------|---------------|-----|----------------|
|id      |3              |0    |0               |
|nombre  |3              |0    |0               |
|apellido|3              |0    |0               |

La tabla ejemplo01 no tiene ningun dato null, por lo que su porcentaje de null es 0

*/


CALL obtener_porcentaje_nulos_Xtabla('ejemplo02')
/*
|columna |total_registros|nulos|porcentaje_nulos|
|--------|---------------|-----|----------------|
|id      |3              |0    |0               |
|nombre  |2              |1    |33,33           |
|apellido|1              |2    |66,67           |

En esta tabla, las columnas poseen valores nulos, por lo que veremos que nombre tiene el 33.33% de valores
nulos, y en el caso de apellido el 66.67% de los registros es nulo. 

*/

CALL obtener_porcentaje_nulos();
/*
|tabla    |columna |total_registros|nulos|porcentaje_nulos|
|---------|--------|---------------|-----|----------------|
|ejemplo01|id      |3              |0    |0               |
|ejemplo01|nombre  |3              |0    |0               |
|ejemplo01|apellido|3              |0    |0               |
|ejemplo02|id      |3              |0    |0               |
|ejemplo02|nombre  |3              |1    |33,33           |
|ejemplo02|apellido|3              |2    |66,67           |

El procedimiento generara todas las columnas de la base de datos indicando la informacion de la nulidad.
*/