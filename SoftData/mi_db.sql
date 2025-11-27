DROP DATABASE IF EXISTS mi_proyecto_f;
CREATE DATABASE mi_proyecto_f CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE mi_proyecto_f;

CREATE TABLE rol(
    id_rol SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_rol VARCHAR(20)
);

-- Hacer carga backend 
-- Programas que pueden ofertarse se carga del CATALOGO DE PROGRAMAS
CREATE TABLE programas_formacion (
    `cod_programa` VARCHAR(16) PRIMARY KEY,        -- PRF_CODIGO
    `cod_version` VARCHAR(20),                    -- cod_VERSION
    `PRF_version` TINYINT UNSIGNED,                    -- PRF_VERSION
    `tipo_formacion` VARCHAR(30),                    -- TIPO DE FORMACION
    `nombre_programa` VARCHAR(255) NOT NULL,          -- PRF_DENOMINACION
    `nivel_formacion` VARCHAR(30),                    -- NIVEL DE FORMACION
    `duracion_maxima` SMALLINT UNSIGNED,                       -- PRF_DURACION_MAXIMA
    `dur_etapa_lectiva` SMALLINT UNSIGNED,                       -- PRF_DUR_ETAPA_LECTIVA
    `dur_etapa_productiva` SMALLINT UNSIGNED,                       -- PRF_DUR_ETAPA_PROD
    `fecha_registro` DATE,                           -- PRF_FCH_REGISTRO
    `fecha_activo` DATE,                           -- Fecha Activo (En Ejecución)
    `edad_min_requerida` CHAR(2),                       -- PRF_EDAD_MIN_REQUERIDA
    `grado_min_requerido` VARCHAR(50),                    -- PRF_GRADO_MIN_REQUERIDO
    `descripcion_req` TEXT,                           -- PRF_DESCRIPCION_REQUISITO
    `resolucion` VARCHAR(250),                    -- PRF_RESOLUCION
    `fecha_resolucion` DATE,                           -- PRF_FECHA_RESOLUCION
    `apoyo_fic` VARCHAR(2),                     -- PRF_APOYO_FIC
    `creditos` TINYINT UNSIGNED,                            -- PRF_CREDITOS
    `alamedida` VARCHAR(2),                     -- PRF_ALAMEDIDA
    `linea_tecnologica` VARCHAR(50),                   -- Linea Tecnológica
    `red_tecnologica`  VARCHAR(80),                   -- Red Tecnológica
    `red_conocimiento` VARCHAR(80),                   -- Red de Conocimiento
    `modalidad` VARCHAR(30),                    -- Modalidad
    `apuestas_prioritarias` VARCHAR(80),                  -- Apuestas Prioritarias
    `fic` VARCHAR(2),                     -- FIC (NO/SI)
    `tipo_permiso` VARCHAR(30),                    -- TIPO PERMISO
    `multiple_inscripcion` VARCHAR(2),                     -- Multiple Inscripcion
    `indice` VARCHAR(20),                    -- Indice
    `ocupacion` VARCHAR(60),                   -- Ocupación
    `estado` BOOLEAN,
    `url_pdf` VARCHAR(250),
    INDEX idx_nivel_modalidad (`nivel_formacion`, `modalidad`)
);
 


DROP TABLE centro;
CREATE TABLE centro(
    cod_centro SMALLINT UNSIGNED PRIMARY KEY,
    nombre_centro VARCHAR(80)
);
