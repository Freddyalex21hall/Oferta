DROP DATABASE IF EXISTS railway;
CREATE DATABASE railway CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE railway;

CREATE TABLE IF NOT EXISTS `rol` (
    `id_rol` SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    `nombre_rol` VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS `usuario` (
    `id_usuario` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    `nombre_completo` VARCHAR(80),
    `num_documento` CHAR(12),
    `correo` VARCHAR(100) UNIQUE,
    `contra_encript` VARCHAR(140),
    `id_rol` SMALLINT UNSIGNED,
    `estado` BOOLEAN,  -- True = 1 Activo   False = 0 Inactivo
    FOREIGN KEY (`id_rol`) REFERENCES rol(`id_rol`)
);

--   dESDE AQUÍ

CREATE TABLE IF NOT EXISTS `centros_formacion` (
	`cod_centro` SMALLINT UNSIGNED NOT NULL UNIQUE,
	`nombre_centro` VARCHAR(160),
	`cod_regional` TINYINT UNSIGNED,
	`nombre_regional` VARCHAR(80),
	PRIMARY KEY(`cod_centro`)
);

CREATE TABLE IF NOT EXISTS `municipios` (
	`cod_municipio` CHAR(10) NOT NULL UNIQUE,
	`nombre` VARCHAR(80),
	PRIMARY KEY(`cod_municipio`)
);


CREATE TABLE IF NOT EXISTS `estrategia` (
	`cod_estrategia` CHAR(5) NOT NULL UNIQUE,
	`nombre` VARCHAR(80),
	PRIMARY KEY(`cod_estrategia`)
);

CREATE TABLE IF NOT EXISTS `programas_formacion` (
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
    `indice` VARCHAR(255),                    
    `ocupacion` VARCHAR(60),                   -- Ocupación
    `estado` BOOLEAN,
    `url_pdf` VARCHAR(250),
    INDEX idx_nivel_modalidad (`nivel_formacion`, `modalidad`)
);



CREATE TABLE IF NOT EXISTS `grupos` (
	`ficha` INTEGER UNSIGNED NOT NULL UNIQUE,
	`cod_programa` VARCHAR(16),
	`cod_centro` SMALLINT UNSIGNED,
	`modalidad` VARCHAR(80),
	`jornada` VARCHAR(80),
	`etapa_ficha` VARCHAR(80),
	`estado_curso` VARCHAR(80),
	`fecha_inicio` DATE,
	`fecha_fin` DATE,
	`cod_municipio` CHAR(10),
	`cod_estrategia` CHAR(5),
	`nombre_responsable` VARCHAR(150),
	`cupo_asignado` SMALLINT UNSIGNED,
	`num_aprendices_fem` SMALLINT UNSIGNED,
	`num_aprendices_mas` SMALLINT UNSIGNED,
	`num_aprendices_nobin` SMALLINT UNSIGNED,
	`num_aprendices_matriculados` SMALLINT UNSIGNED,
	`num_aprendices_activos` SMALLINT UNSIGNED,
	`tipo_doc_empresa` CHAR(5),
	`num_doc_empresa` VARCHAR(30),
	`nombre_empresa` VARCHAR(140),
	PRIMARY KEY(`ficha`),
    FOREIGN KEY(`cod_programa`) REFERENCES `programas_formacion`(`cod_programa`)
    ON UPDATE NO ACTION ON DELETE NO ACTION,
    FOREIGN KEY(`cod_centro`) REFERENCES `centros_formacion`(`cod_centro`)
    ON UPDATE NO ACTION ON DELETE NO ACTION,
    FOREIGN KEY(`cod_municipio`) REFERENCES `municipios`(`cod_municipio`)
    ON UPDATE NO ACTION ON DELETE NO ACTION,
    FOREIGN KEY(`cod_estrategia`) REFERENCES `estrategia`(`cod_estrategia`)
    ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS `historico`(
    `id_historico` INT AUTO_INCREMENT PRIMARY KEY,
    `id_grupo` INTEGER UNSIGNED NOT NULL,  -- Cambiado de INT a INTEGER UNSIGNED
    `num_aprendices_inscritos` SMALLINT,
    `num_aprendices_en_transito` SMALLINT,
    `num_aprendices_formacion` SMALLINT,
    `num_aprendices_induccion` SMALLINT,
    `num_aprendices_condicionados` SMALLINT,
    `num_aprendices_aplazados` SMALLINT,
    `num_aprendices_retirado_voluntario` SMALLINT,
    `num_aprendices_cancelados` SMALLINT,
    `num_aprendices_reprobados` SMALLINT,
    `num_aprendices_no_aptos` SMALLINT,
    `num_aprendices_reingresados` SMALLINT,
    `num_aprendices_por_certificar` SMALLINT,
    `num_aprendices_certificados` SMALLINT,
    `num_aprendices_trasladados` SMALLINT,
    FOREIGN KEY (`id_grupo`) REFERENCES `grupos`(`ficha`)
);

CREATE TABLE IF NOT EXISTS `estado_de_normas` (
    `id_estado_norma` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `cod_programa` INT UNSIGNED NOT NULL,
    `cod_version` VARCHAR(50) NOT NULL,
    `fecha_elaboracion` DATE NULL,
    `anio` SMALLINT NOT NULL,
    `red_conocimiento` VARCHAR(150),
    `nombre_ncl` VARCHAR(150),
    `cod_ncl` INT,
    `ncl_version` SMALLINT,
    `norma_corte_noviembre` VARCHAR(150),
    `version` INT,
    `norma_version` VARCHAR(100),
    `mesa_sectorial` VARCHAR(150),
    `tipo_norma` VARCHAR(80),
    `observacion` VARCHAR(255),
    `fecha_revision` DATE,
    `tipo_competencia` VARCHAR(80),
    `vigencia` VARCHAR(80),
    `fecha_indice` VARCHAR(80),
    CONSTRAINT fk_programa_norma FOREIGN KEY (`cod_programa`)
        REFERENCES `programas_formacion`(`cod_programa`)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `registro_calificado` (
    `cod_programa` VARCHAR(16) NOT NULL,
    `tipo_tramite` VARCHAR(50),
    `fecha_radicado` DATE,
    `numero_resolucion` MEDIUMINT,
    `fecha_resolucion` DATE,
    `fecha_vencimiento` DATE,
    `vigencia` VARCHAR(25),
    `modalidad` VARCHAR(25),
    `clasificacion` VARCHAR(15),
    `estado_catalogo` VARCHAR(50),
    PRIMARY KEY (`cod_programa`),
    FOREIGN KEY (`cod_programa`)
        REFERENCES `programas_formacion`(`cod_programa`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

