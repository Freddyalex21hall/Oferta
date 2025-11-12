DROP DATABASE IF EXISTS mi_proyecto_f;
CREATE DATABASE mi_proyecto_f CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE mi_proyecto_f;

CREATE TABLE rol(
    id_rol SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_rol VARCHAR(20)
);

CREATE TABLE usuario(
    id_usuario INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_completo VARCHAR(80),
    num_documento CHAR(12),
    correo VARCHAR(100) UNIQUE,
    contra_encript VARCHAR(140),
    id_rol SMALLINT UNSIGNED,
    estado BOOLEAN,  -- True = 1 Activo   False = 0 Inactivo
    FOREIGN KEY (id_rol) REFERENCES rol(id_rol)
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
	`cod_municipio` CHAR(5) NOT NULL UNIQUE,
	`nombre` VARCHAR(80),
	PRIMARY KEY(`cod_municipio`)
);


CREATE TABLE IF NOT EXISTS `estrategia` (
	`cod_estrategia` CHAR(5) NOT NULL UNIQUE,
	`nombre` VARCHAR(80),
	PRIMARY KEY(`cod_estrategia`)
);

CREATE TABLE IF NOT EXISTS `programas_formacion` (
	`cod_programa` MEDIUMINT UNSIGNED NOT NULL UNIQUE,
	`version` CHAR(4),
	`nombre` VARCHAR(200),
	`nivel` VARCHAR(70),
	`id_red` INTEGER UNSIGNED,
	`tiempo_duracion` SMALLINT UNSIGNED,
	`unidad_medida` VARCHAR(50),
	`estado` BOOLEAN,
	`url_pdf` VARCHAR(180),
	PRIMARY KEY(`cod_programa`)
);


CREATE TABLE IF NOT EXISTS `grupos` (
	`ficha` INTEGER UNSIGNED NOT NULL UNIQUE,
	`cod_programa` MEDIUMINT UNSIGNED,
	`cod_centro` SMALLINT UNSIGNED,
	`modalidad` VARCHAR(80),
	`jornada` VARCHAR(80),
	`etapa_ficha` VARCHAR(80),
	`estado_curso` VARCHAR(80),
	`fecha_inicio` DATE,
	`fecha_fin` DATE,
	`cod_municipio` CHAR(5),
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
