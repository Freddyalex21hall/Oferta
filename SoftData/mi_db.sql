DROP DATABASE IF EXISTS mi_proyecto_f;
CREATE DATABASE mi_proyecto_f CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE mi_proyecto_f;

CREATE TABLE rol(
    id_rol SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_rol VARCHAR(20)
);

CREATE TABLE catalogo_programas(
    id_programa  INT NOT NULL AUTO_INCREMENT,
    cod_programa  MEDIUMINT,
    ver_sion  VARCHAR(255),
    nombre  VARCHAR(255),
    tipo_formacion  VARCHAR(70),
    nivel_formacion  VARCHAR(70),
    duracion_maxima  VARCHAR(50),
    fecha_registro  DATE,
    linea_tecnologica  VARCHAR(100),
    red_tecnologica  VARCHAR(255),
    modalidad  VARCHAR(100),
    ocupacion  VARCHAR(100),
    duracion_lectiva  DATE,
    descripcion  VARCHAR(255),
    resolucion  VARCHAR(100),
    fecha_resolucion  DATE,
    tipo_permiso  CHAR(50),
    PRIMARY KEY (id_programa)
);
 


DROP TABLE centro;
CREATE TABLE centro(
    cod_centro SMALLINT UNSIGNED PRIMARY KEY,
    nombre_centro VARCHAR(80)
);
