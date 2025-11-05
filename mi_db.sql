DROP DATABASE IF EXISTS mi_proyecto_f;
CREATE DATABASE mi_proyecto_f CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE mi_proyecto_f;

-- Tabla grupos (ya existe en el proyecto, se referencia aquí para la relación)
-- Asumiendo que la tabla grupos ya existe con al menos id_grupo como PK

-- Tabla historico
DROP TABLE IF EXISTS historico;
CREATE TABLE historico(
    id_historico INT AUTO_INCREMENT PRIMARY KEY,
    id_grupo INT NOT NULL,
    num_aprendices_inscritos SMALLINT,
    num_aprendices_en_transito SMALLINT,
    num_aprendices_formacion SMALLINT,
    num_aprendices_induccion SMALLINT,
    num_aprendices_condicionados SMALLINT,
    num_aprendices_aplazados SMALLINT,
    num_aprendices_retirado_voluntario SMALLINT,
    num_aprendices_cancelados SMALLINT,
    num_aprendices_reprobados SMALLINT,
    num_aprendices_no_aptos SMALLINT,
    num_aprendices_reingresados SMALLINT,
    num_aprendices_por_certificar SMALLINT,
    num_aprendices_certificados SMALLINT,
    num_aprendices_trasladados SMALLINT,
    FOREIGN KEY (id_grupo) REFERENCES grupos(id_grupo)
);


-- Tabla estado_de_normas
DROP TABLE IF EXISTS estado_de_normas;
CREATE TABLE estado_de_normas(
    cod_programa MEDIUMINT AUTO_INCREMENT PRIMARY KEY,
    cod_version VARCHAR(50),
    fecha_elaboracion DATE,
    anio SMALLINT,
    red_conocimiento VARCHAR(50),
    nombre_ncl VARCHAR(255),
    cod_ncl INT,
    ncl_version SMALLINT,
    norma_corte_noviembre VARCHAR(255),
    verion INT,
    norma_version VARCHAR(255),
    mesa_sectorial VARCHAR(255),
    tipo_norma VARCHAR(255),
    observacion VARCHAR(255),
    fecha_revision DATE,
    tipo_competencia VARCHAR(50),
    vigencia VARCHAR(20),
    fecha_indice VARCHAR(50)
);


