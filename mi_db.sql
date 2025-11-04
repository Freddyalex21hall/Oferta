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
