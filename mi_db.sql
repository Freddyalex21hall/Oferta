DROP DATABASE IF EXISTS mi_proyecto_f;
CREATE DATABASE mi_proyecto_f CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE mi_proyecto_f;

CREATE TABLE rol(
    id_rol SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_rol VARCHAR(20)
);

INSERT INTO rol (nombre_rol) VALUES 
('Administrador'),
('Editor'),
('Usuario');
