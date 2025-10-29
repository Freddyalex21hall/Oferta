DROP DATABASE IF EXISTS mi_proyecto_f;
CREATE DATABASE mi_proyecto_f CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE mi_proyecto_f;

CREATE TABLE rol(
    id_rol SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_rol VARCHAR(20)
);

DROP TABLE IF EXISTS usuario;
CREATE TABLE usuario(
    id_usuario INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_completo VARCHAR(90),
    num_documento CHAR(15),
    correo VARCHAR(100) UNIQUE,
    contra_encript VARCHAR(140),
    id_rol SMALLINT UNSIGNED,
    estado BOOLEAN,  -- True = 1 Activo   False = 0 Inactivo
    FOREIGN KEY (id_rol) REFERENCES rol(id_rol)
);

INSERT INTO rol (nombre_rol) VALUES 
('Administrador'),
('Editor'),
('Usuario');
