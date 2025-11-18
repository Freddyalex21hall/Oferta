DROP DATABASE IF EXISTS mi_proyecto_f;
CREATE DATABASE mi_proyecto_f CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE mi_proyecto_f;

CREATE TABLE rol(
    id_rol SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    nombre_rol VARCHAR(20)
);

CREATE TABLE programas_formacion (
  cod_programa MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  version VARCHAR(20),
  nombre VARCHAR(255) NOT NULL,
  nivel VARCHAR(100),
  meses_duracion SMALLINT,
  duracion_programa SMALLINT,
  unidad_medida VARCHAR(50),
  estado BOOLEAN DEFAULT TRUE,
  tipo_programa VARCHAR(100),
  url_pdf VARCHAR(255),
  red_conocimiento VARCHAR(150),
  programa_especial MEDIUMINT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla 'grupo' que relaciona fichas/grupos con programas_formacion (n:1 -> cod_programa)
CREATE TABLE grupo (
  ficha integer NOT NULL PRIMARY KEY,
  cod_centro INT NOT NULL,
  cod_programa MEDIUMINT UNSIGNED NOT NULL,
  la_version VARCHAR(50),
  estado_grupo VARCHAR(100),
  nombre_nivel VARCHAR(100),
  jornada VARCHAR(100),
  fecha_inicio DATE,
  fecha_fin DATE,
  etapa VARCHAR(100),
  modalidad VARCHAR(100),
  responsable VARCHAR(255),
  nombre_empresa VARCHAR(255),
  nombre_municipio VARCHAR(150),
  nombre_programa_especial VARCHAR(255),
  hora_inicio TIME,
  hora_fin TIME,
  aula_actual VARCHAR(100),
  CONSTRAINT fk_grupo_programa FOREIGN KEY (cod_programa)
    REFERENCES programas_formacion(cod_programa)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE estado_de_normas (
  id_estado_norma MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cod_programa MEDIUMINT UNSIGNED NOT NULL,
  cod_version VARCHAR(50) NOT NULL,
  fecha_elaboracion DATE NOT NULL,
  anio SMALLINT NOT NULL,
  red_conocimiento VARCHAR(150),
  nombre_ncl VARCHAR(150),
  cod_ncl INT,
  ncl_version SMALLINT,
  norma_corte_noviembre VARCHAR(150),
  version INT,
  norma_version VARCHAR(100),
  mesa_sectorial VARCHAR(150),
  tipo_norma VARCHAR(80),
  observacion VARCHAR(255),
  fecha_revision DATE,
  tipo_competencia VARCHAR(80),
  vigencia VARCHAR(80),
  fecha_indice VARCHAR(80),
  CONSTRAINT fk_programa_norma FOREIGN KEY (cod_programa)
    REFERENCES programas_formacion(cod_programa)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


Table grupos{
	ficha integer PRIMARY KEY integer unique
	cod_programa mediumint
	cod_centro smallint
	modalidad varchar
	jornada varchar
	etapa_ficha varchar
	estado_curso varchar
	fecha_inicio date
	fecha_fin date
	cod_municipio char
	nombre_responsable varchar
	cupo_asignado smallint
	num_aprendices_fem smallint
	num_aprendices_mas smallint
	num_aprendices_nobin smallint
	num_aprendices_matriculados smallint
	num_aprendices_activos smallint
	id_historico integer
	tipo_doc_empresa char
	num_doc_empresa varchar
	nombre_empresa varchar
	nombre_estrategia varchar
}

INSERT INTO rol (nombre_rol) VALUES 
('Administrador'),
('Editor'),
('Usuario');
