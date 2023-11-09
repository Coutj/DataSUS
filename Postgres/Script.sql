CREATE TABLE IF NOT EXISTS datasus (
    UFARQUIVO CHAR(2) NOT NULL, 
    ANOARQUIVO SMALLINT NOT NULL,
    TIPOBITO VARCHAR(9) NOT NULL,
    DTHORAOBITO TIMESTAMP,
    "NATURAL" SMALLINT,
    UFNATU CHAR(2),
    CODMUNNATU INTEGER,
    NOMEMUNNATU VARCHAR(255),
    DTNASC DATE,
    IDADE SMALLINT,
    SEXO VARCHAR(8) NOT NULL,
    RACACOR VARCHAR(25),
    ESTCIV VARCHAR(25),
    ESC2010 VARCHAR(25),
    OCUP INTEGER,
    OCUPTITULO VARCHAR(255),
    UFRES CHAR(2),
    CODMUNRES CHAR(6),
    NOMEMUNRES VARCHAR(255),
    LOCOCOR VARCHAR(50),
    UFMUNOCOR CHAR(2),
    CODMUNOCOR CHAR(6),
    NOMEMUNOCOR VARCHAR(255),
    ASSISTMED VARCHAR(8),
    NECROPSIA VARCHAR(8),
    CAUSABAS VARCHAR(4),
    CAUSABASDESCCAT VARCHAR(510),
    CAUSABASDESCSUBCAT VARCHAR(510),
    CIRCOBITO VARCHAR(10),
    ACIDTRAB VARCHAR(8),
    TPPOS VARCHAR(8),
    CONTADOR INTEGER,
    ID VARCHAR(256) PRIMARY KEY
);

--alter table datasus add primary key (ID);

select * from datasus
--where ufarquivo <> 'RJ'

select count(*) from datasus;
drop table datasus;
