from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker, declarative_base
import logging

# Configuración de logs
logging.basicConfig(filename='orm.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Configuración de conexión a SQL Server usando pymssql
SERVER = "DESKTOP-FLUCE56"
DATABASE = "Testing"
USERNAME = "Usuario"
PASSWORD = ""

# Conexión usando pymssql
CONN_STR = f"mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
engine = create_engine(CONN_STR)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# Definición de la tabla de staging
class StagingTable(Base):
    __tablename__ = "staging_table"
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(255))
    edad = Column(Integer)

# Crear las tablas si no existen
Base.metadata.create_all(engine)

def log_event(message):
    """Registra eventos en el log."""
    logging.info(message)
    print(message)

def truncate_table(table_name):
    """Trunca la tabla indicada en la base de datos."""
    try:
        session.execute(text(f"TRUNCATE TABLE {table_name}"))
        session.commit()
        log_event(f"Tabla {table_name} truncada correctamente.")
    except Exception as e:
        session.rollback()
        log_event(f"Error truncando tabla {table_name}: {str(e)}")

def execute_stored_procedure(proc_name, params=None):
    """Ejecuta un procedimiento almacenado en SQL Server."""
    try:
        if params:
            query = f"EXEC {proc_name} {', '.join(['%s' for _ in params])}"
            session.execute(text(query), params)
        else:
            session.execute(text(f"EXEC {proc_name}"))
        session.commit()
        log_event(f"Procedimiento {proc_name} ejecutado correctamente.")
    except Exception as e:
        session.rollback()
        log_event(f"Error ejecutando {proc_name}: {str(e)}")
