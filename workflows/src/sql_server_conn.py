from contextlib import contextmanager
from sqlalchemy import create_engine, text, URL, orm
from dotenv import load_dotenv
import os


load_dotenv()

CONNECTION_URL = URL.create(
    drivername="mssql+pyodbc",
    username=os.getenv('USERNAME'),
    password=os.getenv('PASSWORD'),
    host=os.getenv('SERVER'),
    database=os.getenv('DATABASE'),
    query={
        "driver": os.getenv("DRIVER"),
        "TrustServerCertificate": "yes",
        "Encrypt": "no",
    }
)

def get_engine(echo=True):
    engine = create_engine(CONNECTION_URL, echo=echo, pool_pre_ping=True)
    return engine

engine  = get_engine()

SessionLocal = orm.sessionmaker(bind=engine, autocommit=False, autoflush=False)

class Base(orm.DeclarativeBase):
    """Classe base para todos os modelos ORM"""
    pass

@contextmanager
def get_session():
    """Gerador de sessão - Usar com 'with get_session() as session'."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()

def test_connection() -> bool:
    """Verifica se a conexão com o banco está ativa"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION;"))
            version = result.scalar()
            print(f"✅ Conexão bem sucedida\n{version}")
            return True
    except Exception as e:
        print(f"❌ Falha na conexão: {e}")
        return False


if __name__=="__main__":
    test_connection()

