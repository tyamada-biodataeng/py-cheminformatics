from pathlib import Path

from dotenv import dotenv_values
from sqlalchemy import create_engine, engine, URL
from sqlalchemy.orm import Session, sessionmaker

DB_DIR = Path(__file__).resolve().parent


def get_engine(dotenv_path: str = DB_DIR / ".env") -> engine.Engine:
    """
    データベース接続のためのEngineを作成する。

    Parameters
    ----------
    dotenv_path : str, default DB_DIR / ".env"
        .envファイルのパス
    
    Returns
    -------
    sqlalchemy.engine.Engine
        データベース接続のためのEngine
    """
    config = dotenv_values(dotenv_path)
    url_object = URL.create(
        "postgresql+psycopg",
        username=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],  # plain (unescaped) text
        host=config.get("POSTGRES_HOST", "postgres"),
        port=config.get("POSTGRES_PORT", "5432"),
        database=config.get("POSTGRES_DB", "postgres"),
        query={"options": f"-c search_path={config.get('POSTGRES_SCHEMA', 'public')}"},
    )
    return create_engine(url_object)


def get_session(dotenv_path: str = DB_DIR / ".env") -> Session:
    """
    データベース接続のためのSessionを作成する。

    Parameters
    ----------
    dotenv_path : str, default DB_DIR / ".env"
        .envファイルのパス
    
    Returns
    -------
    sqlalchemy.orm.Session
        データベース接続のためのSession
    """
    engine = get_engine(dotenv_path)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()
