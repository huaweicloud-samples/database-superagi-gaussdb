from sqlalchemy import create_engine
from superagi.config.config import get_config
from superagi.helper.db_connection_helper import build_database_url, register_gaussdb_compat
from superagi.lib.logger import logger

engine = None


def connect_db():
    """
    Connects to the database using SQLAlchemy (GaussDB 507 compatible).

    Returns:
        engine: The SQLAlchemy engine object representing the database connection.
    """

    global engine
    if engine is not None:
        return engine

    db_url = build_database_url()
    engine = create_engine(db_url,
                           pool_size=20,  # Maximum number of database connections in the pool
                           max_overflow=50,  # Maximum number of connections that can be created beyond the pool_size
                           pool_timeout=30,  # Timeout value in seconds for acquiring a connection from the pool
                           pool_recycle=1800,  # Recycle connections after this number of seconds (optional)
                           pool_pre_ping=False,  # Enable connection health checks (optional)
                           )
    register_gaussdb_compat(engine)

    # Test the connection
    try:
        connection = engine.connect()
        logger.info("Connected to the database! @ " + db_url)
        connection.close()
    except Exception as e:
        logger.error(f"Unable to connect to the database:{e}")
    return engine
