from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session

from src.core.loader import PROJECT_ROOT


DATABASE_PATH = PROJECT_ROOT / 'data' / 'app.db'
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    f'sqlite:///{DATABASE_PATH}',
    echo=False,
    connect_args={
        'check_same_thread': False,
        'timeout': 30
    }
)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()


def init_db():
    from src.core import models
    Base.metadata.create_all(engine)


def get_session():
    return Session()


init_db()

