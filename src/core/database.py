import json
from typing import List

from sqlalchemy import create_engine, Column, String, Text, Integer, Float
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session

from src.core.loader import PROJECT_ROOT


DATABASE_PATH = PROJECT_ROOT / 'data' / 'app.db'
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(f'sqlite:///{DATABASE_PATH}', echo=False)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()


class Job(Base):
    __tablename__ = 'jobs'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50), nullable=False)
    account_label = Column(String(100))
    from_date = Column(String(10))
    to_date = Column(String(10))
    status = Column(String(20), nullable=False, default='pending')
    error = Column(Text)
    files_json = Column(Text)
    file_count = Column(Integer, default=0)
    duration_seconds = Column(Float)
    created_at = Column(String(30), nullable=False)
    updated_at = Column(String(30), nullable=False)

    @property
    def files(self) -> List[str]:
        return json.loads(self.files_json) if self.files_json else []

    @files.setter
    def files(self, value: List[str]):
        self.files_json = json.dumps(value) if value else None
        self.file_count = len(value) if value else 0

    def to_dict(self) -> dict:
        result = {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }
        
        if self.account_label:
            result['account_label'] = self.account_label
        if self.from_date:
            result['from_date'] = self.from_date
        if self.to_date:
            result['to_date'] = self.to_date
        if self.error:
            result['error'] = self.error
        if self.files_json:
            result['files'] = self.files
            result['file_count'] = self.file_count
        if self.duration_seconds is not None:
            result['duration_seconds'] = self.duration_seconds
            
        return result


def init_db():
    Base.metadata.create_all(engine)


def get_session():
    return Session()


init_db()

