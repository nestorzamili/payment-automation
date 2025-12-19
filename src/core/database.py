import json
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, Column, String, Text, Integer
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
    type = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False, default='pending')
    metadata_json = Column(Text, default='{}')
    result_json = Column(Text)
    error = Column(Text)
    created_at = Column(String(30), nullable=False)
    updated_at = Column(String(30), nullable=False)

    @property
    def job_metadata(self) -> dict:
        return json.loads(self.metadata_json) if self.metadata_json else {}

    @job_metadata.setter
    def job_metadata(self, value: dict):
        self.metadata_json = json.dumps(value)

    @property
    def result(self) -> Any:
        return json.loads(self.result_json) if self.result_json else None

    @result.setter
    def result(self, value: Any):
        self.result_json = json.dumps(value) if value is not None else None

    def to_dict(self) -> dict:
        return {
            'job_id': self.job_id,
            'type': self.type,
            'status': self.status,
            'metadata': self.job_metadata,
            'result': self.result,
            'error': self.error,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }


class Download(Base):
    __tablename__ = 'downloads'

    download_id = Column(Integer, primary_key=True, autoincrement=True)
    account_label = Column(String(100), nullable=False)
    platform = Column(String(50), nullable=False)
    from_date = Column(String(10), nullable=False)
    to_date = Column(String(10), nullable=False)
    file_path = Column(Text, nullable=False)
    file_hash = Column(String(64))
    downloaded_at = Column(String(30), nullable=False)

    def to_dict(self) -> dict:
        return {
            'download_id': self.download_id,
            'account_label': self.account_label,
            'platform': self.platform,
            'from_date': self.from_date,
            'to_date': self.to_date,
            'file_path': self.file_path,
            'file_hash': self.file_hash,
            'downloaded_at': self.downloaded_at,
        }


def init_db():
    Base.metadata.create_all(engine)


def get_session():
    return Session()


init_db()
