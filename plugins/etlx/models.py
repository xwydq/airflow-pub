# coding: utf8 
from __future__ import absolute_import, unicode_literals

from datetime import datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, BigInteger, Float)


Base = declarative_base()

class IndxInfo(Base):
    __tablename__ = "indx_info"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    owner = Column(String(128), nullable=False)
    ind_name = Column(String(256), nullable=False)
    formula = Column(Text, nullable=False)
    ind_metric = Column(Text, default='{}', nullable=False)
    ind_dim = Column(Text, default='{}')
    is_delete = Column(Boolean, default=0)
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now,
                         onupdate=datetime.now)
    uuid = Column(String(256), nullable=False)

    def fmt_date(self, d):
        return d.strftime('%Y-%m-%d %H:%M:%S')

    def to_json(self):
        return {'uuid': self.uuid or '',
                'owner': self.owner or 'admin',
                'ind_name': self.ind_name or '',
                'formula': self.formula or '',
                'ind_metric': self.ind_metric or '',
                'ind_dim': self.ind_dim or '',
                'create_time': str(self.create_time or ''),
                'update_time': str(self.update_time or '')}

