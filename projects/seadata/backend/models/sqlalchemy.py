""" CUSTOM Models for the relational database """

from restapi.connectors.sqlalchemy.models import db
from sqlalchemy.dialects.postgresql import JSONB


class DataObject(db.Model):  # type: ignore
    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(46), unique=True, nullable=False)
    path = db.Column(db.String(100), index=True, nullable=False)
    object_metadata = db.Column(JSONB, nullable=True)

    def __repr__(self):
        return f"<DataObject(uuid='{self.uid}', path='{self.path}')"
