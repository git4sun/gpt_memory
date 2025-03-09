from sqlalchemy import create_engine, Column, Integer, String, Text, LargeBinary, DateTime, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pickle
from datetime import datetime

Base = declarative_base()

class Mem(Base):
    __tablename__ = 'mem'
    id = Column(Integer, primary_key=True)
    text = Column(Text)
    role = Column(Text)
    ts = Column(DateTime)
    categories = Column(Text)
    labels = Column(Text)
    embedding = Column(LargeBinary)
    continued = Column(Integer)
    level1 = Column(Text)
    level2 = Column(Text)
    level3 = Column(Text)
    user_id = Column(Text)

class Log(Base):
    __tablename__ = 'log'
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime)
    decay_weights = Column(LargeBinary)
    feedback = Column(Text)
    inputs = Column(LargeBinary)
    outputs = Column(LargeBinary)

class DB:
    def __init__(self, db_url='sqlite:///memory.db'):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.setup_database()

    def setup_database(self):
        """Create tables if they don't exist."""
        Base.metadata.create_all(self.engine)

    def insert_mem(self, data):
        """Insert a new record into the 'mem' table."""
        session = self.Session()
        data['embedding'] = pickle.dumps(data['embedding']) if 'embedding' in data and data['embedding'] is not None else None
        mem = Mem(**data)
        session.add(mem)
        session.commit()
        session.refresh(mem)
        session.close()
        return mem.id

    def read_mems(self, user_id=0, limit=100)->dict[int, dict]:
        """Retrieve message history for a given user, returned as a dictionary keyed by message ID."""
        session = self.Session()
        mems = session.query(Mem).filter_by(user_id=user_id).order_by(Mem.ts.desc()).limit(limit).all()
        session.close()
        return {mem.id: self._row_to_dict(mem) for mem in mems}
    
    def show_mems(self, user_id=0, limit=100):
        """Retrieve message history for a given user."""
        session = self.Session()
        mems = session.query(Mem.id, Mem.text, Mem.role, Mem.ts, Mem.categories, Mem.labels, Mem.continued, Mem.level1, Mem.level2, Mem.level3, Mem.user_id).filter_by(user_id=user_id).order_by(Mem.ts.desc()).limit(limit).all()
        session.close()
        return mems

    def delete_mem_by_ids(self, ids):
        """Delete records from the 'mem' table given a list of IDs."""
        session = self.Session()
        session.query(Mem).filter(Mem.id.in_(ids)).delete(synchronize_session=False)
        session.commit()
        session.close()

    def insert_log(self, ts, decay_weights, feedback, inputs, outputs):
        """Insert a new log record into the 'log' table."""
        session = self.Session()
        log = Log(
            ts=ts,
            decay_weights=pickle.dumps(decay_weights),
            feedback=feedback,
            inputs=pickle.dumps(inputs),
            outputs=pickle.dumps(outputs)
        )
        session.add(log)
        session.commit()
        session.refresh(log)
        session.close()
        return log.id

    def update_log(self, log_id, **kwargs):
        """Update information for a given log entry in the 'log' table."""
        session = self.Session()
        if 'decay_weights' in kwargs:
            kwargs['decay_weights'] = pickle.dumps(kwargs['decay_weights'])
        if 'inputs' in kwargs:
            kwargs['inputs'] = pickle.dumps(kwargs['inputs'])
        if 'outputs' in kwargs:
            kwargs['outputs'] = pickle.dumps(kwargs['outputs'])
        session.query(Log).filter_by(id=log_id).update(kwargs)
        session.commit()
        session.close()

    def update_message_info(self, message_id, **kwargs):
        """Update information for a given message."""
        session = self.Session()
        session.query(Mem).filter(Mem.id == message_id).update(kwargs)
        session.commit()
        session.close()

    def get_message_text_by_id(self, message_id):
        """
        Retrieve the text of a message by its ID.

        Parameters:
        - message_id: ID of the message to retrieve text for.
        """
        session = self.Session()
        result = session.query(Mem.text).filter(Mem.id == message_id).scalar()
        session.close()
        return result

    def get_abstract(self, level, message_id):
        """
        Retrieve the abstract of a message by its ID and level.

        Parameters:
        - level: The abstraction level (1, 2, or 3).
        - message_id: ID of the message to retrieve abstract for.
        """
        session = self.Session()
        column = getattr(Mem, f'level{level}')
        result = session.query(column).filter(Mem.id == message_id).scalar()
        session.close()
        return result

    def update_abstract(self, message_id, abstraction_text, level):
        """
        Update the database with the generated abstraction text.

        Parameters:
        - message_id: ID of the message to update.
        - abstraction_text: The abstraction text to be set.
        - level: The abstraction level (1, 2, or 3).
        """
        session = self.Session()
        column = getattr(Mem, f'level{level}')
        session.query(Mem).filter(Mem.id == message_id).update({column: abstraction_text})
        session.commit()
        session.close()

    def _row_to_dict(self, row):
        """Convert a SQLAlchemy row to a dictionary."""
        return {
            'id': row.id,
            'text': row.text,
            'role': row.role,
            'ts': row.ts,
            'categories': row.categories,
            'labels': row.labels,
            'embedding': pickle.loads(row.embedding) if row.embedding else None,
            'continued': row.continued,
            'level1': row.level1,
            'level2': row.level2,
            'level3': row.level3,
            'user_id': row.user_id
        }
