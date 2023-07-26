from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:123@localhost:5432/calliper')
