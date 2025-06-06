from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

connection_url = URL.create(
    "mssql+pyodbc",
    username="sa",
    password="Tan1gut19",
    host="TKPC2933\\SQLEXPRESS",
    #host="K-DP2404\\SQLEXPRESS",
    port="1433",
    database="MonthlyReport",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "autocommit": "True",
        "LongAsMax": "Yes",
    },
)
engine = create_engine(
    connection_url,
    echo=False,  # ログ出力を抑える
    connect_args={"TrustServerCertificate": "yes"},
    pool_pre_ping=True,
    pool_size=10,  # 最大接続数
    max_overflow=20,  # ピーク時の最大増加接続数
    pool_recycle=3600,  # 1時間ごとに接続をリサイクル
    pool_timeout=30,  # 接続取得のタイムアウトを30秒
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()