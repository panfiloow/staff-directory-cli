import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from models import Base
from config import config

class DatabaseManager:
    """
    Основной класс для работы с базой данных.
    Реализует подключение, создание таблиц и базовые операции.
    """
    
    def __init__(self):
        """Инициализирует подключение к базе данных"""
        self.connection_string = config.get_connection_string()
        self.engine = sa.create_engine(self.connection_string)
        self.Session = sessionmaker(bind=self.engine)
        print(f"Connected to database: {self.connection_string}")
    
    def create_tables(self):
        """
        Создает таблицы в базе данных.
        Режим 1: Создание таблицы сотрудников
        """
        try:
            Base.metadata.create_all(self.engine)
            print("   Table 'employees' created successfully!")
            print("   Columns: id, full_name, birth_date, gender")
            print("   Unique constraint: full_name + birth_date")
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise
    
    def test_connection(self):
        """Проверяет подключение к базе данных"""
        try:
            with self.engine.connect() as conn:
                print("Database connection test: SUCCESS")
            return True
        except Exception as e:
            print(f"Database connection test: FAILED - {e}")
            return False