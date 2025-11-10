from datetime import date
from sqlalchemy.exc import IntegrityError
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from models import Base, Employee, EmployeeDB
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
            print("Table 'employees' created successfully!")
            print("   Columns: id, full_name, birth_date, gender")
            print("   Unique constraint: full_name + birth_date")
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise
    
    def test_connection(self):
        """Проверяет подключение к базе данных"""
        try:
            with self.engine.connect():
                print("Database connection test: SUCCESS")
            return True
        except Exception as e:
            print(f"Database connection test: FAILED - {e}")
            return False
    
    def create_employee(self, employee: Employee) -> int:
        """
        Создает запись сотрудника в базе данных.
        Режим 2: Создание сотрудника
        """
        with self.Session() as session:
            try:
                if self.employee_exists(employee.full_name, employee.birth_date):
                    raise ValueError(f"Employee '{employee.full_name}' ({employee.birth_date}) already exists")
                
                db_employee = EmployeeDB(**employee.to_dict())
                session.add(db_employee)
                session.commit()
                
                self._print_employee_created(db_employee, employee)
                return db_employee.id
                
            except (ValueError, IntegrityError) as e:
                session.rollback()
                if isinstance(e, IntegrityError) and self._is_unique_constraint_error(e):
                    raise ValueError(f"Employee '{employee.full_name}' ({employee.birth_date}) already exists (database constraint)")
                raise e
                
            except Exception as e:
                session.rollback()
                raise Exception(f"Failed to create employee: {e}")
    
    def employee_exists(self, full_name: str, birth_date: date) -> bool:
        """
        Проверяет, существует ли сотрудник с указанным ФИО и датой рождения
        """
        with self.Session() as session:
            existing = session.query(EmployeeDB).filter(
                EmployeeDB.full_name == full_name,
                EmployeeDB.birth_date == birth_date
            ).first()
            return existing is not None
    
    def _is_unique_constraint_error(self, error: IntegrityError) -> bool:
        """Проверяет, является ли ошибка нарушением уникальности"""
        error_str = str(error).lower()
        return any(keyword in error_str for keyword in ['unique_employee', 'unique', 'duplicate'])
    
    def _print_employee_created(self, db_employee: EmployeeDB, employee: Employee):
        """Вспомогательный метод для вывода информации о созданном сотруднике"""
        print("Employee created successfully!")
        print(f"   ID: {db_employee.id}")
        print(f"   Name: {db_employee.full_name}")
        print(f"   Birth Date: {db_employee.birth_date}")
        print(f"   Gender: {db_employee.gender}")
        print(f"   Age: {employee.calculate_age()} years")