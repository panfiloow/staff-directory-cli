from datetime import date
from dataclasses import dataclass
from typing import Optional
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class EmployeeDB(Base):
    """Модель сотрудника для базы данных"""
    
    __tablename__ = 'employees'
    
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    full_name: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    birth_date: Mapped[date] = mapped_column(sa.Date, nullable=False)  
    gender: Mapped[str] = mapped_column(sa.String(10), nullable=False)

    __table_args__ = (
        sa.UniqueConstraint('full_name', 'birth_date', name='unique_employee'),
    )
    
    def __repr__(self):
        return f"<EmployeeDB(id={self.id}, name='{self.full_name}', birth_date={self.birth_date})>"
    
@dataclass
class Employee:
    """Бизнес-модель сотрудника для работы в приложении"""
    full_name: str
    birth_date: date
    gender: str
    id: Optional[int] = None
    
    def calculate_age(self) -> int:
        """Рассчитывает возраст в полных годах"""
        today = date.today()
        age = today.year - self.birth_date.year
        
        if today.month < self.birth_date.month or \
           (today.month == self.birth_date.month and today.day < self.birth_date.day):
            age -= 1
            
        return age
    
    def to_dict(self):
        """Конвертирует в словарь для БД"""
        return {
            'full_name': self.full_name,
            'birth_date': self.birth_date,
            'gender': self.gender
        }
    
    @classmethod
    def from_db_model(cls, db_employee: EmployeeDB):
        """Создает бизнес-модель из модели БД"""
        return cls(
            id=db_employee.id,
            full_name=db_employee.full_name,
            birth_date=db_employee.birth_date,
            gender=db_employee.gender
        )