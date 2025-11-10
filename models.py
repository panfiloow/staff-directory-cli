from datetime import date, datetime
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
    
    def validate(self):
        """Валидация данных сотрудника"""
        errors = []
        
        # Проверка ФИО
        if not self.full_name or len(self.full_name.strip()) == 0:
            errors.append("Full name cannot be empty")
        elif len(self.full_name) > 200:
            errors.append("Full name too long (max 200 characters)")
        
        # Проверка даты рождения
        if not self.birth_date:
            errors.append("Birth date is required")
        else:
            if self.birth_date > date.today():
                errors.append("Birth date cannot be in the future")
            
            # Проверяем что сотруднику не больше 150 лет
            min_birth_date = date.today().replace(year=date.today().year - 150)
            if self.birth_date < min_birth_date:
                errors.append("Birth date seems too far in the past")
        
        # Проверка пола
        valid_genders = ["Male", "Female"]
        if self.gender not in valid_genders:
            errors.append(f"Gender must be one of: {', '.join(valid_genders)}")
        
        if errors:
            raise ValueError("; ".join(errors))
        
    @classmethod
    def from_command_line(cls, full_name: str, birth_date_str: str, gender: str):
        """Создает объект Employee из аргументов командной строки"""
        try:
            birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d').date()
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")
        
        employee = cls(
            full_name=full_name,
            birth_date=birth_date,
            gender=gender
        )
        
        employee.validate()
        
        return employee