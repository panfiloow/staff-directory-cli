from datetime import date, timedelta
import random
import time
from typing import Any, Dict, List
from sqlalchemy.exc import IntegrityError
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from data.middle_names import MIDDLE_NAMES_FEMALE, MIDDLE_NAMES_MALE
from data.names_female import FEMALE_NAMES
from data.names_male import MALE_NAMES
from data.surnames import SURNAMES
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

    
    def get_all_employees_unique_sorted(self) -> List[Dict[str, Any]]:
        """
        Получает всех сотрудников с уникальным ФИО+дата, отсортированных по ФИО.
        Режим 3: Вывод всех уникальных сотрудников
        
        Returns:
            List[Dict]: Список словарей с информацией о сотрудниках
        """
        with self.Session() as session:
            try:
                # Для SQLite используем оконные функции для получения уникальных записей
                if config.db_type == "sqlite":
                    # Создаем подзапрос с номером строки для каждой группы ФИО+дата
                    subquery = session.query(
                        EmployeeDB.id,
                        EmployeeDB.full_name,
                        EmployeeDB.birth_date,
                        EmployeeDB.gender,
                        sa.func.row_number().over(
                            partition_by=[EmployeeDB.full_name, EmployeeDB.birth_date],
                            order_by=EmployeeDB.id
                        ).label('row_num')
                    ).subquery()
                    
                    db_employees = session.query(
                        subquery.c.id,
                        subquery.c.full_name,
                        subquery.c.birth_date,
                        subquery.c.gender
                    ).filter(
                        subquery.c.row_num == 1
                    ).order_by(
                        subquery.c.full_name
                    ).all()
                    
                else:
                    # Для PostgreSQL и других СУБД можно использовать DISTINCT ON
                    db_employees = session.query(EmployeeDB).distinct(
                        EmployeeDB.full_name, 
                        EmployeeDB.birth_date
                    ).order_by(
                        EmployeeDB.full_name, 
                        EmployeeDB.birth_date
                    ).all()
                
                result = []
                for emp in db_employees:
                    employee_obj = Employee.from_db_model(emp) if hasattr(emp, 'id') else Employee(
                        full_name=emp.full_name,
                        birth_date=emp.birth_date,
                        gender=emp.gender,
                        id=emp.id
                    )
                    
                    result.append({
                        'full_name': employee_obj.full_name,
                        'birth_date': employee_obj.birth_date.strftime('%Y-%m-%d'),
                        'gender': employee_obj.gender,
                        'age': employee_obj.calculate_age()
                    })
                
                return result
                
            except Exception as e:
                raise Exception(f"Error fetching unique employees: {e}")
            
    def print_employees_table(self, employees: List[Dict[str, Any]]):
        """
        Форматированный вывод таблицы сотрудников
        
        Args:
            employees: Список сотрудников для вывода
        """
        if not employees:
            print("No employees found in database")
            return
        
        print(f"Found {len(employees)} unique employees:")
        print()
        
        header = f"{'Full Name':<30} {'Birth Date':<12} {'Gender':<8} {'Age':<4}"
        print(header)
        print("-" * 60)
        
        for emp in employees:
            print(f"{emp['full_name']:<30} {emp['birth_date']:<12} {emp['gender']:<8} {emp['age']:<4}")
        
        print("-" * 60)
        print(f"Total: {len(employees)} employees")
    
    def bulk_insert_employees(self, employees: List[Employee], batch_size: int = 1000):
        """
        Пакетная вставка сотрудников в базу данных БЕЗ обработки дубликатов
        Предполагается, что все сотрудники уже уникальны
        """
        if not employees:
            print("No employees to insert")
            return
        
        total_employees = len(employees)
        print(f"Inserting {total_employees:,} employees in batches of {batch_size}...")
        
        start_time = time.time()
        inserted_count = 0
        
        with self.Session() as session:
            try:
                for i in range(0, total_employees, batch_size):
                    batch = employees[i:i + batch_size]
                    db_employees = [EmployeeDB(**emp.to_dict()) for emp in batch]
                    
                    try:
                        session.bulk_save_objects(db_employees)
                        session.commit()
                        inserted_count += len(db_employees)
                        
                    except IntegrityError:
                        session.rollback()
                        successful_in_batch = 0
                        
                        for db_emp in db_employees:
                            try:
                                session.add(db_emp)
                                session.commit()
                                successful_in_batch += 1
                            except IntegrityError:
                                session.rollback()
                                continue  
                        
                        inserted_count += successful_in_batch
                    
                    progress = (i + len(batch)) / total_employees * 100
                    print(f"Progress: {i + len(batch):,}/{total_employees:,} ({progress:.1f}%) - Inserted: {inserted_count:,}")
                
                end_time = time.time()
                total_time = end_time - start_time
                
                print(f"Successfully inserted {inserted_count:,} employees")
                print(f"Total time: {total_time:.2f} seconds")
                if total_time > 0:
                    print(f"Speed: {inserted_count/total_time:.0f} employees/second")
                
            except Exception as e:
                session.rollback()
                raise Exception(f"Error during bulk insert: {e}")
            
    def generate_sample_data(self):
        """
        Генерирует тестовые данные.
        Создаем большую таблицу (1M+ записей) с небольшим процентом целевых записей.
        """
        print("Generating sample data optimized for index demonstration...")
        print("This will create:")
        print("   - 1,000,000 base employees (various genders and last names)")
        print("   - ADDITIONAL 100 male employees with F-surnames") 
        print("   - TOTAL: 1,000,100 employees")
        print("   - Query will find ~100 male employees with 'F' last names")
        print()
        
        all_employees = []
        seen_combinations = set()
        
        total_main = 1000000
        target_male_f_count = 100  
        
        print(f"🔄 Generating {total_main:,} base employees...")
        
        i = 0
        generated_count = 0
        
        while generated_count < total_main:
            if generated_count % 100000 == 0 and generated_count > 0:
                print(f"   Generated {generated_count:,}/{total_main:,} base employees...")
            
            gender = 'Male' if (i % 2 == 0) else 'Female'
            is_male = gender == 'Male'
            
            available_surnames = [(male, female) for male, female in SURNAMES if not male.startswith('F')]
            surname_male, surname_female = random.choice(available_surnames)
            surname = surname_male if is_male else surname_female
            
            if is_male:
                first_name = random.choice(MALE_NAMES)
                middle_name = random.choice(MIDDLE_NAMES_MALE)
            else:
                first_name = random.choice(FEMALE_NAMES)
                middle_name = random.choice(MIDDLE_NAMES_FEMALE)
            
            full_name = f"{surname} {first_name} {middle_name}"
            
            # Генерируем дату рождения (от 18 до 65 лет)
            start_date = date.today().replace(year=date.today().year - 65)
            end_date = date.today().replace(year=date.today().year - 18)
            random_days = random.randint(0, (end_date - start_date).days)
            birth_date = start_date + timedelta(days=random_days)
            
            employee = Employee(full_name, birth_date, gender)
            
            key = (employee.full_name, employee.birth_date)
            if key not in seen_combinations:
                seen_combinations.add(key)
                all_employees.append(employee)
                generated_count += 1
            
            i += 1
            
            if i > total_main * 3:
                print("Breaking generation loop to avoid infinite cycle")
                break
        
        print(f"{len(all_employees):,} base employees generated")
        
        print(f"ADDING {target_male_f_count} TARGET male employees with 'F' last names...")
        
        f_surnames = [(male, female) for male, female in SURNAMES if male.startswith('F')]
        
        if not f_surnames:
            raise Exception("No F-surnames found in the database!")
        
        target_employees = []
        
        for i in range(target_male_f_count):
            surname_male, surname_female = random.choice(f_surnames)
            first_name = random.choice(MALE_NAMES)
            middle_name = random.choice(MIDDLE_NAMES_MALE)
            full_name = f"{surname_male} {first_name} {middle_name}"
            
            start_date = date.today().replace(year=date.today().year - 65)
            end_date = date.today().replace(year=date.today().year - 18)
            random_days = random.randint(0, (end_date - start_date).days)
            birth_date = start_date + timedelta(days=random_days)
            
            employee = Employee(full_name, birth_date, "Male")
            
            key = (employee.full_name, employee.birth_date)
            if key not in seen_combinations:
                seen_combinations.add(key)
                target_employees.append(employee)
            else:
                i -= 1
        
        all_employees.extend(target_employees)
        
        print(f"{len(target_employees)} target male F-surname employees generated")
        print(f"Total employees to insert: {len(all_employees):,}")
        
        print("Shuffling employees for realistic distribution...")
        random.shuffle(all_employees)
        
        print()
        self.bulk_insert_employees(all_employees)
        
        self._print_generation_stats()

    def _print_generation_stats(self):
        """Выводит статистику после генерации данных"""
        with self.Session() as session:
            try:
                total_count = session.query(EmployeeDB).count()
                
                gender_stats = session.query(
                    EmployeeDB.gender,
                    sa.func.count(EmployeeDB.id)
                ).group_by(EmployeeDB.gender).all()
        
                print("\n Calculating first letter distribution...")
                first_letter_stats = session.query(
                    sa.func.substr(EmployeeDB.full_name, 1, 1).label('first_letter'),
                    sa.func.count(EmployeeDB.id).label('count'),
                    sa.func.avg(sa.case((EmployeeDB.gender == 'Male', 1), else_=0)).label('male_ratio')
                ).group_by('first_letter').order_by('first_letter').all()
                
                print()
                print("GENERATION STATISTICS:")
                print(f"   Total employees in database: {total_count:,}")
                print("   Gender distribution:")
                for gender, count in gender_stats:
                    percentage = (count / total_count) * 100
                    print(f"     {gender}: {count:,} ({percentage:.1f}%)")
                
                print("\n   FIRST LETTER DISTRIBUTION:")
                print("   " + "="*50)
                print(f"   {'Letter':<6} {'Count':<10} {'Percentage':<12} {'Male %':<10} {'Note':<10}")
                print("   " + "-"*50)
                
                for letter_stat in first_letter_stats:
                    letter = letter_stat.first_letter
                    count = letter_stat.count
                    percentage = (count / total_count) * 100
                    male_percentage = letter_stat.male_ratio * 100
                    
                    note = ""
                    if letter == 'F':
                        note = "← QUERY TARGET"
                    elif count == 0:
                        note = "NO DATA"
                    elif percentage < 0.1:
                        note = "RARE"
                    
                    print(f"   {letter:<6} {count:<10,} {percentage:<11.2f}% {male_percentage:<9.1f}% {note:<10}")
                
            except Exception as e:
                print(f"Could not generate statistics: {e}")
                import traceback
                traceback.print_exc()

    def get_total_employees_count(self) -> int:
        """Возвращает общее количество сотрудников в базе"""
        with self.Session() as session:
            return session.query(EmployeeDB).count()

    def query_male_f_surnames(self) -> List[Dict[str, Any]]:
        """
        Режим 5: Выборка мужчин с фамилией на 'F'
        """
        with self.Session() as session:
            try:
                if config.db_type == "sqlite":
                    
                    db_employees = session.query(EmployeeDB).filter(
                        EmployeeDB.gender == "Male",
                        EmployeeDB.full_name.like('F% %')  
                    ).order_by(EmployeeDB.full_name).all()  
                else:
                    db_employees = session.query(EmployeeDB).filter(
                        EmployeeDB.gender == "Male",
                        EmployeeDB.full_name.like('F%')
                    ).all()
                
                result = []
                for db_emp in db_employees:
                    employee = Employee.from_db_model(db_emp)
                    result.append({
                        'full_name': employee.full_name,
                        'birth_date': employee.birth_date.strftime('%Y-%m-%d'),
                        'gender': employee.gender,
                        'age': employee.calculate_age()
                    })
                
                return result
                
            except Exception as e:
                raise Exception(f"Error querying male F-surnames: {e}")

    def create_optimization_indexes(self):
        """
        Режим 6: Создание индексов для оптимизации запросов
        """
        with self.engine.connect() as conn:
            try:
                print("Creating optimization indexes...")
                
                drop_indexes_sql = [
                    "DROP INDEX IF EXISTS idx_employees_gender",
                    "DROP INDEX IF EXISTS idx_employees_full_name_prefix", 
                    "DROP INDEX IF EXISTS idx_employees_gender_fname_prefix",
                    "DROP INDEX IF EXISTS idx_employees_composite_optimized",
                    "DROP INDEX IF EXISTS idx_employees_gender_fname"
                ]
                
                for sql in drop_indexes_sql:
                    try:
                        conn.execute(sa.text(sql))
                    except:  # noqa
                        pass
                
                print("   Creating optimized composite index...")
                conn.execute(sa.text("CREATE INDEX idx_employees_gender_fname ON employees(gender, full_name)"))
                
                # Для SQLite важно обновить статистику
                if config.db_type == "sqlite":
                    conn.execute(sa.text("ANALYZE"))
                    conn.execute(sa.text("PRAGMA optimize"))
                    conn.execute(sa.text("PRAGMA cache_size=10000"))  
                    
                print("Optimized index created and statistics updated")
                
                print("\nOPTIMIZATION STRATEGY:")
                print("   - Single composite index on (gender, full_name)")
                print("   - Covers both filter conditions in one index")
                print("   - Database can perform index range scan")
                print("   - No need for multiple indexes")
                
            except Exception as e:
                raise Exception(f"❌ Error creating optimization indexes: {e}")

    def force_disable_indexes(self):
        """
        Вспомогательный метод: принудительно отключает индексы для тестирования
        """
        if config.db_type != "sqlite":
            print("Index disabling only supported for SQLite")
            return
            
        with self.engine.connect() as conn:
            try:
                conn.execute(sa.text("PRAGMA optimize=0"))
                print("Indexes temporarily disabled for testing")
            except Exception as e:
                print(f"Could not disable indexes: {e}")