import sys
import traceback
from database import DatabaseManager
from models import Employee

def print_usage():
    """Выводит справку по использованию приложения"""

    print("Employee Directory Application")
    print("Usage: python main.py <mode> [arguments]")
    print("\nModes:")
    print("  1 - Create employees table")
    print("  2 - Create employee record: python main.py 2 \"Full Name\" YYYY-MM-DD Gender")
    print("  3 - List all unique employees")
    print("  4 - Generate sample data (1,000,000 records)")
    print("  5 - Query male employees with last name starting with 'F'")
    print("  6 - Optimize database and measure performance")
    print("\nExamples:")
    print("  python main.py 1")
    print('  python main.py 2 "Ivanov Petr Sergeevich" 2009-07-12 Male')


def mode_1(db_manager: DatabaseManager):
    """Режим 1: Создание таблицы сотрудников"""
    print("🔄 Creating employees table...")
    if db_manager.test_connection():
        db_manager.create_tables()
        print("Таблица сотрудников успешно создана")
    else:
        print("Ошибка подключения к базе данных")

def mode_2(args, db_manager):
    """Обрабатывает режим 2: создание сотрудника"""
    if len(args) != 5:
        print("Invalid arguments for mode 2")
        print("Usage: python main.py 2 \"Full Name\" YYYY-MM-DD Gender")
        print("Example: python main.py 2 \"Ivanov Petr Sergeevich\" 2009-07-12 Male")
        return
    
    full_name = args[2]
    birth_date_str = args[3]
    gender = args[4]
    
    try:
        employee = Employee.from_command_line(full_name, birth_date_str, gender)
        employee_id = db_manager.create_employee(employee)
        
        print(f"Employee successfully created with ID: {employee_id}")
        
    except ValueError as e:
        print(f"Validation error: {e}")
    except Exception as e:
        print(f"Error creating employee: {e}")

def main():
    """
    Главная функция приложения.
    Обрабатывает аргументы командной строки и запускает соответствующий режим.
    """
    if len(sys.argv) < 2:
        print_usage()
        return
    
    mode = sys.argv[1]
    db_manager = DatabaseManager()
    
    try:
        if mode == "1":
            mode_1(db_manager)
        
        elif mode == "2":
            mode_2(sys.argv, db_manager)
        
    except Exception as e:
        print(f"💥 Application error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()