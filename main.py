import sys
import traceback
import time
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
    print("Creating employees table...")
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


def mode_3(db_manager: DatabaseManager):
    """Режим 3: Вывод всех уникальных сотрудников"""
    print("Fetching all unique employees (sorted by full name)...")
    
    try:
        employees = db_manager.get_all_employees_unique_sorted()
        db_manager.print_employees_table(employees)
        
    except Exception as e:
        print(f"Error fetching employees: {e}")


def mode_4(db_manager: DatabaseManager):
    """Режим 4: Генерация тестовых данных"""
    print("Starting sample data generation...")
    
    try:
        current_count = db_manager.get_total_employees_count()
        if current_count > 0:
            print(f"Database already contains {current_count:,} employees")
            response = input("Continue and add more data? (y/N): ")
            if response.lower() != 'y':
                print("Operation cancelled")
                return
        
        db_manager.generate_sample_data()
        print("Sample data generation completed successfully!")
        
    except Exception as e:
        print(f"Error generating sample data: {e}")
        traceback.print_exc()


def mode_5(db_manager: DatabaseManager):
    """Режим 5: Запрос мужчин с фамилией на 'F' с замером времени"""
    print("Querying male employees with last name starting with 'F'...")
    
    try:
        start_time = time.time()
        results = db_manager.query_male_f_surnames()
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        print(f"Query completed in {execution_time:.4f} seconds")
        print(f"Found {len(results)} employees matching criteria")
        
        if results:
            print("\nFirst 10 results:")
            print("-" * 80)
            print(f"{'Full Name':<30} {'Birth Date':<12} {'Gender':<8} {'Age':<4}")
            print("-" * 80)
            
            for i, emp in enumerate(results[:10]):
                print(f"{emp['full_name']:<30} {emp['birth_date']:<12} {emp['gender']:<8} {emp['age']:<4}")
            
            if len(results) > 10:
                print(f"... and {len(results) - 10} more employees")
        
        print(f"\n Execution time: {execution_time:.4f} seconds")
        
        with open("query_time_before_optimization.txt", "w") as f:
            f.write(f"{execution_time:.4f}")
            
    except Exception as e:
        print(f"Error executing query: {e}")
        traceback.print_exc()


def mode_6(db_manager: DatabaseManager):
    """Режим 6: Оптимизация базы данных и сравнение производительности"""
    print("Starting database optimization...")
    
    try:
        print("1. Measuring performance BEFORE optimization...")
        start_time_before = time.time()
        results_before = db_manager.query_male_f_surnames()
        end_time_before = time.time()
        time_before = end_time_before - start_time_before
        
        print(f"   Query time before optimization: {time_before:.4f} seconds")
        print(f"   Found {len(results_before)} employees")
        
        print("\n2. Creating optimization indexes...")
        db_manager.create_optimization_indexes()
        
        print("\n3. Measuring performance AFTER optimization...")
        start_time_after = time.time()
        results_after = db_manager.query_male_f_surnames()
        end_time_after = time.time()
        time_after = end_time_after - start_time_after
        
        print(f"   Query time after optimization: {time_after:.4f} seconds")
        print(f"   Found {len(results_after)} employees")
        
        print("\n4. Performance comparison:")
        print(f"   Before optimization: {time_before:.4f} seconds")
        print(f"   After optimization:  {time_after:.4f} seconds")
        
        if time_before > 0:
            improvement = ((time_before - time_after) / time_before) * 100
            print(f"   Performance improvement: {improvement:+.1f}%")
        
        with open("optimization_results.txt", "w") as f:
            f.write("Database Optimization Results\n")
            f.write("=" * 40 + "\n")
            f.write("Query: Male employees with last name starting with 'F'\n")
            f.write(f"Results count: {len(results_before)} employees\n\n")
            f.write(f"BEFORE optimization: {time_before:.4f} seconds\n")
            f.write(f"AFTER optimization:  {time_after:.4f} seconds\n")
            f.write(f"Performance improvement: {improvement:+.1f}%\n\n")
            f.write("Optimization details:\n")
            f.write("- Created index on gender column\n")
            f.write("- Created index on full_name column\n")
            f.write("- Created composite index on (gender, full_name)\n")
        
        print("\nOptimization completed! Results saved to 'optimization_results.txt'")
        
    except Exception as e:
        print(f"Error during optimization: {e}")
        traceback.print_exc()


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
        
        elif mode == "3":
            mode_3(db_manager)
            
        elif mode == "4":
            mode_4(db_manager)
            
        elif mode == "5":
            mode_5(db_manager)
            
        elif mode == "6":
            mode_6(db_manager)
        
        else:
            print(f"Unknown mode: {mode}")
            print_usage()
          
    except Exception as e:
        print(f"Application error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()