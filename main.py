import sys
import traceback
from database import DatabaseManager

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
            # Режим 1: Создание таблицы сотрудников
            print("🔄 Creating employees table...")
            if db_manager.test_connection():
                db_manager.create_tables()
        
        else:
            print(f"❌ Mode {mode} will be implemented in next steps")
            print("   For now, only mode 1 (create table) is available")
            
    except Exception as e:
        print(f"💥 Application error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()