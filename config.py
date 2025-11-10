from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """
    Конфигурация для подключения к базе данных.
    """
    db_type: str = "sqlite"
    host: str = "localhost"
    port: int = 5432
    database: str = "employees.db"
    username: str = ""
    password: str = ""
    
    def get_connection_string(self):
        """Возвращает строку подключения в зависимости от типа БД"""
        if self.db_type == "sqlite":
            return f"sqlite:///{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

config = DatabaseConfig()