import os
import csv
import psycopg2


def connect_to_db():
    """
    Подключение к БД
    """
    try:
        connection = psycopg2.connect(
            host='127.0.0.1',
            port=5432,
            database='north',
            user='postgres',
            password='admin'
        )
        return connection, connection.cursor()
    except psycopg2.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None


def insert_data_from_csv(cursor, table_name, csv_filename):
    """Считывание csv файла"""
    with open(csv_filename, newline='') as file:
        reader = csv.reader(file)
        # Пропустить заголовок
        next(reader)
        for row in reader:
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(insert_query, row)


def main():
    connection, cursor = connect_to_db()
    if not connection:
        return

    data_folder = f"{os.path.join(os.path.dirname(__file__))}/north_data"

    try:
        # Заполнение таблицы "employees"
        insert_data_from_csv(cursor, 'employees', os.path.join(data_folder, 'employees_data.csv'))

        # Заполнение таблицы "customers"
        insert_data_from_csv(cursor, 'customers', os.path.join(data_folder, 'customers_data.csv'))

        # Заполнение таблицы "orders"
        insert_data_from_csv(cursor, 'orders', os.path.join(data_folder, 'orders_data.csv'))

        # Сохранение изменений и закрытие соединения с БД
        connection.commit()
    except psycopg2.Error as e:
        print(f"Ошибка при выполнении SQL-запросов: {e}")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
