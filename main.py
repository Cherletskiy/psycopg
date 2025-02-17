import psycopg2
import logging
from logging.handlers import RotatingFileHandler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler("app.log", maxBytes=1024*1024, backupCount=50, encoding="utf-8", delay=True),
        logging.StreamHandler()
    ]
)


def create_db(cursor) -> None:
    """Функция, создающая структуру БД (таблицы)."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL);

        CREATE TABLE IF NOT EXISTS phones (
            id SERIAL PRIMARY KEY,
            number VARCHAR(255) NOT NULL,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE); 
        """)
    logging.info("База данных и таблицы созданы (если их не было).")


def add_user(cursor, first_name, last_name, email) -> None:
    """Функция, позволяющая добавить нового клиента."""
    cursor.execute("""
        INSERT INTO users (first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING *;""", (first_name, last_name, email))
    logging.info(f"Добавлен новый пользователь: {cursor.fetchall()}")


def add_phone(cursor, number, user_id) -> None:
    """Функция, позволяющая добавить телефон для существующего клиента."""
    cursor.execute("""
        INSERT INTO phones (number, user_id)
        VALUES (%s, %s) RETURNING *;""", (number, user_id))
    logging.info(f"Добавлен новый телефон: {cursor.fetchall()}")


def edit_user(cursor, user_id, first_name=None, last_name=None, email=None) -> None:
    """Функция, позволяющая изменить данные о клиенте."""
    fields_list = []
    values_list = []

    if first_name:
        fields_list.append(f"first_name = %s")
        values_list.append(first_name)
    if last_name:
        fields_list.append(f"last_name = %s")
        values_list.append(last_name)
    if email:
        fields_list.append(f"email = %s")
        values_list.append(email)

    if not fields_list:
        logging.warning("Нет полей для обновления.")
        return

    values_list.append(user_id)

    cursor.execute(f"""
        UPDATE users
        SET {", ".join(fields_list)}
        WHERE id = %s RETURNING *;""", tuple(values_list))

    logging.info(f"Пользователь обновлен: {cursor.fetchall()}")


def delete_phone(cursor, user_id) -> None:
    """Функция, позволяющая удалить телефон для существующего клиента."""
    cursor.execute("""
        DELETE FROM phones
        WHERE user_id = %s RETURNING *;""", (user_id,))
    logging.info(f"Телефон удален: {cursor.fetchall()}")


def delete_user(cursor, user_id) -> None:
    """Функция, позволяющая удалить существующего клиента."""
    cursor.execute("""
        DELETE FROM users
        WHERE id = %s RETURNING *;""", (user_id,))
    logging.info(f"Пользователь удален: {cursor.fetchall()}")


def find_user(cursor, first_name=None, last_name=None, email=None, number=None) -> None:
    """Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону."""
    fields_list = []
    values_list = []

    if first_name:
        fields_list.append(f"first_name = %s")
        values_list.append(first_name)
    if last_name:
        fields_list.append(f"last_name = %s")
        values_list.append(last_name)
    if email:
        fields_list.append(f"email = %s")
        values_list.append(email)
    if number:
        result = find_user_by_phone(cursor, number)
        if result:
            fields_list.append(f"id = %s")
            values_list.append(result[2])

    if not fields_list:
        logging.warning("Не передана информация для поиска")
        return

    cursor.execute(f"""
            SELECT * FROM users
            WHERE {" AND ".join(fields_list)};""", tuple(values_list))

    result = cursor.fetchall()

    if result:
        logging.info(f"Пользователь найден: {result}")
    else:
        logging.warning(f"Не найден пользователь по данным {values_list}")


def find_user_by_phone(cursor, number) -> None:
    """Функция, позволяющая найти пользователя по его телефону."""
    cursor.execute("""
        SELECT * FROM phones
        WHERE number = %s;""", (number,))
    result = cursor.fetchone()
    return result


def select_all_users(cursor) -> None:
    """Функция, позволяющая вывести всех пользователей."""
    cursor.execute("""
        SELECT * FROM users;""")
    users = cursor.fetchall()
    logging.info("Все пользователи: " + str(users))


with psycopg2.connect(database="python_db2", user="postgres", password="postgres") as conn:
    def main():
        with conn.cursor() as cursor:
            # Удаление таблиц
            cursor.execute("""
                    DROP TABLE IF EXISTS phones;
                    DROP TABLE IF EXISTS users;
                """)
            logging.info("Удалены таблицы phones и users (если они существовали).")

            # Создание таблиц
            create_db(cursor)

            # Добавление пользователей
            add_user(cursor, 'Ivan', 'Ivanov', 'ivan@example.com')
            add_user(cursor, 'Petr', 'Petrov', 'petr@example.com')
            add_user(cursor, 'Anna', 'Sidorova', 'anna@example.com')

            # Добавление телефонов
            add_phone(cursor, '1234567890', 1)
            add_phone(cursor, '9876543210', 1)
            add_phone(cursor, '1112233445', 2)

            # Изменение пользователя
            edit_user(cursor, 1, first_name="Ivan_1")
            edit_user(cursor, 1, last_name="Ivanov_1")
            edit_user(cursor, 1, email="new_ivan@example.com")
            edit_user(cursor, 2, first_name="Petr_1", last_name="Petrov_2", email="new_petr@example.com")

            # Удаление телефона
            delete_phone(cursor, 3)

            # Поиск пользователей
            find_user(cursor, first_name="Ivan")
            find_user(cursor, last_name="Ivanov")
            find_user(cursor, email="ivan@example.com")
            find_user(cursor, number="1234567890")
            find_user(cursor, first_name="Ivan", last_name="Ivanov", email="ivan@example.com", number="1234567890")

            # Вывод всех пользователей
            select_all_users(cursor)

            # Удаление пользователя
            delete_user(cursor, 3)

            # Повторный вывод всех пользователей после удаления
            select_all_users(cursor)


    # Запуск проверки функций
    main()

logging.info("Соединение с базой данных закрыто.")