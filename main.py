import psycopg2
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

conn = psycopg2.connect(
    database="python_db2",
    user="postgres",
    password="postgres"
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


def edit_user(cursor, user_id, first_name, last_name, email) -> None:
    """Функция, позволяющая изменить данные о клиенте."""
    cursor.execute("""
        UPDATE users
        SET first_name = %s, last_name = %s, email = %s
        WHERE id = %s RETURNING *;""", (first_name, last_name, email, user_id))
    logging.info(f"Пользователь изменен: {cursor.fetchall()}")


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


def find_user_by_id(cursor, user_id) -> None:
    """Функция, позволяющая найти пользователя по его id."""
    cursor.execute("""
        SELECT * FROM users
        WHERE id = %s;""", (user_id,))
    result = cursor.fetchall()
    if result:
        logging.info(f"Пользователь найден: {result}")
    else:
        logging.warning(f"Пользователь с id {user_id} не найден.")


def find_user_by_name(cursor, first_name, last_name) -> None:
    """Функция, позволяющая найти пользователя по его имени."""
    cursor.execute("""
        SELECT * FROM users
        WHERE first_name = %s AND last_name = %s;""", (first_name, last_name))
    result = cursor.fetchall()
    if result:
        logging.info(f"Пользователь найден: {result}")
    else:
        logging.warning(f"Пользователь с именем {first_name} {last_name} не найден.")


def find_user_by_email(cursor, email) -> None:
    """Функция, позволяющая найти пользователя по его email."""
    cursor.execute("""
        SELECT * FROM users
        WHERE email = %s;""", (email,))
    result = cursor.fetchall()
    if result:
        logging.info(f"Пользователь найден: {result}")
    else:
        logging.warning(f"Пользователь с email {email} не найден.")


def find_user_by_phone(cursor, number) -> None:
    """Функция, позволяющая найти пользователя по его телефону."""
    cursor.execute("""
        SELECT * FROM phones
        WHERE number = %s;""", (number,))
    result = cursor.fetchall()
    if result:
        logging.info(f"Пользователь найден: {result}")
    else:
        logging.warning(f"Пользователь с номером {number} не найден.")


def select_all_users(cursor) -> None:
    """Функция, позволяющая вывести всех пользователей."""
    cursor.execute("""
        SELECT * FROM users;""")
    users = cursor.fetchall()
    logging.info("Все пользователи: " + str(users))


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
        edit_user(cursor, 2, 'Petr', 'Petrovich', 'petr.petrovich@example.com')

        # Удаление телефона
        delete_phone(cursor, 2)

        # Поиск пользователей
        find_user_by_id(cursor, 1)
        find_user_by_name(cursor, 'Anna', 'Sidorova')
        find_user_by_email(cursor, 'ivan@example.com')
        find_user_by_phone(cursor, '1234567890')

        # Вывод всех пользователей
        select_all_users(cursor)

        # Удаление пользователя
        delete_user(cursor, 3)

        # Повторный вывод всех пользователей после удаления
        select_all_users(cursor)

        # Фиксация изменений
        conn.commit()


# Запуск проверки функций
main()

conn.close()
logging.info("Соединение с базой данных закрыто.")