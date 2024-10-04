import pandas as pd
import numpy as np
import os
import telebot
from telebot import types
from requests.exceptions import ConnectionError
import pendulum
import traceback
from sqlalchemy import create_engine, MetaData, Table, select, delete, update, insert, text, Column, String, Integer, BigInteger, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from dbconfig_conn_BotDB import TgBot_Prikaz65


# Получение данных для подключения к базе
host, port, user, password, database = TgBot_Prikaz65()

# Получаем токен бота из переменных окружения
# BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
# ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID')

# Токен бота
BOT_TOKEN = 'TELEGRAM_BOT_TOKEN' # Указываем свой токен
ADMIN_CHAT_ID = 297179772  # Указываем свой ID администратора (здесь - мой - Олег)

# Создание экземпляра бота
bot = telebot.TeleBot(BOT_TOKEN)

# Подключение к PostgreSQL
conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(conn_string)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

# Таблица авторизованных пользователей
auth_users = Table('auth_users', metadata, autoload_with=engine)

 # Временное хранилище данных пользователей, запросивших доступ
users_requested_access = {}

# Временные переменные
messages = {}

# Проверка, есть ли бользователь в БД
def is_authorized_user(user_id):
    """Проверка, есть ли пользователь в таблице auth_users."""
    try:
        with engine.connect() as connection:
            stmt = select(auth_users.c.user_id).where(auth_users.c.user_id == user_id)
            result = connection.execute(stmt)
            user = result.fetchone()
            return user is not None
    except SQLAlchemyError as e:
        print(f"Ошибка при проверке пользователя: {e}")
        return False

# Добавление пользователя в БД
def add_user_to_db(user_id, username, user_first_name, user_last_name, date_time):
    """Добавить пользователя в базу данных"""
    with engine.connect() as conn:
        trans = conn.begin() # Начало транзакции
        try:
            # Проверка, существует ли уже пользователь перед добавлением в БД
            query = select(auth_users.c.user_id).where(auth_users.c.user_id == user_id)
            result = conn.execute(query).fetchone()

            if result:
                raise ValueError(f"Пользователь {username} уже существует в базе данных")
            else:
                # Добавление пользователя
                insert_stmt = insert(auth_users).values(user_id=user_id, username=username, user_first_name=user_first_name, user_last_name=user_last_name, date_time=date_time)
                conn.execute(insert_stmt)
        except:
            trans.rollback() # Откат транзакции в случае ошибки
            raise
        else:
            trans.commit() # Подтверждение транзакции

# Стартовое сообщение
@bot.message_handler(commands=["start"])
def start(message):
    user_id = message.from_user.id
    date_time = pendulum.from_timestamp(message.date).in_timezone("Europe/Moscow").strftime("%Y-%m-%d %H:%M:%S")
    # Добавим отладочное сообщение, чтобы проверить, отлавливает ли бот пользователя
    print(f"Пользователь с ID {user_id} запустил команду /start")

    # Проверяем, авторизован ли пользователь
    if not is_authorized_user(user_id):
        # Добавим отладочное сообщение, чтобы проверить условие
        print(f"Пользователь с ID {user_id} не авторизован")
        
        # Создаем инлайн кнопку
        markup = types.InlineKeyboardMarkup()
        join_button = types.InlineKeyboardButton('Присоединиться', callback_data='join_request')  # Инлайн кнопка
        markup.add(join_button)
        
        # Отправляем сообщение с клавиатурой
        msg = bot.send_message(message.chat.id, "Вы не авторизованы. Пожалуйста, запросите доступ у администратора - нажмите кнопку Присоединиться.", reply_markup=markup)

        # Сохраняем ID сообщения с кнопкой Join
        users_requested_access[user_id] = {
            "message_id": msg.message_id,
            "user_id": user_id,
            "username": message.from_user.username,
            "user_first_name": message.from_user.first_name,
            "user_last_name": message.from_user.last_name,
            "date_time": date_time
        }
        return
    
"""Остальной код"""

# Обработка коллбэков от администратора
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    if call.data == 'join_request':
        user_id = call.from_user.id
        print(f"Пользователь с ID {user_id} нажал кнопку Join")
        
        # Проверяем, есть ли пользователь в словаре и добавляем, если нет
        if user_id not in users_requested_access:
            print(f"Добавляем пользователя {user_id} во временное хранилище users_requested_access")
            users_requested_access[user_id] = {
                'message_id': call.message.message_id,  # Сохраняем message_id
                'user_id': user_id,
                'username': call.from_user.username,
                'user_first_name': call.from_user.first_name,
                'user_last_name': call.from_user.last_name,
                'date_time': pendulum.now().to_datetime_string()
            }
        else:
            print(f"Пользователь {user_id} уже существует в временном хранилище")
        
        # Запрос на одобрение отправляется администратору
        markup = types.InlineKeyboardMarkup()
        accept_button = types.InlineKeyboardButton("Принять", callback_data=f"accept_{user_id}")
        decline_button = types.InlineKeyboardButton("Отклонить", callback_data=f"decline_{user_id}")
        markup.add(accept_button, decline_button)
        
        # Отправляем администратору запрос на подтверждение
        bot.send_message(ADMIN_CHAT_ID, f"Пользователь {call.from_user.first_name} {call.from_user.last_name} с ID {user_id} запросил доступ. Принять?", reply_markup=markup)
        
        # Сообщение для пользователя, что его запрос отправлен
        bot.answer_callback_query(call.id, "Запрос отправлен администратору. Ожидайте. После обработки запроса вам поступит соответствующее уведомление.")
    
    # Одобрение пользователя
    elif call.data.startswith('accept_'):
        user_id = int(call.data.split('_')[1])
        print(f"Администратор одобрил пользователя {user_id}")
        
        # Проверяем, есть ли пользователь в словаре
        if user_id in users_requested_access:
            user_info = users_requested_access[user_id]
            print(f"Информация о пользователе найдена: {user_info}")
            try:
                # Сохраняем пользователя в БД через функцию
                add_user_to_db(
                    user_id=user_info['user_id'],
                    username=user_info['username'],
                    user_first_name=user_info['user_first_name'],
                    user_last_name=user_info['user_last_name'],
                    date_time=user_info['date_time']
                )
                
                # Уведомляем пользователя о доступе
                bot.send_message(user_id, "Ваш запрос на доступ был одобрен. Добро пожаловать!")
                
                # Удаляем сообщение с кнопкой Join, если нужно
                if 'message_id' in user_info:
                    bot.delete_message(chat_id=user_id, message_id=user_info['message_id'])
                
                # Уведомляем администратора
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=f"Пользователь с ID {user_id} одобрен.")
                
                # Удаляем данные о пользователе из временного хранилища после обработки
                print(f"Удаляем данные о пользователе {user_id} из временного хранилища")
                users_requested_access.pop(user_id, None)
            except Exception as e:
                bot.send_message(call.message.chat.id, f"Ошибка при добавлении пользователя: {str(e)}")
        else:
            print(f"Информация о пользователе с ID {user_id} не найдена в users_requested_access")
            bot.send_message(call.message.chat.id, "Ошибка: информация о пользователе не найдена.")

    # Отклонение пользователя
    elif call.data.startswith('decline_'):
        user_id = int(call.data.split('_')[1])
        bot.send_message(user_id, "Ваш запрос на доступ был отклонен.")
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=f"Пользователь c ID {user_id} отклонен.")
        
        # Удаляем данные о пользователе из временного хранилища после обработки
        print(f"Удаляем данные о пользователе {user_id} из временного хранилища")
        users_requested_access.pop(user_id, None)

"""Остальной код"""