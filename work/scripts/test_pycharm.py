# BigTestingDotNew @DoctorApBot с синим цветочком
import pandas as pd
import numpy as np
import os
import sqlite3
import telebot
from telebot import types
from requests.exceptions import ConnectionError

from yandexgptlite import YandexGPTLite
account = YandexGPTLite('b1g40ih9vkcl3ce44hob', 'y0_AgAAAABDOmatAATuwQAAAAEM6u7IAAA3BzDC5OJHoImEL0vYxPon0E1tfQ')


def simply(text):
    lst = text.split("\n\n")[0].split(",")
    lst_a = []
    for element in lst:
        element = "".join(
            x for x in element if x.isalnum() or x in [" ", "–", "-", "."]
        )
        element = (
            element.replace("Ключевые слова", "")
            .replace("ключевые слова", "")
            .replace("В этом тексте", "")
            .replace("В тексте", "")
            .replace("в этом тексте", "")
            .replace("в тексте", "")
            .replace("можно выделить", "")
            .replace("следующие", "")
            .replace("из текста", "")
        )
        lst_a.append(element)
    return ", ".join(lst_a)


def y_gpt(message):
    text = account.create_completion(
        f'Выдели ключевые слова из текста: "{message}?"',
        "1",
        system_prompt="отвечай на русском языке",
    )
    simply_text = simply(text)
    text_2 = account.create_completion(
        f'Выдели ключевые слова из текста: "{simply_text}"',
        "1",
        system_prompt="отвечай на русском языке",
    )
    # text_2
    return simply_text, simply(text_2)


# Токен
TOKEN = "7213028141:AAGztt3IBI8wDipz_gmXmbNhVagpnDqfwzU"  # token BigTestingDotNew
# Создание экземпляра бота
bot = telebot.TeleBot(TOKEN)


# меню (меню)
@bot.message_handler(commands=["start"])
def start(message):
    """меню бота"""
    try:
        # bot.delete_message(message.chat.id, message.message_id)
        bot.send_message(
            message.chat.id,
            text=f"""
            Здраствуйте, *{message.from_user.first_name}!*
    📖Этот чат-бот позволяет тестировать взаимодействие с *Yandex_GPT*""",
            parse_mode="Markdown",
        )
    except Exception as e:
        bot.send_message(
            message.chat.id,
            f"""🚫Общая ошибка: {e}
        ❗️Очистите историю, вернитесь в меню или обратитесь в службу поддержки""",
        )


# обработка сообщений (не меню)
@bot.message_handler(content_types=["text"])
def handle_message(message):
    """обработка сообщений"""
    aswer_ygpt_1, aswer_ygpt_2 = y_gpt(message.text)
    bot.send_message(
        message.chat.id,
        f"*Yandex GPT сперава сказал*: {aswer_ygpt_1}, *а засим добавил*: {aswer_ygpt_2}",
        parse_mode="Markdown",
    )


# запуск бота
bot.infinity_polling()