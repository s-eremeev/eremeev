# BigTestingDotNew @DoctorApBot с синим цветочком
# управление мониторингом субъектов

import pandas as pd
import numpy as np
import os
import sqlite3
import telebot
from telebot import types
from requests.exceptions import ConnectionError

# import time
# import datetime
from datetime import datetime

# Токен
TOKEN = "7213028141:AAGztt3IBI8wDipz_gmXmbNhVagpnDqfwzU"  # token BigTestingDotNew

# пути
dir_path = os.getcwd().replace("\\", "/") + "/"
db_path = dir_path + "db.sqlite"
data_path = dir_path + "data/"

messages = {}
reg_name_eng = {}
period = {}
level = {}

rus_dict = {
    "Динамика достижения ЦУ по СЗП": "_1.png",
    "Тепловая карта – врачи": "_2.png",
    "Тепловая карта – СМП": "_3.png",
    "ЦУ по СЗП не достигнут": "_4.png",
    "ССЧ и ФОТ по категориям": "_5.png",
    "Доля ССЧ АУП": "_6.png",
}

sub_dict = {
    "Динамика достижения ЦУ по СЗП": "_1.png",
    "ЦУ по СЗП не достигнут": "_2.png",
    "ТОП МО с высоким ЦУ": "_3.png",
    "ТОП МО с низким ЦУ": "_4.png",
    "ССЧ и ФОТ по категориям": "_5.png",
    "Доля ССЧ АУП": "_6.png",
}


def user_activity(message):
    """логирование активности юзера"""
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name
    user_last_name = message.from_user.last_name
    user_message = message.text
    date_time = datetime.fromtimestamp(message.date).strftime("%Y-%m-%d %H:%M:%S")
    user_activity_list = [
        user_id,
        user_first_name,
        user_last_name,
        user_message,
        date_time,
    ]
    user_activity = pd.DataFrame(
        [user_activity_list],
        columns=[
            "user_id",
            "user_first_name",
            "user_last_name",
            "user_message",
            "date_time",
        ],
    )
    conn = sqlite3.connect(db_path)
    user_activity.to_sql("user_activity", conn, if_exists="append", index=False)


def errors(message, e):
    """логирование ошибок"""
    user_id = message.from_user.id
    date_time = datetime.fromtimestamp(message.date).strftime("%Y-%m-%d %H:%M:%S")
    error_list = [user_id, date_time, str(e)]
    error = pd.DataFrame([error_list], columns=["user_id", "date_time", "error"])
    conn = sqlite3.connect(db_path)
    error.to_sql("errors", conn, if_exists="append", index=False)


def send_report(message, answer_path):
    """отправка отчёта"""
    try:
        answer_report = open(answer_path, "rb")
        bot.send_photo(message.chat.id, answer_report)
        answer_report.close()
    except Exception as e:
        bot.send_message(message.chat.id, f"🚫Ошибка отчёта: {e}")
        errors(message, e)


# Создание экземпляра бота
bot = telebot.TeleBot(TOKEN)


# меню (меню)
@bot.message_handler(commands=["start"])
def start(message):
    """меню бота"""
    try:
        # логирование активности юзера
        user_activity(message)

        bot.delete_message(message.chat.id, message.message_id)
        bot.send_message(
            message.chat.id,
            text=f"""
            Здраствуйте, *{message.from_user.first_name}!*
    📖Этот чат-бот позволяет получить данные о заработной плате...""",
            parse_mode="Markdown",
        )
        bot.send_message(
            message.chat.id,
            text=f"""
        Для выбора режима работы воспользуйтесть *меню* в левом нижнем углу↙️

    ✅*Меню позволяет:*
    1️⃣Выбрать разрез данных (Российская федераци в целом или субъект Российской Федерации) и отчётный период (текущий год или текущий квартал).
    3️⃣Повторно отобразить данное сообщение. 

    ✅*Без использования меню возможно:*
    1️⃣Изменить отчётный период, отправив боту сообщение "год" ("квартал").
    2️⃣Изменить разрез информации, отправив боту сообщение с названием субъекта Российской Федерации и или сообщение "РФ" (варианты: "Россия", "Рф", "рф", символ 🇷🇺) для получения информации по Российской Федерации в целом.
    При изменении разреза информации отчётный период остётся прежним, при изменении отчётного периода не изменяется разрез информации.

    ❌Меню не позволяет очистить историю. Для этого воспользуйтесь соответствующей функцией🧹, выбрав _три вертикальные точки_ в правом верхнем углу экрана↗️. Очистка истории удаляет всю ранее введённую информацию.

    🚫 При указании некорректных данных бот вернёт соответствующее сообщение. В этом случае необходимо указать верное наименование субъекта Российской Федерации или воспользоваться меню🔄.
    """,
            parse_mode="Markdown",
        )

    except Exception as e:
        bot.send_message(
            message.chat.id,
            f"""🚫Общая ошибка: {e}
        ❗️Очистите историю, вернитесь в меню или обратитесь в службу поддержки""",
        )
        errors(message, e)


# начало работы с ботом
@bot.message_handler(commands=["launch"])
def all(message):
    """начало работы"""
    try:
        # логирование активности юзера
        user_activity(message)

        user_name = message.from_user.first_name

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton("🇷🇺 Россия: год")
        btn2 = types.KeyboardButton("🇷🇺 Россия: квартал")
        btn3 = types.KeyboardButton("Субъекты РФ: год")
        btn4 = types.KeyboardButton("Субъекты РФ: квартал")
        markup.add(btn1, btn2, btn3, btn4)

        bot.send_message(
            message.chat.id,
            f"🔀*{user_name}, выберете разрез данных и отчётный период:*",
            reply_markup=markup,
            parse_mode="Markdown",
        )
        bot.delete_message(message.chat.id, message.message_id)

    except Exception as e:
        bot.send_message(
            message.chat.id,
            f"""🚫Общая ошибка: {e}
        ❗️Очистите историю, вернитесь в меню или обратитесь в службу поддержки""",
        )
        errors(message, e)


# обработка сообщений (не меню)
@bot.message_handler(content_types=["text"])
def handle_message(message):
    """обработка сообщений"""
    try:
        # логирование активности юзера
        user_activity(message)

        global db_path, data_path, messages, reg_name_eng, df_valid_regions, period, level

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        query_m = """SELECT rr.reg_name_rus, rr.reg_name_eng FROM reg_ref rr;"""
        df_valid_regions = pd.DataFrame(cur.execute(query_m).fetchall())
        valid_regions_list_rus = df_valid_regions[0].tolist()

        query_u = """SELECT rr.reg_name_rus, rr.reg_name_eng FROM reg_ref rr
        LEFT JOIN unmonitoring um ON rr.reg_name_eng = um.reg_name_eng 
        WHERE um.reg_name_eng IS NOT NULL;"""
        df_unmonitoring_regions = pd.DataFrame(cur.execute(query_u).fetchall())
        unmonitoring_regions_list_rus = df_unmonitoring_regions[0].tolist()
        unmonitoring_regions_list_eng = df_unmonitoring_regions[1].tolist()
        unmonitoring_regions_set_eng = set(unmonitoring_regions_list_eng)

        user_id = message.from_user.id
        messages[user_id] = message.text

        if messages.get(user_id) in [
            "🇷🇺 Россия: год",
            "Субъекты РФ: год",
            "год",
            "Год",
        ]:
            period[user_id] = "год"

        elif messages.get(user_id) in [
            "🇷🇺 Россия: квартал",
            "Субъекты РФ: квартал",
            "квартал",
            "Квартал",
        ]:
            period[user_id] = "квартал"

        if (
            messages.get(user_id) in ["🇷🇺 Россия: год", "🇷🇺 Россия: квартал"]
            or (
                messages.get(user_id) in ["год", "Год", "Квартал", "квартал"]
                and level[user_id] == "rus"
            )
            or (messages.get(user_id) in ["РФ", "Рф", "рф", "Россия", "🇷🇺"])
        ):
            level[user_id] = "rus"
            bot.delete_message(message.chat.id, message.message_id)
            bot.delete_message(message.chat.id, message.message_id - 1)
            answer_title_path = data_path + "Russia/title.png"
            # bot.send_message(message.chat.id, f"титул РФ: {answer_title_path}")
            send_report(message, answer_title_path)
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
            btn1 = types.KeyboardButton("Динамика достижения ЦУ по СЗП")
            btn2 = types.KeyboardButton("Тепловая карта – врачи")
            btn3 = types.KeyboardButton("Тепловая карта – СМП")
            btn4 = types.KeyboardButton("ЦУ по СЗП не достигнут")
            btn5 = types.KeyboardButton("ССЧ и ФОТ по категориям")
            btn6 = types.KeyboardButton("Доля ССЧ АУП")
            markup.add(btn1, btn2, btn3, btn4, btn5, btn6)
            bot.send_message(
                message.chat.id,
                f"Выберете отчёт для РФ ({period.get(user_id)})",
                reply_markup=markup,
            )

        elif messages.get(user_id) in ["Субъекты РФ: год", "Субъекты РФ: квартал"]:
            level[user_id] = "sub"
            bot.delete_message(message.chat.id, message.message_id)
            bot.send_message(
                message.chat.id,
                f"*Укажите субъект Российской Федерации, по которому необходимо предоставить информацию:*",
                parse_mode="Markdown",
            )

        elif messages.get(user_id) == "*666#":
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
            btn1 = types.KeyboardButton("Список исключений")
            btn2 = types.KeyboardButton("Изменить исключения")
            btn3 = types.KeyboardButton("Вернуться в меню")
            markup.add(btn1, btn2, btn3)
            bot.delete_message(message.chat.id, message.message_id)
            bot.send_message(
                message.chat.id,
                "🖥В этом разделе осуществляется управление `глобальным` списком субъектов Российской Федерации, по которым временно не проводится мониторинг",
                reply_markup=markup,
                parse_mode="Markdown",
            )
            markup_l = types.InlineKeyboardMarkup()
            btn4 = types.InlineKeyboardButton(
                "получить таблицу соответствия наименований",
                url="https://file.rosminzdrav.ru/s/cHDQCPDcNnXfC4c",
            )
            markup_l.add(btn4)
            bot.send_message(
                message.chat.id,
                "🔰Для редактирования списка субъектов РФ потребуется знание _корректного_ наименования региона (lat), принятого в `КЦ МЗ России`:",
                reply_markup=markup_l,
                parse_mode="Markdown",
            )

        elif messages.get(user_id) == "Вернуться в меню":
            bot.delete_message(message.chat.id, message.message_id - 1)
            all(message)

        elif messages.get(user_id) == "Список исключений":
            bot.delete_message(message.chat.id, message.message_id)
            # bot.delete_message(message.chat.id, message.message_id-1)
            bot.send_message(
                message.chat.id,
                str("\n".join(list(unmonitoring_regions_set_eng))),
                parse_mode="Markdown",
            )

        elif messages.get(user_id) == "Изменить исключения":
            bot.delete_message(message.chat.id, message.message_id)
            # bot.delete_message(message.chat.id, message.message_id-1)
            bot.send_message(
                message.chat.id,
                """
        Для включения региона в список исключений отправьте:
                1# <_название региона, lat_>
Для удаления региона из списка исключений отправьте:
                2# <_название региона, lat_>""",
                parse_mode="Markdown",
            )

        elif messages.get(user_id).split(" ")[0] == "1#":
            bot.delete_message(message.chat.id, message.message_id)
            ans = messages.get(user_id).split(" ")[1]
            bot.send_message(
                message.chat.id,
                f"""
            ☝️Если список исключений не изменился, название региона было указано неверно.
    Не упорствуйте в своём заблеждении, укажите _принятое в КЦ МЗ_  название региона в латинской раскладке""",
                parse_mode="Markdown",
            )
            query_in = "INSERT INTO unmonitoring (reg_name_eng) VALUES ('" + ans + "')"
            cur.execute(query_in)
            conn.commit()

        elif messages.get(user_id).split(" ")[0] == "2#":
            bot.delete_message(message.chat.id, message.message_id)
            ans = messages.get(user_id).split(" ")[1]
            bot.send_message(
                message.chat.id,
                f"""
        ☝️Если список исключений не изменился, *название региона было указано неверно*.
    Не упорствуйте в своём заблеждении, укажите _принятое в КЦ МЗ_ название региона в латинской раскладке""",
                parse_mode="Markdown",
            )
            query_out = "DELETE FROM unmonitoring WHERE reg_name_eng = '" + ans + "'"
            cur.execute(query_out)
            conn.commit()

        elif (
            messages.get(user_id) in valid_regions_list_rus
            and messages.get(user_id) not in unmonitoring_regions_list_rus
        ):
            bot.delete_message(message.chat.id, message.message_id)
            level[user_id] = "sub"
            reg_name_eng[user_id] = df_valid_regions[
                df_valid_regions[0] == messages.get(user_id)
            ].iloc[0][1]
            answer_title_path_reg = data_path + reg_name_eng.get(user_id) + "/title.png"
            # bot.send_message(message.chat.id, f"титул региона: {answer_title_path_reg}")
            send_report(message, answer_title_path_reg)
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
            btn1 = types.KeyboardButton("Динамика достижения ЦУ по СЗП")
            btn2 = types.KeyboardButton("ЦУ по СЗП не достигнут")
            btn3 = types.KeyboardButton("ТОП МО с высоким ЦУ")
            btn4 = types.KeyboardButton("ТОП МО с низким ЦУ")
            btn5 = types.KeyboardButton("ССЧ и ФОТ по категориям")
            btn6 = types.KeyboardButton("Доля ССЧ АУП")
            markup.add(btn1, btn2, btn3, btn4, btn5, btn6)
            bot.send_message(
                message.chat.id,
                f"Выберете отчёт для: {messages.get(user_id)} ({period.get(user_id)})",
                reply_markup=markup,
            )

        elif (
            messages.get(user_id) in valid_regions_list_rus
            and messages.get(user_id) in unmonitoring_regions_list_rus
        ):
            bot.delete_message(message.chat.id, message.message_id)
            bot.send_message(
                message.chat.id,
                f"⛔️Мониторинг по *{messages.get(user_id)}* временно не осуществляется",
                parse_mode="Markdown",
            )

        elif (
            messages.get(user_id) in ["год", "Год", "Квартал", "квартал"]
            and level[user_id] == "sub"
        ):
            bot.delete_message(message.chat.id, message.message_id)
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
            btn1 = types.KeyboardButton("Динамика достижения ЦУ по СЗП")
            btn2 = types.KeyboardButton("ЦУ по СЗП не достигнут")
            btn3 = types.KeyboardButton("ТОП МО с высоким ЦУ")
            btn4 = types.KeyboardButton("ТОП МО с низким ЦУ")
            btn5 = types.KeyboardButton("ССЧ и ФОТ по категориям")
            btn6 = types.KeyboardButton("Доля ССЧ АУП")
            markup.add(btn1, btn2, btn3, btn4, btn5, btn6)
            bot.send_message(
                message.chat.id,
                f"Выберете отчёт за {messages.get(user_id)} (*регион прежний*)",
                reply_markup=markup,
                parse_mode="Markdown",
            )

        elif messages.get(user_id) in rus_dict and level[user_id] == "rus":
            bot.delete_message(message.chat.id, message.message_id)
            answer_path_rus = (
                data_path
                + "Russia/"
                + period.get(user_id)
                + "/"
                + "Russia"
                + rus_dict.get(messages.get(user_id))
            )
            # bot.send_message(message.chat.id, f"отчёт: {answer_path_rus}")
            send_report(message, answer_path_rus)

        elif messages.get(user_id) in sub_dict and level[user_id] == "sub":
            bot.delete_message(message.chat.id, message.message_id)
            answer_path_sub = (
                data_path
                + "/"
                + reg_name_eng.get(user_id)
                + "/"
                + period.get(user_id)
                + "/"
                + reg_name_eng.get(user_id)
                + sub_dict.get(messages.get(user_id))
            )
            # bot.send_message(message.chat.id, f"отчёт: {answer_path_sub}")
            send_report(message, answer_path_sub)

        # неизвестное слово
        elif (
            messages.get(user_id) not in rus_dict
            and messages.get(user_id) not in sub_dict
            and messages.get(user_id)
            not in [
                "РФ",
                "Рф",
                "рф",
                "Россия",
                "🇷🇺",
                "🇷🇺 Россия: год",
                "🇷🇺 Россия: квартал",
                "Субъекты РФ: год",
                "Субъекты РФ: квартал",
                "год",
                "Год",
                "квартал",
                "Квартал",
            ]
        ):
            bot.delete_message(message.chat.id, message.message_id)
            bot.send_message(
                message.chat.id,
                "🚫Неизвестный запрос или команда меню",
                parse_mode="Markdown",
            )

    except Exception as e:
        bot.send_message(
            message.chat.id,
            f"""🚫Общая ошибка: {e}
❗️Очистите историю, вернитесь в меню или обратитесь в службу поддержки""",
        )
        errors(message, e)


# запуск бота
bot.infinity_polling()