from random import randint
# %load_ext lab_black

f = open("/home/sk27/work/python/tk/q.txt", "r", encoding="utf-8")
numq = 0  # Случайное число для выбора вопроса из общего списка вопросов
numberQuestions = int(
    input("Определите количество вопросов в тестовом задании (обычно 60): ")
)
questionsAsked = 0  # Счётчик количества заданных вопросов
numCorrectAnswers = 0  # Количество правильных ответов
userName = input("Укажите вашу фамилию и инициалы: ")
dateTest = input("Укажите дату тестирования (дд.мм.гггг): ")
fq = open("/home/sk27/work/python/tk/Протокол.txt", "w")
fq.write("Фамилия, инициалы: ")
fq.write(userName)
fq.write(" Дата тестирования: ")
fq.write(dateTest)
fq.write(" Вопросов в задании (пользователь): ")
fq.write(str(numberQuestions))
allQuestions = {}  # Создали пустой словать для хранения полного списка вопросов
for x in f:  # Извлекаем из файла с вопросами по одной строке
    id_Question = int(x[0:4])  # Отделяем первые символы и преобразуем их в целое число
    Question = x[5:1000]  # А сюда помещаем всё, что осталось в строке
    Question = Question.replace("\n", "")  # Убираем разрыв строки
    allQuestions[
        id_Question
    ] = Question  # помещаем номер вопроса и всё остальное в словарь
if (
    len(allQuestions) >= numberQuestions
):  # Проверим, не превышает ли заявленное для тестирования число вопросов их общее количество
    numberQuestions = numberQuestions
else:
    numberQuestions = len(allQuestions)
while questionsAsked < numberQuestions:
    numq = randint(
        1, 12
    )  # Сгенерируем случайное число, НЕ ЗАБЫТЬ ЗАМЕНИТЬ МАКСИМАЛЬНОЕ ЗНАЧЕНИЕ В СООТВЕТСТВИИ С КОЛИЧЕСТВОМ ВОПРОСОВ!
    nextQuestion = allQuestions.get(
        numq, ""
    )  # Выберем из словаря вопрос, номер которого равен случайному числу. Если такого элемента в словаре нет, Q принимает пустое значение
    if nextQuestion != "":
        Qstr = nextQuestion.split(
            "|"
        )  # Разбиваем строку вопроса и делаем из нее список
        fq.write("\n")
        fq.write("\n")
        fq.write("Вопрос: ")
        fq.write(Qstr[0])  # Записываем в файл протокола вопрос
        for (
            y
        ) in (
            Qstr
        ):  # Отвечаем на этот вопрос и заносим результаты в протокол. Теперь это надо зациклить и добавить удаление
            # вопроса, если он уже задавался. И подсчёт результатов тестирования
            if len(y) > 2:
                print(y)
            else:
                QQ = input("Введите номер правильного ответа и нажмите Enter: ")
                if QQ == y:
                    otm = "Верно"
                    numCorrectAnswers += 1  # Посчитаем правильный ответ
                else:
                    otm = "Неверно"
                print(otm)
                fq.write("\n")  # Запишем это всё в файл протокола
                fq.write("Правильный ответ: ")
                fq.write(Qstr[int(y)])
                fq.write("\n")
                fq.write("Дан ответ: ")
                try:
                    fq.write(
                        Qstr[int(QQ)]
                    )  # Если ввести число большее, чем количество ответов, ТУТ ВОЗНИКАЕТ ИСКЛЮЧЕНИЕ!!!
                except IndexError:
                    fq.write(
                        "введённый пользователем номер ответа больше, чем предложенное количество вариантов ответа"
                    )  # А тут я его обработал!
                fq.write("\n")
                fq.write(otm)
                allQuestions.pop(numq, "")  # Удаляем из словаря использованный вопрос
        questionsAsked += 1  # Увеличиваем счётчик количества заданных вопросов
dol = numCorrectAnswers / questionsAsked * 100  # Определяем долю верных ответов
fq.write("\n")
fq.write(
    "________________________________________________________________________________"
)
fq.write("\n")
fq.write("Количество верных ответов: ")
fq.write(str(numCorrectAnswers))
fq.write("\n")
fq.write("Доля верных ответов: ")
fq.write(str(dol))
print("Заявлено вопросов: ", numberQuestions)
print("Задано вопросов: ", questionsAsked)
print("Дано верных ответов: ", numCorrectAnswers)
print("Доля верных ответов: %.2f" % dol, "%")
f.close()
fq.close()
input("Нажмите...")


