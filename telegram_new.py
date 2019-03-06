# -*- coding: utf-8 -*-
import logging

from telethon import TelegramClient,sync, utils, errors
from telethon.tl.types import PeerUser, PeerChat, ChatFull

from telethon import functions, types
from telethon.utils import get_display_name
import pandas as pd
import time
import asyncio
import socks
from contextlib import suppress
import traceback
import datetime
import csv
import os

log = logging.getLogger(__name__)
format = '%(asctime)s %(levelname)s:%(message)s'
logging.basicConfig(format=format, level=logging.INFO)

count_succes = 0   #Счетчик успешных

api_id = 510183                  # API ID (получается при регистрации приложения на my.telegram.org)
api_hash = "deafc7e8b314702bdefc032177ad74c9"              # API Hash (оттуда же)



distribution_list = []

is_last = True

time_sleep = 5
error_documents = []
queue_entity = asyncio.Queue() #Очередь для нераспределенных entity
queue_entity_last = asyncio.Queue( ) #Очередь для прошлых entity
numbers = []
count_thread = os.cpu_count()   #Количество потоков = равное количество потоков я у вашего процессора
filename_excel = "project_for_export_no_formulas_31_10_18.xlsx"   #Названия файла с entity
filename_numbers = "number.txt" #Название файла с номерами

proxy = (socks.SOCKS5, '185.142.96.117', 65234, True, 'Artyombubnoff', 'S4a9KkC')   #Прокси, можно использовать HTTHP, HTTPS

error = []
client_dict = {} #Словарь
clients = []
db_bd = "telegram"
db_user = "root"
db_password = ""
db_host = "localhost"

# queue_entity.put_nowait("https://t.me/meetluna")
# queue_entity.put_nowait("https://t.me/cryptogovno")
# queue_entity.put_nowait("https://t.me/BlockchainSchoolRu")
# queue_entity.put_nowait("https://t.me/Adabsolutions")
# queue_entity.put_nowait("https://t.me/cryptocritique")
# queue_entity.put_nowait("https://t.me/joinchat/HnVrsRcPBy7G9evnpCCC7g")
# queue_entity.put_nowait("https://t.me/joinchat/CCWVlhdLX6TDaCbQJN2_eA")
# queue_entity.put_nowait("https://t.me/cryptocritique")

from mysql.connector import MySQLConnection, Error


type_actions = []

def load_numbers(filename):
    """
    Загрузка номеров телеграмм аккаунтов из текстового файла
    :param filename:
    :return:
    """
    with open(filename, "r") as file:
        content = file.read().split("\n")
        for conten in content:
            numbers.append(conten)

def load_excel(output_filename):
    """Метод для загрузки entity из excel"""
    phone_list = []
    list_entity = []
    list_last_entity = []
    #Читаем Excel
    data = pd.read_excel(output_filename, 'Projects', dtype=str)
    for item in data["Telegram link"]:
        if item != "nan":
            #Добавляем необходимые entity в список
            list_entity.append(item)

    #Получаем список, уже распределенных элементов (entity, phone)
    list_last = read_distribution()
    #Проходимся по списку элементов
    for last in list_last:

        #Разбираем элемент
        block = last.split(";")
        entity = block[0]
        phone = block[1]
        # Добавляем элемент в очередь
        queue_entity_last.put_nowait([entity, phone])


        #Добавляем в список телефонов
        phone_list.append(phone)
        #Добавляем в список entity, которые уже были распределены
        list_last_entity.append(entity)
    #Определяем разницу между всеми, и уже распределенными
    difference = list(set(list_entity) - set(list_last_entity))
    #Разница, которую заносим в очередь не распределенных
    for diff in difference:
        queue_entity.put_nowait(diff)
    #Убираем дубилкаты номеров
    new_phone_list = list(set(phone_list))
    #Создаем клиенты
    for phone in new_phone_list:
        #Создаем клиент
        create_client(phone)





def select_database(sql, data):
    """
    Метод для выполнения select запроса к СУБД
    :param sql:
    :param data:
    :return:
    """
    try:
        conn = MySQLConnection(user = db_user, password = db_password, host = db_host, database = db_bd)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql, data)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Error as e:
        print("Ошибка select:", e)

def update_database(sql, data):
    """
    Метод для выполнения update запроса к СУБД
    :param sql:
    :param data:
    :return:
    """
    try:
        conn = MySQLConnection(user = db_user, password = db_password, host = db_host, database = db_bd)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql, data)
        lastrowid = cursor.lastrowid
        cursor.close()
        conn.close()
        return lastrowid
    except Error as e:
        print("Ошибка update:", e)

def insert_database(sql, data):
    """
    Метод для выполнения insert запроса к СУБД
    :param sql:
    :param data:
    :return:
    """
    try:
        conn = MySQLConnection(user = db_user, password = db_password, host = db_host, database = db_bd)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql, data)
        lastrowid = cursor.lastrowid
        cursor.close()
        conn.close()
        return lastrowid
    except Error as e:
        print("Ошибка insert", e)

async def add_entity_db(entity):
    """
    Добавление entity в БД
    :param entity:
    :return:
    """
    sql = "SELECT * FROM entity WHERE name = %s LIMIT 1"
    data = (entity["name"],)
    print(data)
    rows = select_database(sql, data)
    #Если entity существует, обновляем
    if (len(rows) == 1):
        sql = "UPDATE entity SET count_subscribers = %s, description = %s, count_photos =%s, count_videos = %s, " \
              "count_audio = %s, count_shared_links = %s, count_voice =%s, pinned_message_id =%s, type = %s WHERE name = %s"
        data = (entity["count_subscribers"], entity["description"], entity["count_photos"], entity["count_videos"],
                entity["count_audio"], entity["count_shared_links"], entity["count_voice"], entity["pinned_message_id"],
                entity["type"], entity["name"])
        new_rows = update_database(sql, data)
        if (new_rows is 0):
            return rows[0][0]
        return new_rows

    #Иначе добавляем
    else:
        sql = "INSERT INTO entity(address, name, count_subscribers, description, count_photos, count_videos, count_audio," \
              "count_shared_links, count_voice, type, pinned_message_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        data = (entity["address"], entity["name"], entity["count_subscribers"], entity["description"], entity["count_photos"],
                entity["count_videos"], entity["count_audio"], entity["count_shared_links"], entity["count_voice"],
                entity["type"], entity["pinned_message_id"])

        return insert_database(sql, data)

async def add_message_db(message, id_entity):
    """
    Добавление сообщение в БД
    :param message:
    :param id_entity:
    :return:
    """
    sql = "SELECT id FROM message WHERE id_entity = %s and username = %s and post_date = %s and message_time = %s LIMIT 1"
    data = (id_entity, message["username"], message["post_date"], message["message_time"])
    print(data)
    try:
        rows = select_database(sql, data)
    except Exception as e:
        print("Ошибка 1", e)
    # Если entity существует, обновляем
    if (len(rows) == 1):
        sql = "UPDATE message SET message_type = %s, message = %s, shared_link = %s WHERE id_entity = %s and username = %s and post_date = %s and message_time = %s"
        data = (message["message_type"], message["message"], message["shared_link"], id_entity, message["username"],
                message["post_date"], message["message_time"])
        print(data)
        try:
            new_rows = update_database(sql, data)
        except Exception as e:
            print("Ошибка 2", e)
        if (new_rows is 0):
            return rows[0][0]
        return new_rows

    # Иначе добавляем
    else:
        sql = "INSERT INTO message(id_entity, username, post_date, message_time, message_type, message, shared_link) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        data = (id_entity, message["username"], message["post_date"], message["message_time"],
                message["message_type"], message["message"], message["shared_link"])
        try:
            return insert_database(sql, data)
        except Exception as e:
            print("Ошибка 3", e)

async def update_count_db(counts, id_entity):
    """
    Метод для обновления счетчика у entity
    :param counts:
    :param id_entity:
    :return:
    """
    sql = "UPDATE entity SET count_photos = %s, count_videos = %s, count_audio = %s, count_shared_links =%s, count_voice = %s WHERE id = %s"
    data = (counts["count_photos"], counts["count_videos"], counts["count_audio"], counts["count_shared_links"], counts["count_voice"], id_entity)
    print(data)
    try:
        update_database(sql, data)
    except Exception as e:
        print("Ошибка insert count", e)

def create_client(number):
    """
    Создание telegram клиента по номеру
    :param number:
    :return:
    """
    global clients
    try:
        print("Создание подключения по номеру:", str(number), ". Если запросит ввести номер, то введите его, а потом введите номер кода")
        client = [TelegramClient(str(number), api_id, api_hash, proxy=proxy).start(), number]
        clients.append(client)
        client_dict[number] = client

    except errors.PhoneNumberBannedError:
        print("Аккаунт "+str(number)+ " забанен")
    except errors.FloodWaitError as e:
        print("Аккаунт не сможет выполнять некоторое время действия:", e , "Возможно вы уже вошли в парсере с этого аккаунта")
    except errors.PhoneNumberInvalidError as e:
        print("Не валидный номер.", e)


async def parse_entity(entity, url, client, phone):
    """
    Метод разбора entity
    :param entity:
    :param url:
    :param client:
    :param phone:
    :return:
    """
    temp = []
    type = None
    # print(entity.stringify())

    try:
        type_text = entity.chats[0].megagroup
    except:
        type = "Чат"
        type_text = None
    if(type_text is True):
        type = "Группа"
    elif(type_text is False):
        type = "Канал"
    print("Тип:", type)
    try:
        name = entity.chats[0].title
    except:
        try:
            name = entity.chats[0].username
        except:
            name = None
    print("Имя:", name)
    temp.append(["name", name])

    temp.append(["type", type])
    try:
        description = entity.full_chat.about
    except:
        try:
            description = entity.chats[0].about
        except:
            description = None
    print("description:", description)
    temp.append(["description", description])

    address = url
    print("address:", address)
    temp.append(["address", address])

    # id = entity.full_chat.id
    # print("ID:", id)
    # temp.append(["description", description])
    try:
        count_subscribers = int(entity.full_chat.participants_count)
    except:
        try:
            count_subscribers = int(entity.chats[0].participants_count)
        except:
            count_subscribers = None
    print("Количество подписчиков:", count_subscribers)
    temp.append(["count_subscribers", count_subscribers])
    try:
        pinned_message_id = entity.full_chat.pinned_msg_id
    except:
        try:
            pinned_message_id = entity.chats[0].pinned_msg_id
        except:
            pinned_message_id = None
    print("Pinned message id:", pinned_message_id)
    temp.append(["pinned_message_id", pinned_message_id])

    temp.append(["count_photos", None])
    temp.append(["count_videos", None])
    temp.append(["count_audio", None])
    temp.append(["count_shared_links", None])
    temp.append(["count_voice", None])

    entity_dict = dict(temp)
    try:
        id_entity = await add_entity_db(entity_dict)

        if id_entity is not None:
            #Получаем сообщения
            await get_messages(url, type, id_entity, client)
        return id_entity
    except Exception as e:
        errors.append(e)
        print(e)
        exit(-8)





async def get_messages(entity, type_entity, id_entity, client):
    """
    Метод для получения ссобщений
    :param entity:
    :param type_entity:
    :param id_entity:
    :param client:
    :return:
    """
    count_photo = 0
    count_video = 0
    count_shared_link = 0
    count_files = 0
    count_audio = 0
    count_voice = 0
    messages = await client.get_messages(entity, limit=None)

    for message in messages:
        shared_link_use = True
        temp = []
        message_type = None
        if((type_entity == "Группа") or (type_entity == "Чат")):
            message_type = 1
        elif(type_entity == "Канал"):
            message_type = 4

        print(message.stringify())

        message_text = message.message


        post_date = str(message.date.date())
        print("Дата:", post_date)
        temp.append(["post_date", post_date])

        message_time = str(message.date.time())
        print("Время публикации:", message_time)
        temp.append(["message_time", message_time])


        action = message.action
        if(action is not None):
            message_type = 5
            if(type(action) == types.MessageActionChannelCreate):
                action = "Создание канала"
                message_text = action
            elif (type(action) == types.MessageActionChatCreate):
                action = "Создание чата"
                message_text = action
            elif(type(action) == types.MessageActionChatAddUser):
                # users = action.users
                # for user in users:
                #     print("Users:", get_display_name(user))
                if(type_entity == "Группа"):
                    action = "Вступление в группу"
                elif(type_entity == "Канал"):
                    action = "Вступление в канал"
                elif(type_entity == "Чат"):
                    action = "Вступление в чат"
                message_text = action
            elif(type(action) == types.MessageActionChatEditTitle):
                action = "Был изменен заголовок чата"
                message_text = action
            elif(type(action) == types.MessageActionChatEditPhoto):
                action = "Было изменено изображение чата"
                message_text = action
            elif(type(action) == types.MessageActionChatDeleteUser):
                if (type_entity == "Группа"):
                    action = "Пользователь покинул группу"
                elif (type_entity == "Канал"):
                    action = "Пользователь покинул канал"
                elif (type_entity == "Чат"):
                    action = "Пользователь покинул чат"
                message_text = action
            elif(type(action) == types.MessageActionChatJoinedByLink):
                action = "Присоединение к чату по ссылке"
                message_text = action
            elif(type(action) == types.MessageActionChannelMigrateFrom):
                action = "Migrate Channel"
                message_text = action
            elif(type(action) == types.MessageActionCustomAction):
                action = "Пользовательское действие"
                message_text = action
            elif(type(action) == types.MessageActionGameScore):
                action = "Что-то связанное с рекордом игры"
                message_text = action
            elif(type(action) == types.MessageActionPinMessage):
                action = "Прикрепленно сообщение"
                message_text = action

            else:
                type_actions.append(type(action))


        print("Action", action)


        sender = get_display_name(message.sender)
        print("Отправитель:", sender)
        if(sender == ""):
            print("Отсутствует отправитель")

        temp.append(["username", sender])

        try:
            message_photo = message.media.photo
            if (message_text == ""):
                message_text = "Картинка"
            count_photo+= 1
        except:
            print("Изображение не найдено")

        try:
            message_document_type = message.media.document.mime_type
            if(message_document_type.find("video")!= -1):
                if(message_text == ""):
                    message_text = "Видео"
                count_video+= 1
            elif(message_document_type.find("text")!= -1):
                if (message_text == ""):
                    message_text = "Файл"
                count_files+= 1
            elif(message_document_type.find("audio")!= -1):
                for attributes in message.media.document.attributes:
                    if(type(attributes) == types.DocumentAttributeAudio):
                        if(attributes.voice is False):
                            if (message_text == ""):
                                message_text = "Музыка"
                            count_audio += 1
                        elif(attributes.voice is True):
                            if (message_text == ""):
                                message_text = "Звуковое сообщение"
                            count_voice += 1
                        break
            elif(message_document_type.find("pdf")!= -1):
                if (message_text == ""):
                    message_text = "Файл"
                count_files+= 1
            elif(message_document_type.find("image")!= -1):
                count_photo+= 1
            elif(message_document_type.find("application") != -1):
                count_files += 1
            else:
                error_documents.append(message_document_type)

        except:
            print("Изображение не найдено")
        # try:
        #     entities = message.entities
        #     for ent in entities:
        #         if(type(ent) == types.MessageEntityUrl):
        #             count_shared_link+= 1
        #             shared_link_use = False
        #         print(type(ent))
        #         print("TEST ENTITY")
        # except:
        #     entities = None

        try:
            shared_link = message.media.webpage.url
            if (shared_link_use):
                count_shared_link += 1
        except Exception as e:
            shared_link = None

        print("Сообщение:", message_text)
        temp.append(["message", message_text])

        print("Shared link:", shared_link)
        temp.append(["shared_link", shared_link])

        reply_to_msg_id = message.reply_to_msg_id
        print("REPLY:", reply_to_msg_id)
        if reply_to_msg_id is not None:
            if (type_entity == "Группа"):
                message_type = 2

        print("Message type:", message_type)
        temp.append(["message_type", message_type])




        message_dict = dict(temp)
        #Добавление в бд
        id_message = await add_message_db(message_dict, id_entity)
        print("ID Message:", id_message)

    #Счетчики, не все правильно считают, будем разбираться
    temp_counts = []
    print("Количество фото:", count_photo)
    print("Количество видео:", count_video)
    print("Количество ссылок:", count_shared_link)
    print("Количество файлов:", count_files)
    print("Количество audio:", count_audio)
    print("Количество voice:", count_voice)

    temp_counts.append(["count_photos", count_photo])
    temp_counts.append(["count_videos", count_video])
    temp_counts.append(["count_audio", count_audio])
    temp_counts.append(["count_shared_links", count_shared_link])
    temp_counts.append(["count_voice", count_voice])

    entity_count_dict = dict(temp_counts)

    #Обновляем счетчики БД
    await update_count_db(entity_count_dict, id_entity)


async def get_entity(entity, client_main):
    """
    Метод получения ответа о entity
    :param entity: Название entity
    :param client_main: список: клиент, и его номер
    :return:
    """
    client = client_main[0]
    phone = client_main[1]
    result = None
    try:
        result = await client(functions.channels.GetFullChannelRequest(
                channel=entity
            ))
    except TypeError:
        try:
            result = await client(functions.users.GetFullUserRequest(
                id=entity
            ))
        except errors.BadMessageError as e:
            print("Что-то с сообщением", e, client)
        except TypeError:
            try:
                result = await client(functions.messages.GetFullChatRequest(
                    chat_id=entity
                ))
            except errors.BadMessageError as e:
                print("Что-то с сообщением", e, client)
            except Exception as e:
                print("Какая-то ошибка", e)

    except errors.UsernameInvalidError:
        print("Не найден пользователь, канал или чат")
    except errors.InviteHashExpiredError:
        print("Чата больше нет")
    except errors.InviteHashInvalidError:
        print("Ссылка приглашения не валидна")
    except ValueError as e:
        print("Невозможно получить entity.", e)
        try:
            print("Попытка вступить в чат/группу/канал")
            result = await client(functions.users.GetFullUserRequest(
                id=entity
            ))
            print("Попытка удалась")
            await get_entity(entity,client)
        except Exception as e:
            print("Попытка не удалась.",e)

    except errors.FloodWaitError as e:
        print("Ожидание", e)
    except errors.BadMessageError as e:
        print("Что-то с сообщением",e, client)
    if(result is not None):
        global count_succes
        count_succes += 1
        #Заносим в список распределения
        distribution_list.append([entity+";"+ phone])
        #Выполняем разбор result
        id_entity = await parse_entity(result, entity, client, phone)

async def crawl(future):
    """
    Паук нераспределенных entity
    :param future:
    :return:
    """
    futures = []
    clients = await future
    #Пока очередь из entity не пуста
    while queue_entity.qsize() > 0:
        #Распределяем задачи примерно поровну на все аккаунты
        for client in clients:
            #Защита от того, что более позднему аккаунту не достанется задач
            if(queue_entity.qsize() > 0):
                print("Тест:", client[0], client[1])
                futures.append(asyncio.ensure_future(get_entity(queue_entity.get_nowait(), client)))
    if futures:
        await asyncio.wait(futures)

async def crawl_last(text):
    """
    Паук распределенных entity
    :param text:
    :return:
    """
    futures = []
    test = await text #Заглушка
    # Пока очередь из entity не пуста
    while queue_entity_last.qsize() > 0:
        parameters = queue_entity_last.get_nowait()
        entity = parameters[0]
        phone = parameters[1]
        futures.append(asyncio.ensure_future(get_entity(entity, client_dict[phone])))
    if futures:
        await asyncio.wait(futures)

async def start_main(root):
    """Запуск паука нераспределенных задач"""
    loop = asyncio.get_event_loop()
    initial_future = loop.create_future()
    initial_future.set_result(root)
    await crawl(initial_future)

async def start_last(text):
    """Запуск паука распределенных задач"""
    loop = asyncio.get_event_loop()
    initial_future = loop.create_future()
    initial_future.set_result(text)

    await crawl_last(initial_future)




def save_disribution():
    """
    Функция для сохранения распределения entity по клиентам телеграм, для дальнейшего использования
    :return:
    """
    with open("distribution.txt", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(distribution_list)



def read_distribution():
    """
    Функция для чтения распределения entity по клиентам телеграм
    :return:
    """
    list_entity = []
    try:
        with open("distribution.txt", "r", newline="") as file:
            reader = csv.reader(file)
            for row in reader:
                list_entity.append(row[0])
    except:
        print("Файл не создан")
    return list_entity


if __name__ == '__main__':
    start = time.time()
    load_numbers(filename_numbers) #Загрузка телефонов
    if(queue_entity.qsize()==0):
        load_excel(filename_excel)     #Загрузка Excel

    #Запуск паука для прошлых (уже распределенных entity)
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    try:
        loop.run_until_complete(start_last("test_last"))
    except KeyboardInterrupt:
        for task in asyncio.Task.all_tasks():
            task.cancel()
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)

    #Проверяем есть ли из загруженных телефонов, те телефоны, по которым не было создано telegram - клиента в пауке распределенных entity

    for number in numbers:
        #Проверяем есть ли уже созданные клиенты:
        try:
            print("Такой client уже есть:", client_dict[number])
        except KeyError:
            #Нету, значит создаем
            create_client(number)

    # Запускаем loop для нераспределенных entity
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    try:
        loop.run_until_complete(start_main(clients))
    except KeyboardInterrupt:
        for task in asyncio.Task.all_tasks():
            task.cancel()
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)

    #Сохраняем результаты распределения всех
    save_disribution()


    #Временно --------------------------------------------------------
    #Избавления от дубликатов action, которые вообще есть в библиотеке
    new_type_actions = list(set(type_actions))
    for i in new_type_actions:
        print("Необработанные type_actions", i)

    # Временно --------------------------------------------------------
    # Избавления от дубликатов типов документов, которые необработаны в парсере
    new_documents_type = list(set(error_documents))
    for new in new_documents_type:
        print("Необработанные документы", new)

    print("Время парсинга:", time.time() - start)

