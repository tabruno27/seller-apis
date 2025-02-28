import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина Озон.

    Эта функция отправляет запрос к API Озон для получения списка товаров, 
    начиная с указанного идентификатора товара.

    Аргументы:
        last_id (str): Идентификатор последнего товара, с которого нужно начать выборку.
        client_id (str): Идентификатор клиента для аутентификации в API.
        seller_token (str): Токен продавца для доступа к API.

    Возвращает:
        list: Список товаров магазина. Если товаров нет, возвращает пустой список.

    Raises:
        requests.exceptions.HTTPError: Если ответ API содержит ошибку (например, 4xx или 5xx).

    Примеры:
        Пример корректного исполнения функции:
        >>> products = get_product_list("12345", "your_client_id", "your_seller_token")
        >>> print(products)  # Выводит список товаров.

        Пример некорректного исполнения функции:
        >>> products = get_product_list("12345", "invalid_client_id", "invalid_seller_token")
        >>> print(products)  # Вызывает исключение HTTPError из-за неверных учетных данных.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина Озон.

    Эта функция извлекает все артикулы товаров из магазина Озон, 
    отправляя запросы к API для получения списка продуктов.

    Аргументы:
        client_id (str): Идентификатор клиента для аутентификации в API.
        seller_token (str): Токен продавца для доступа к API.

    Возвращает:
        list: Список артикулов товаров магазина. Если товаров нет, возвращает пустой список.

    Примеры:
        Пример корректного исполнения функции:
        >>> offer_ids = get_offer_ids("your_client_id", "your_seller_token")
        >>> print(offer_ids)  # Выводит список артикулов товаров.

        Пример некорректного исполнения функции:
        >>> offer_ids = get_offer_ids("invalid_client_id", "invalid_seller_token")
        >>> print(offer_ids)  # Может вызвать исключение HTTPError из-за неверных учетных данных.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items", []))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров в магазине Озон.

    Эта функция отправляет обновленные цены для товаров в магазин Озон 
    через API, используя указанные идентификатор клиента и токен продавца.

    Аргументы:
        prices (list): Список словарей, каждый из которых содержит информацию 
                       о товаре и его новой цене. Например:
                       [{'offer_id': '12345', 'price': 1000}, ...].
        client_id (str): Идентификатор клиента для аутентификации в API.
        seller_token (str): Токен продавца для доступа к API.

    Возвращает:
        dict: Ответ API, содержащий информацию об обновлении цен.

    Примеры:
        Пример корректного исполнения функции:
        >>> prices = [{'offer_id': '12345', 'price': 1000}]
        >>> response = update_price(prices, "your_client_id", "your_seller_token")
        >>> print(response)  # Выводит ответ API с информацией об обновлении.

        Пример некорректного исполнения функции:
        >>> prices = [{'offer_id': '12345', 'price': 1000}]
        >>> response = update_price(prices, "invalid_client_id", "invalid_seller_token")
        >>> print(response)  # Вызывает исключение HTTPError из-за неверных учетных данных.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров в магазине Озон.

    Эта функция отправляет обновленные остатки для товаров в магазин Озон 
    через API, используя указанные идентификатор клиента и токен продавца.

    Аргументы:
        stocks (list): Список словарей, каждый из которых содержит информацию 
                       о товаре и его новом остатке. Например:
                       [{'offer_id': '12345', 'stock': 50}, ...].
        client_id (str): Идентификатор клиента для аутентификации в API.
        seller_token (str): Токен продавца для доступа к API.

    Возвращает:
        dict: Ответ API, содержащий информацию об обновлении остатков.

    Примеры:
        Пример корректного исполнения функции:
        >>> stocks = [{'offer_id': '12345', 'stock': 50}]
        >>> response = update_stocks(stocks, "your_client_id", "your_seller_token")
        >>> print(response)  # Выводит ответ API с информацией об обновлении.

        Пример некорректного исполнения функции:
        >>> stocks = [{'offer_id': '12345', 'stock': 50}]
        >>> response = update_stocks(stocks, "invalid_client_id", "invalid_seller_token")
        >>> print(response)  # Может вызвать исключение HTTPError из-за неверных учетных данных.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл остатков с сайта Casio и вернуть данные о часах.

    Эта функция загружает ZIP-файл с остатками часов с указанного сайта, 
    распаковывает его и извлекает данные из Excel-файла, 
    возвращая их в виде списка словарей.

    Возвращает:
        list: Список словарей, где каждый словарь содержит информацию о часах.

    Примеры:
        Пример корректного исполнения функции:
        >>> watch_data = download_stock()
        >>> print(watch_data)  # Выводит список остатков часов.

        Пример некорректного исполнения функции:
        >>> # Если сайт недоступен или файл не существует, 
        >>> # будет вызвано исключение HTTPError.
        >>> watch_data = download_stock()  # Может вызвать исключение.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков для товаров на основе данных о часах.

    Эта функция принимает данные о часах и идентификаторы предложений, 
    фильтрует и создает список остатков, устанавливая количество для 
    каждого товара в зависимости от условий.

    Аргументы:
        watch_remnants (list): Список словарей, содержащих данные о часах. 
                                Каждый словарь должен содержать ключи "Код" 
                                и "Количество".
        offer_ids (list): Список идентификаторов предложений, которые 
                          необходимо проверить и обновить.

    Возвращает:
        list: Список словарей, где каждый словарь содержит идентификатор 
              предложения и соответствующее количество на складе.

    Примеры:
        Пример корректного исполнения функции:
        >>> watch_remnants = [{'Код': '123', 'Количество': '15'}, {'Код': '456', 'Количество': '1'}]
        >>> offer_ids = ['123', '456', '789']
        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> print(stocks)  
        [{'offer_id': '123', 'stock': 100}, {'offer_id': '456', 'stock': 0}, {'offer_id': '789', 'stock': 0}]

        Пример некорректного исполнения функции:
        >>> watch_remnants = []  # Пустой список остатков
        >>> offer_ids = ['123']
        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> print(stocks)  # Выводит [{'offer_id': '123', 'stock': 0}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен для товаров на основе данных о часах.

    Эта функция принимает данные о часах и идентификаторы предложений, 
    создает список цен, устанавливая информацию о цене для каждого товара.

    Аргументы:
        watch_remnants (list): Список словарей, содержащих данные о часах. 
                                Каждый словарь должен содержать ключи "Код" 
                                и "Цена".
        offer_ids (list): Список идентификаторов предложений, которые 
                          необходимо проверить и обновить.

    Возвращает:
        list: Список словарей, где каждый словарь содержит информацию о цене 
              товара, включая идентификатор предложения и валюту.

    Примеры:
        Пример корректного исполнения функции:
        >>> watch_remnants = [{'Код': '123', 'Цена': '1000'}, {'Код': '456', 'Цена': '2000'}]
        >>> offer_ids = ['123', '456']
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)  
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': 1000},
         {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '456', 'old_price': '0', 'price': 2000}]

        Пример некорректного исполнения функции:
        >>> watch_remnants = []  # Пустой список остатков
        >>> offer_ids = ['123']
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)  # Выводит [] (пустой список)
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать строку цены в числовой формат.

    Эта функция принимает строку, представляющую цену, и удаляет все 
    символы, кроме цифр, возвращая только числовое значение. 
    Например, строка "5'990.00 руб." будет преобразована в "5990".

    Аргументы:
        price (str): Строка, представляющая цену, содержащая символы, 
                      которые нужно удалить (например, пробелы, валюту, 
                      разделители тысяч).

    Возвращает:
        str: Строка, содержащая только числовое значение цены.

    Примеры:
        Пример корректного исполнения функции:
        >>> price = "5'990.00 руб."
        >>> converted_price = price_conversion(price)
        >>> print(converted_price)  # Выводит '5990'

        Пример некорректного исполнения функции:
        >>> price = "Цена: 1000 руб."
        >>> converted_price = price_conversion(price)
        >>> print(converted_price)  # Выводит '1000'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список на части по n элементов.

    Эта функция принимает список и делит его на подсписки, каждый из 
    которых содержит не более n элементов. Если длина списка не делится 
    нацело на n, последний подсписок может содержать меньше элементов.

    Аргументы:
        lst (list): Список, который необходимо разделить.
        n (int): Количество элементов в каждом подсписке.

    Возвращает:
        generator: Генератор, который возвращает подсписки, каждый из 
                   которых содержит до n элементов.

    Примеры:
        Пример корректного исполнения функции:
        >>> lst = [1, 2, 3, 4, 5, 6]
        >>> result = list(divide(lst, 2))
        >>> print(result)  # Выводит [[1, 2], [3, 4], [5, 6]]

        Пример некорректного исполнения функции:
        >>> lst = [1, 2, 3]
        >>> result = list(divide(lst, 5))
        >>> print(result)  # Выводит [[1, 2, 3]] (весь список в одном подсписке)
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id: str, seller_token: str):
    """Загрузить цены для предложений продавца.

    Эта асинхронная функция принимает остатки товаров и загружает 
    соответствующие цены в систему, разбивая их на группы по 1000 
    элементов для обновления. Функция также получает идентификаторы 
    предложений на основе переданных идентификатора клиента и токена 
    продавца.

    Аргументы:
        watch_remnants: Остатки товаров, для которых необходимо 
                         обновить цены. Тип зависит от реализации.
        client_id (str): Идентификатор клиента, используемый для 
                         аутентификации.
        seller_token (str): Токен продавца, используемый для 
                            аутентификации.

    Возвращает:
        list: Список обновленных цен для предложений.

    Примеры:
        Пример корректного исполнения функции:
        >>> watch_remnants = [...]  # Остатки товаров
        >>> client_id = "12345"
        >>> seller_token = "token_xyz"
        >>> updated_prices = await upload_prices(watch_remnants, client_id, seller_token)
        >>> print(updated_prices)  # Выводит список обновленных цен

        Пример некорректного исполнения функции:
        >>> watch_remnants = None  # Некорректные остатки
        >>> client_id = "12345"
        >>> seller_token = "token_xyz"
        >>> updated_prices = await upload_prices(watch_remnants, client_id, seller_token)
        # Ожидается ошибка, так как watch_remnants не может быть None
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загружает остатки товаров и обновляет их на стороне сервера.

    Эта асинхронная функция получает остатки товаров, идентификаторы предложений,
    разбивает остатки на части и обновляет их на сервере, возвращая только те
    остатки, которые не равны нулю.

    Аргументы:
        watch_remnants (list): Список словарей, каждый из которых содержит информацию о товаре.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Возвращает:
        tuple: Кортеж, содержащий два элемента:
            - list: Список остатков, которые не равны нулю.
            - list: Полный список остатков, включая нулевые.

    Примеры:
        Пример корректного исполнения функции:
        >>> watch_remnants = [{"Код": "1", "stock": 10}, {"Код": "2", "stock": 0}]
        >>> client_id = "client123"
        >>> seller_token = "token123"
        >>> await upload_stocks(watch_remnants, client_id, seller_token)
        ([{"Код": "1", "stock": 10}], [{"Код": "1", "stock": 10}, {"Код": "2", "stock": 0}])

        Пример некорректного исполнения функции:
        >>> await upload_stocks([], "invalid_client", "invalid_token")
        ([], [])  # Возвращает пустые списки, если остатки товаров отсутствуют
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для обновления остатков и цен товаров.

    Эта функция выполняет следующие действия:
    1. Получает идентификаторы предложений.
    2. Загружает данные о текущих остатках.
    3. Обновляет остатки товаров в Яндекс.Маркете.
    4. Создает новые цены на товары.
    5. Обновляет цены в Яндекс.Маркете.

    Аргументы:
        Нет.

    Возвращает:
        None: Функция не возвращает значения, но обновляет остатки и цены в системе.

    Примеры:
        Пример корректного исполнения функции:
        >>> main()
        Программа успешно обновила остатки и цены.

        Пример некорректного исполнения функции:
        >>> main()
        Превышено время ожидания...
        Ошибка соединения
        ERROR_2
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
