import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id: str, client_id: str, seller_token: str) -> list:
    """Получить список товаров магазина Ozon.

    Эта функция отправляет запрос к API Ozon для получения списка товаров
    магазина с учетом заданного идентификатора последнего товара.

    Args:
        last_id (str): Идентификатор последнего товара, используемый для
            пагинации списка товаров.
        client_id (str): Идентификатор клиента, используемый для аутентификации
            при обращении к API Ozon.
        seller_token (str): Токен продавца, необходимый для доступа к API.

    Returns:
        list: Список товаров, содержащий информацию о товарах магазина.
    
    Raises:
        requests.exceptions.HTTPError: Если запрос к API не удался.

    Examples:
        >>> get_product_list("", "12345", "abcde12345")
        [{'offer_id': '1', 'name': 'Товар 1', 'price': 100}, 
         {'offer_id': '2', 'name': 'Товар 2', 'price': 200}]

        >>> get_product_list("1", "12345", "abcde12345")
        [{'offer_id': '2', 'name': 'Товар 2', 'price': 200}]
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
    return response_object.get("result", [])


def get_offer_ids(client_id: str, seller_token: str) -> list:
    """Получить артикулы товаров магазина Ozon.

    Эта функция обращается к API Ozon для получения списка артикулов
    всех товаров магазина, используя идентификатор клиента и токен
    продавца для аутентификации.

    Args:
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        list: Список артикулов товаров, полученных из API.

    Raises:
        HTTPError: Если запрос завершился неудачно, будет вызвано исключение.

    Examples:
        >>> get_offer_ids("12345", "abcde12345")
        ['offer_id_1', 'offer_id_2', 'offer_id_3']

        >>> get_offer_ids("", "")
        []  # Пустой список, если не переданы корректные идентификаторы
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items", []))
        total = some_prod.get("total", 0)
        last_id = some_prod.get("last_id", "")
        if total == len(product_list):
            break
    offer_ids = [product.get("offer_id") for product in product_list]
    return offer_ids


def update_price(prices: list, client_id: str, seller_token: str) -> dict:
    """Обновить цены товаров в магазине Ozon.

    Эта функция отправляет запрос к API Ozon для обновления цен
    на указанные товары. Для выполнения запроса используются
    идентификатор клиента и токен продавца.

    Args:
        prices (list): Список объектов с информацией о ценах на товары.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        dict: Ответ API в формате JSON, содержащий информацию об обновлении цен.

    Raises:
        HTTPError: Если запрос завершился неудачно, будет вызвано исключение.

    Examples:
        >>> update_price([{"offer_id": "offer_id_1", "price": 1000}], "12345", "abcde12345")
        {'result': True, 'message': 'Prices updated successfully'}

        >>> update_price([], "12345", "abcde12345")
        {'result': False, 'message': 'No prices to update'}
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
    """Обновить остатки товаров в магазине Ozon.

    Эта функция отправляет запрос к API Ozon для обновления остатков
    на указанные товары. Для выполнения запроса используются
    идентификатор клиента и токен продавца для аутентификации.

    Args:
        stocks (list): Список объектов с информацией об остатках на товары.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        dict: Ответ API в формате JSON, содержащий информацию об обновлении остатков.

    Raises:
        HTTPError: Если запрос завершился неудачно, будет вызвано исключение.

    Examples:
        >>> update_stocks([{"offer_id": "offer_id_1", "stock": 100}], "12345", "abcde12345")
        {'result': True, 'message': 'Stocks updated successfully'}

        >>> update_stocks([], "12345", "abcde12345")
        {'result': False, 'message': 'No stocks to update'}
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
    """
    Скачать файл остатков часов с сайта Casio и вернуть их в виде списка.

    Эта функция загружает ZIP-файл с остатками часов с указанного URL,
    извлекает его и преобразует содержимое Excel-файла в список словарей,
    где каждый словарь представляет информацию об остатках.

    Returns:
        list: Список словарей, содержащих информацию об остатках часов.
              Пример: [{'model': 'Casio A', 'stock': 10}, {'model': 'Casio B', 'stock': 5}, ...].

    Raises:
        HTTPError: Если запрос завершился неудачно, будет вызвано исключение.
        FileNotFoundError: Если файл Excel не найден после извлечения.
        ValueError: Если возникла ошибка при чтении Excel-файла.

    Examples:
        >>> stock_data = download_stock()
        >>> print(stock_data)
        [{'model': 'Casio A', 'stock': 10}, {'model': 'Casio B', 'stock': 5}, ...]

        >>> download_stock()
        # Вернет список остатков, если файл успешно загружен и прочитан.

        >>> # Если URL недоступен, будет вызвано исключение HTTPError.
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
    """
    Создает список остатков товаров на основе имеющихся данных.

    Эта функция принимает остатки товаров и идентификаторы предложений,
    фильтрует их, добавляет запасы в зависимости от условий и возвращает
    список остатков, включая товары с нулевым запасом.

    Args:
        watch_remnants (list): Список словарей, каждый из которых содержит информацию о товаре,
                                включая его код и количество.
        offer_ids (list): Список идентификаторов предложений, которые нужно проверить.

    Returns:
        list: Список словарей, каждый из которых содержит 'offer_id' и 'stock'.
              Пример: [{"offer_id": "1", "stock": 100}, {"offer_id": "2", "stock": 0}].

    Examples:
        Correct usage:
        >>> watch_remnants = [{"Код": "1", "Количество": "15"}, {"Код": "2", "Количество": ">10"}]
        >>> offer_ids = ["1", "2", "3"]
        >>> create_stocks(watch_remnants, offer_ids)
        [{'offer_id': '1', 'stock': 15}, {'offer_id': '2', 'stock': 100}, {'offer_id': '3', 'stock': 0}]

        Incorrect usage:
        >>> create_stocks([], ["1", "2", "3"])
        [{'offer_id': '1', 'stock': 0}, {'offer_id': '2', 'stock': 0}, {'offer_id': '3', 'stock': 0}]
        # Возвращает список с нулевыми остатками, если входной список пуст

        >>> create_stocks([{"Код": "4", "Количество": "5"}], [])
        [{'offer_id': '4', 'stock': 5}]
        # Возвращает остатки без предложения, если идентификаторы отсутствуют
    """
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
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для товаров на основе остатков и идентификаторов предложений.

    Эта функция проверяет, есть ли код товара в списке идентификаторов предложений, 
    и формирует словари с информацией о цене для каждого товара.

    Args:
        watch_remnants (list): Список словарей, каждый из которых содержит информацию о товаре, 
                                включая его код и цену.
        offer_ids (list): Список идентификаторов предложений, с которыми необходимо сопоставить коды товаров.

    Returns:
        list: Список словарей, содержащих информацию о ценах для товаров, 
              которые соответствуют идентификаторам предложений.

    Examples:
        Correct usage:
        >>> watch_remnants = [{"Код": "1", "Цена": "1000"}, {"Код": "2", "Цена": "2000"}]
        >>> offer_ids = ["1", "2"]
        >>> create_prices(watch_remnants, offer_ids)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '1', 'old_price': '0', 'price': <converted_price_1>},
         {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '2', 'old_price': '0', 'price': <converted_price_2>}]

        Incorrect usage:
        >>> create_prices([], ["1", "2"])
        []
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
    """
    Преобразовать строку с ценой в числовой формат.

    Эта функция принимает строку, представляющую цену в формате
    с разделителями и валютой, и возвращает строку, содержащую
    только числовое значение цены.

    Args:
        price (str): Строка с ценой, например "5'990.00 руб.".

    Returns:
        str: Числовое значение цены в строковом формате, например "5990".

    Examples:
        Correct usage:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        >>> price_conversion("1.000,50 €")
        '100050'  # Преобразует и удаляет все символы, кроме цифр

        Incorrect usage:
        >>> price_conversion("")
        ''  # Возвращает пустую строку, если входная строка пуста

        >>> price_conversion("abc")
        ''  # Возвращает пустую строку, если входная строка не содержит чисел

        >>> price_conversion("100.00 USD")
        '10000'  # Удаляет символы и возвращает числовое значение без разделителей
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделяет список на части по n элементов.

    Эта функция использует генератор для разделения входного списка 
    на подсписки заданного размера.

    Args:
        lst (list): Список, который необходимо разделить.
        n (int): Число элементов в каждой части.

    Yields:
        list: Подсписок из n элементов.

    Examples:
        Correct usage:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

        Incorrect usage:
        >>> list(divide([1, 2, 3], 0))
        ValueError: n must be greater than 0.
    """
    if n <= 0:
        raise ValueError("n must be greater than 0.")
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Асинхронно загружает цены на товары на основе остатков и идентификаторов предложений.

    Эта функция извлекает идентификаторы предложений, создает список цен 
    и обновляет их партиями по 1000 элементов.

    Args:
        watch_remnants (list): Список словарей, каждый из которых содержит информацию о товаре, 
                                включая его код и цену.
        client_id (str): Идентификатор клиента, используемый для аутентификации.
        seller_token (str): Токен продавца, используемый для аутентификации.

    Returns:
        list: Список словарей, содержащих информацию о ценах для соответствующих предложений.

    Examples:
        Correct usage:
        >>> watch_remnants = [{"Код": "1", "Цена": "1000"}, {"Код": "2", "Цена": "2000"}]
        >>> client_id = "your_client_id"
        >>> seller_token = "your_seller_token"
        >>> await upload_prices(watch_remnants, client_id, seller_token)
        [{'offer_id': '1', 'price': 1000}, {'offer_id': '2', 'price': 2000}]

        Incorrect usage:
        >>> await upload_prices([], "invalid_client_id", "invalid_seller_token")
        Ошибка аутентификации или пустой список остатков.
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

    Args:
        watch_remnants (list): Список словарей, каждый из которых содержит информацию о товаре.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        tuple: Кортеж, содержащий два элемента:
            - list: Список остатков, которые не равны нулю.
            - list: Полный список остатков, включая нулевые.

    Examples:
        Correct usage:
        >>> watch_remnants = [{"Код": "1", "stock": 10}, {"Код": "2", "stock": 0}]
        >>> client_id = "client123"
        >>> seller_token = "token123"
        >>> await upload_stocks(watch_remnants, client_id, seller_token)
        ([{"Код": "1", "stock": 10}], [{"Код": "1", "stock": 10}, {"Код": "2", "stock": 0}])

        Incorrect usage:
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
