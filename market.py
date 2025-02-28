import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список продуктов для заданной кампании.

    Аргументы:
        page (str): Токен страницы для пагинации. Укажите пустую строку для получения первой страницы.
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        access_token (str): Токен доступа для авторизации в API Яндекс.Маркета.

    Возвращает:
        list: Список продуктов в формате JSON, содержащий информацию о товарах.

    Примеры:
        Пример корректного исполнения функции:
        >>> get_product_list("", "123456", "your_access_token")
        [{'offerMappingEntries': [...], 'paging': {'nextPageToken': 'token'}}]

        Пример некорректного исполнения функции:
        >>> get_product_list("", "invalid_id", "your_access_token")
        requests.exceptions.HTTPError: 404 Client Error: Not Found for url: https://api.partner.market.yandex.ru/campaigns/invalid_id/offer-mapping-entries
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет остатки товаров в Яндекс.Маркете для заданной кампании.

    Эта функция отправляет запрос на обновление остатков товаров по их SKU в указанной кампании.

    Аргументы:
        stocks (list): Список SKU товаров, для которых необходимо обновить остатки.
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        access_token (str): Токен доступа для авторизации в API Яндекс.Маркета.

    Возвращает:
        dict: Ответ сервера в формате JSON, содержащий информацию об обновленных остатках.

    Примеры:
        Пример корректного исполнения функции:
        >>> update_stocks(["sku1", "sku2"], "123456", "your_access_token")
        {'result': 'success', 'updatedCount': 2}

        Пример некорректного исполнения функции:
        >>> update_stocks([], "123456", "your_access_token")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request for url: https://api.partner.market.yandex.ru/campaigns/123456/offers/stocks
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров в Яндекс.Маркете для заданной кампании.

    Эта функция отправляет запрос на обновление цен товаров по их идентификаторам в указанной кампании.

    Аргументы:
        prices (list): Список цен товаров, которые необходимо обновить. Каждый элемент должен содержать идентификатор товара и новую цену.
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        access_token (str): Токен доступа для авторизации в API Яндекс.Маркета.

    Возвращает:
        dict: Ответ сервера в формате JSON, содержащий информацию об обновленных ценах.

    Примеры:
        Пример корректного исполнения функции:
        >>> update_price([{"offerId": "offer1", "price": 100}, {"offerId": "offer2", "price": 200}], "123456", "your_access_token")
        {'result': 'success', 'updatedCount': 2}

        Пример некорректного исполнения функции:
        >>> update_price([], "123456", "your_access_token")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request for url: https://api.partner.market.yandex.ru/campaigns/123456/offer-prices/updates
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает артикулы товаров Яндекс.Маркета для заданной кампании.

    Эта функция извлекает идентификаторы предложений (артикулы) товаров из Яндекс.Маркета, 
    используя токен доступа и идентификатор кампании.

    Аргументы:
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        market_token (str): Токен доступа для авторизации в API Яндекс.Маркета.

    Возвращает:
        list: Список артикулов (shopSku) товаров в виде строк.

    Примеры:
        Пример корректного исполнения функции:
        >>> get_offer_ids("123456", "your_market_token")
        ['sku1', 'sku2', 'sku3']

        Пример некорректного исполнения функции:
        >>> get_offer_ids("", "your_market_token")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request for url: https://api.partner.market.yandex.ru/campaigns//offers
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries", []))
        page = some_prod.get("paging", {}).get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer", {}).get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создает список остатков товаров для заданного склада.

    Эта функция формирует список остатков товаров на основе предоставленных остатков 
    и идентификаторов предложений, учитывая наличие товаров на складе.

    Аргументы:
        watch_remnants (list): Список остатков товаров, где каждый элемент представляет собой 
                                словарь с кодом и количеством.
        offer_ids (list): Список идентификаторов предложений (артикулов) товаров, доступных 
                          в Яндекс.Маркете.
        warehouse_id (str): Идентификатор склада, для которого создаются остатки.

    Возвращает:
        list: Список остатков товаров, каждый из которых представлен в виде словаря 
              с информацией о SKU, идентификаторе склада и количестве.

    Примеры:
        Пример корректного исполнения функции:
        >>> create_stocks([{"Код": "sku1", "Количество": "5"}, {"Код": "sku2", "Количество": ">10"}], ["sku1", "sku2", "sku3"], "warehouse_1")
        [{'sku': 'sku1', 'warehouseId': 'warehouse_1', 'items': [{'count': 5, 'type': 'FIT', 'updatedAt': '2023-10-01T12:00:00Z'}]}, 
         {'sku': 'sku2', 'warehouseId': 'warehouse_1', 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2023-10-01T12:00:00Z'}]}, 
         {'sku': 'sku3', 'warehouseId': 'warehouse_1', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2023-10-01T12:00:00Z'}]}]

        Пример некорректного исполнения функции:
        >>> create_stocks([], ["sku1"], "warehouse_1")
        [{'sku': 'sku1', 'warehouseId': 'warehouse_1', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2023-10-01T12:00:00Z'}]}]
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для заданных товаров на основе остатков.

    Эта функция формирует список цен товаров, проверяя наличие их идентификаторов 
    в списке предложений и преобразуя цену с помощью функции price_conversion.

    Аргументы:
        watch_remnants (list): Список остатков товаров, где каждый элемент представляет собой 
                                словарь с кодом и ценой.
        offer_ids (list): Список идентификаторов предложений (артикулов) товаров, доступных 
                          в Яндекс.Маркете.

    Возвращает:
        list: Список цен товаров, каждый из которых представлен в виде словаря 
              с информацией о идентификаторе и цене.

    Примеры:
        Пример корректного исполнения функции:
        >>> create_prices([{"Код": "sku1", "Цена": "100.00"}, {"Код": "sku2", "Цена": "200.50"}], ["sku1", "sku2", "sku3"])
        [{'id': 'sku1', 'price': {'value': 100, 'currencyId': 'RUR'}}, 
         {'id': 'sku2', 'price': {'value': 200, 'currencyId': 'RUR'}}]

        Пример некорректного исполнения функции:
        >>> create_prices([], ["sku1"])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    "currencyId": "RUR",
                },
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загружает цены товаров в заданную кампанию.

    Эта асинхронная функция получает идентификаторы предложений для указанной кампании, 
    создает список цен на основе остатков и загружает их партиями.

    Аргументы:
        watch_remnants (list): Список остатков товаров, где каждый элемент представляет собой 
                                словарь с кодом и ценой.
        campaign_id (str): Идентификатор кампании, в которую загружаются цены.
        market_token (str): Токен доступа к API маркетплейса для аутентификации.

    Возвращает:
        list: Список цен товаров, который был загружен в кампанию.

    Примеры:
        Пример корректного исполнения функции:
        >>> await upload_prices([{"Код": "sku1", "Цена": "100.00"}, {"Код": "sku2", "Цена": "200.50"}], "campaign_123", "token_abc")
        [{'id': 'sku1', 'price': {'value': 100, 'currencyId': 'RUR'}}, 
         {'id': 'sku2', 'price': {'value': 200, 'currencyId': 'RUR'}}]

        Пример некорректного исполнения функции:
        >>> await upload_prices([], "campaign_123", "token_abc")
        []
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загружает остатки товаров в заданную кампанию и возвращает список не пустых остатков.

    Эта асинхронная функция получает идентификаторы предложений для указанной кампании, 
    создает список остатков на основе данных о товарах и загружает их партиями.

    Аргументы:
        watch_remnants (list): Список остатков товаров, где каждый элемент представляет собой 
                                словарь с кодом и количеством.
        campaign_id (str): Идентификатор кампании, в которую загружаются остатки.
        market_token (str): Токен доступа к API маркетплейса для аутентификации.
        warehouse_id (str): Идентификатор склада, на который загружаются остатки.

    Возвращает:
        tuple: Кортеж, содержащий два элемента:
            - list: Список остатков товаров, у которых количество больше нуля.
            - list: Полный список остатков товаров, загруженных в кампанию.

    Примеры:
        Пример корректного исполнения функции:
        >>> await upload_stocks([{"Код": "sku1", "Количество": 10}, {"Код": "sku2", "Количество": 0}], "campaign_123", "token_abc", "warehouse_1")
        ([{'id': 'sku1', 'items': [{'count': 10}]}], [{'id': 'sku1', 'items': [{'count': 10}]}, {'id': 'sku2', 'items': [{'count': 0}]}])

        Пример некорректного исполнения функции:
        >>> await upload_stocks([], "campaign_123", "token_abc", "warehouse_1")
        ([], [])
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)


def main():
    """Основная функция для обновления остатков и цен на товары в FBS и DBS кампаниях.

    Эта функция загружает остатки товаров, обновляет их и меняет цены для двух типов кампаний 
    (FBS и DBS) на маркетплейсе, используя соответствующие идентификаторы и токены.

    Аргументы:
        Нет.

    Возвращает:
        None: Эта функция не возвращает значения, но может вызвать исключения в случае ошибок.

    Примеры:
        Пример корректного исполнения функции:
        >>> main()  # Предполагается, что все переменные окружения установлены корректно.
        Остатки и цены успешно обновлены.

        Пример некорректного исполнения функции:
        >>> main()  # Если переменные окружения не установлены или возникла ошибка соединения.
        Превышено время ожидания...
        Ошибка соединения
        ERROR_2
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
