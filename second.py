#!/usr/bin/env python3
import json
import os

import requests
import openpyxl
import yaml
import pandas as pd
import first

BASE_DIR = os.path.normpath(os.getcwd())
SECOND_SHEET = "Все Данные с API"
MAX_REQUEST_PER_KEY = 99


def need_to_find_inn(table_height: int, ws):
    return str(ws['A' + str(table_height)].value) in ("1", "True")


def main() -> None:
    config = yaml.load(
        stream=open(
            file=os.path.join(BASE_DIR, 'config.yml'),
            mode='r',
            encoding='utf-8'
        ),
        Loader=yaml.Loader
    )
    # out_inn.xlsx file
    file_name = config['out_inn_file']['xlsx_inn_write_file']
    # out.xlsx file
    xlsx_load_file = config['out_file']['xlsx_write_file']

    # load inn from 'xlsx_write_file'
    companies_inn = load_inn(xlsx_load_file, only_matched_city=False)

    url = 'https://api.checko.ru/v2/company?key={key}&inn={inn}&active=true'

    workbook = openpyxl.load_workbook(xlsx_load_file)
    worksheet = workbook[SECOND_SHEET]

    api_keys = config['api_keys']
    # Номер строки после последней не пустой строки
    # table_height = get_column_height(path=xlsx_load_file, sheet_name=worksheet.title)
    # if table_height > 2:
    #     print(f"Вы остановились на строке: № {table_height - 1} обработка пойдет с этого места")
    table_height = 2

    # Начало цикла
    while True:
        inn = companies_inn[table_height - 2]
        attempts = 0
        successful = False

        if table_height > len(companies_inn):
            print(f'{table_height} row: OK')
            return
        if not need_to_find_inn(table_height, worksheet):
            print(f"{table_height}: ИНН '{inn}' не помечен на дальнейший отбор. Проверяю далее")
            table_height += 1
            continue
        print(f"{table_height}: ИНН '{inn}' помечен на дальнейший отбор. Запрашиваю данные")

        successful, table_height = try_getting_data(api_keys, attempts, file_name, inn, successful, table_height, url,
                                                    workbook)

        if not successful:
            print(f'{table_height}: Ignore company with the INN: "{inn}".'
                  f' Data from api is corrupted or All keys were used!')
            table_height += 1


def try_getting_data(api_keys, attempts, file_name, inn, successful, table_height, url, workbook):
    while attempts <= len(api_keys):
        key = api_keys[0 + attempts]

        print(f'{table_height + 1}: request to api with key: "{key}", inn: "{inn}"')
        data, err = request_to_api(url, key, inn)
        attempts += 1

        if not err:
            successful = True
            table_height = write_data(api_keys, data, file_name, inn, key, successful, table_height,
                                      workbook)
            break
        else:
            print(f'{table_height} (Attempt #{attempts}) with key: "{key}" - Fail')
    return successful, table_height


def write_data(api_keys, data, file_name, inn, key, table_height, workbook):
    print(f'{table_height + 1}: successful request to api with key: "{key}, inn: "{inn}"!')
    cur_key_valid = write_json(BASE_DIR, file_name, data, inn, table_height, workbook)
    table_height += 1
    validate_key(api_keys, cur_key_valid)
    return table_height


def validate_key(api_keys, cur_key_valid):
    if not cur_key_valid:
        print(f"Key {api_keys[0]} is fully used and deleted from stack.")
        api_keys.pop(0)


def request_to_api(url: str, key: str, inn: str) -> tuple[list[dict], bool]:
    response = requests.get(
        url=url.format(key=key, inn=inn),
    )
    if response.status_code == 200:
        data = response.json()
        return data, False
    else:
        return {}, True


def get_column_height(path, sheet_name):
    excel_table = pd.read_excel(path, sheet_name)
    company_columns = ['УпрОрг ИНН', 'УпрОрг НаимПолн', 'УстКап Сумма', 'Руковод', 'Контакты Телефон',
                       'Контакты email', 'Контакты ВебСайт', 'СЧР', 'this_key_requests']
    company_data = excel_table[company_columns]
    if company_data.last_valid_index():
        return company_data.last_valid_index() + 1
    else:
        return 2


def write_json(data: dict, series) -> bool:
    data_str = json.dumps(data)
    series.append(data_str)

    return series


def load_inn(xlsx_path: str, only_matched_city=False) -> list[str]:
    companies_inn = []
    workbook = openpyxl.load_workbook(xlsx_path)
    worksheet = workbook[SECOND_SHEET]
    table_height = first.get_table_height(xlsx_path, worksheet.title)
    for i in range(2, table_height):
        if only_matched_city:
            if str(worksheet['B' + str(i)].value).lower() in ("false", "0", "нет"):
                continue

        company_inn = str(worksheet['E' + str(i)].value)
        if company_inn not in ['0', None, 'None']:
            companies_inn.append(company_inn)
    return companies_inn


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
