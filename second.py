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
    # create out_inn.xlsx file
    make_xlsx_out_file(
        base_dir=BASE_DIR,
        file_name=file_name,
        fields=config['out_inn_file']['fields'].values()
    )
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
        if table_height > len(companies_inn):
            print(f'{table_height} row: OK')
            return
        inn = companies_inn[table_height - 2]

        if not need_to_find_inn(table_height, worksheet):
            print(f"{table_height}: ИНН '{inn}' не помечен на дальнейший отбор. Проверяю далее")
            table_height += 1
            continue

        print(f"{table_height}: ИНН '{inn}' помечен на дальнейший отбор. Запрашиваю данные")

        attempts = 0
        successful = False

        while attempts <= len(api_keys):
            key = api_keys[0 + attempts]

            print(f'{table_height + 1}: request to api with key: "{key}", inn: "{inn}"')
            data, err = request_to_api(url, key, inn)
            attempts += 1

            if not err:
                print(f'{table_height + 1}: successful request to api with key: "{key}, inn: "{inn}"!')
                cur_key_valid = write_json(BASE_DIR, file_name, data, inn, table_height, workbook)
                table_height += 1
                successful = True
                if not cur_key_valid:
                    print(f"Key {api_keys[0]} is fully used and deleted from stack.")
                    api_keys.pop(0)
                break
            else:
                print(f'{table_height} (Attempt #{attempts}) with key: "{key}" - Fail')

        if not successful:
            print(f'{table_height}: Ignore company with the INN: "{inn}".'
                  f' Data from api is corrupted or All keys were used!')
            table_height += 1


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


def write_json(base_dir: str, file_name: str, data: dict, inn: str, table_height: str,
               workbook: openpyxl.workbook.workbook) -> bool:
    xlsx_write_file = os.path.join(base_dir, file_name)
    # workbook = openpyxl.load_workbook(xlsx_write_file)
    worksheet = workbook[SECOND_SHEET]

    worksheet['P' + str(table_height)].value = str(data)

    workbook.save(xlsx_write_file)
    workbook.close()

    return data['meta']['today_request_count'] < MAX_REQUEST_PER_KEY


def write_data_to_xlsx(base_dir: str, file_name: str, data: dict, inn: str, table_height: str) -> bool:
    xlsx_write_file = os.path.join(base_dir, file_name)
    workbook = openpyxl.load_workbook(xlsx_write_file)
    worksheet = workbook[SECOND_SHEET]

    try:
        row = data['data']
        managing_organization = row['УпрОрг']
    except Exception as e:
        print(f"{table_height}: Не вышло получить данные из запроса с ИНН '{inn}': {e}")
        return

    letters = ('O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')

    if managing_organization:
        # worksheet['O' + str(table_height)].value = managing_organization['ИНН']
        worksheet['P' + str(table_height)].value = managing_organization['НаимПолн']

    auth_capital_amount = row['УстКап']
    if auth_capital_amount:
        worksheet['R' + str(table_height)].value = auth_capital_amount['Сумма']

    ceo = row['Руковод']
    if ceo:
        ceos = ''
        for item in ceo:
            ceos += f"{item['ФИО']}:{item['НаимДолжн']};"
        worksheet['S' + str(table_height)].value = ceos

    contacts = row['Контакты']
    if contacts:
        phone_numbers = ''
        if contacts['Тел']:
            for phone in contacts['Тел']:
                phone_numbers += phone + ';'
        worksheet['T' + str(table_height)].value = phone_numbers

        email_addresses = ''
        if contacts['Емэйл']:
            for email in contacts['Емэйл']:
                email_addresses += email + ';'
        worksheet['U' + str(table_height)].value = email_addresses

        websites = ''
        if contacts['ВебСайт']:
            for website in contacts['ВебСайт']:
                websites += website + ';'
        worksheet['V' + str(table_height)].value = websites

    worksheet['W' + str(table_height)].value = row['СЧР']

    worksheet['X' + str(table_height)].value = data['meta']['today_request_count']

    workbook.save(xlsx_write_file)
    workbook.close()

    return data['meta']['today_request_count'] < MAX_REQUEST_PER_KEY


def make_xlsx_out_file(
        base_dir: str,
        file_name: str,
        fields: list[str]
) -> None:
    """ Create xlsx file to write response data.

    :arg:
        base_dir: dir from this module.
        file_name: xlsx file name.
        fields: header fields to fill in xlsx file.
    """
    file_path = os.path.join(base_dir, file_name)
    workbook = openpyxl.load_workbook(file_path)
    worksheet = workbook[SECOND_SHEET]

    if os.path.exists(file_path):
        print(f'Cannot create file: {file_path}, file already exists')
        letters = ('O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')
        for field, letter in zip(fields, letters):
            worksheet[letter + '1'].value = field
        workbook.save(file_name)
        workbook.close()
        print(f'Create table names for {file_path}: OK')
    else:
        print("Error: " + file_name + " must exist.")
        raise NoFileException(Exception)

    # letters = ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K')
    # for field, letter in zip(fields, letters):
    #     worksheet[letter + '1'].value = field
    # workbook.save(file_name)
    # workbook.close()
    # print(f'Create {file_path}: OK')


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


class NoFileException(Exception):
    pass


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass