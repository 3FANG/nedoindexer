# nedoindexer

[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/3FANG/nedoindexer/badges/quality-score.png?b=main)](https://scrutinizer-ci.com/g/3FANG/nedoindexer/?branch=main)

Упрощенная и дешевая в обслуживании версия индексатора для блокчейна TON.

Работает совместно с SDK pytoniq и индексатором toncenter.com

## Зависимости
- Python >=3.10,<3.11.0
- Poetry ^1.8  
- PostgreSQL ^16

## Установка
1. Склонировать репозиторий:
 ```bash
 git clone https://github.com/3FANG/python-django-developer-project-52.git
 ```
2. В корень проекта добавить файл ```.env```, список переменных - ```.env_EXAMPLE```.

3. В корень проекта добавить пары прокси-ключ в файл ```proxy_keys.txt```, пример формата записи - ```EXAMPLE_proxy_keys.txt```.
> Скорость работы программы зависит от количества пар прокси-ключ и их тарифного плана. В моем случае использовались бесплатные ключи,
> зарегистрированные на разные прокси. Если вы будете использовать платные ключи - скрипт будет работать намного лучше.

4. В директории проекта выполнить:
 ```bash
 make setup 
```

## Запуск приложения
Используйте ```make start``` для запуска приложения, либо в уже запущенном виртуальном окружении команду `python3 -m nedoindexer`
