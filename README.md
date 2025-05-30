# DataFrameProcessorFramework

Фреймворк для интерактивной обработки Pandas DataFrame в среде Google Colab с удобным пользовательским интерфейсом.

## Описание

DataFrameProcessorFramework позволяет пользователю выбирать различные шаги предобработки данных (очистка, преобразование типов, обработка пропусков и т.д.), применять их к "сырому" Pandas DataFrame, сохранять обработанный DataFrame на Google Drive и передавать его для дальнейшего использования в следующих ячейках ноутбука.

## Возможности

- **Интерактивный интерфейс пользователя** для обработки данных без написания кода
- **Гибкая настройка операций обработки** данных через UI
- **Сохранение** обработанных данных на Google Drive
- **Интеграция** с другими фреймворками и инструментами обработки данных

## Технологический стек

* **Язык:** Python 3.6+
* **Обработка данных:** pandas
* **Интерфейс в Colab:** ipywidgets
* **Работа с Google Drive:** google.colab.drive
* **Даты/Время:** datetime

## Установка

### Вариант 1: Клонирование репозитория в Colab

```python
!git clone https://github.com/username/DataFrameProcessorFramework.git
import sys
sys.path.append('/content/DataFrameProcessorFramework')
```

### Вариант 2: Загрузка файлов вручную

Загрузите папку проекта в свой Google Colab workspace или локальную папку проекта.

## Возможности обработки данных

Фреймворк предоставляет следующие опции обработки данных:

### 1. Установка индекса времени
   - Преобразование выбранного столбца в формат datetime
   - Установка этого столбца в качестве индекса DataFrame
   - Настройки обработки ошибок и формата даты

### 2. Обработка пропусков (NaN)
   - Множество стратегий: заполнение вперед/назад, удаление, интерполяция и др.
   - Опции для выбора подмножества колонок
   - Настройки для каждой стратегии (значение для заполнения, метод интерполяции)

### 3. Преобразование типов данных
   - Конвертация колонок в нужные типы (float, int, str, category, bool, datetime)
   - Настройка обработки ошибок при конвертации

### 4. Удаление дублирующихся строк
   - Выбор сохраняемых дубликатов (первый, последний, удалять все)
   - Определение дубликатов по подмножеству колонок

### 5. Переименование колонок
   - Простое переименование через словарь {старое_имя: новое_имя}

### 6. Выбор нужных колонок
   - Удобное указание списка колонок, которые нужно сохранить

### 7. Сохранение результата
   - Сохранение на Google Drive в выбранном формате
   - Опции для именования файлов и добавления временной метки

## Примеры использования

### Пример 1: Базовое использование

```python
import pandas as pd
from dataframe_processor_framework import launch_processor_ui

# Загружаем "сырые" данные
raw_dataframe = pd.read_csv('your_data.csv')

# Запускаем UI для обработки
processor_ui_instance = launch_processor_ui(raw_dataframe)

# После работы с UI и нажатия кнопки "Обработать и сохранить"
# получаем обработанный DataFrame
processed_df = processor_ui_instance.get_final_dataframe()

# Проверяем результат
if processed_df is not None:
    print("Обработанный DataFrame готов к использованию:")
    display(processed_df.head())
else:
    print("Обработка не была завершена или DataFrame не доступен.")
```

### Пример 2: Использование после другого фреймворка

```python
# Предполагается, что raw_dataframe доступен из предыдущей ячейки
# например, после использования другого фреймворка для загрузки данных

# Проверяем наличие данных
if 'raw_dataframe' in locals() and raw_dataframe is not None:
    from dataframe_processor_framework import launch_processor_ui
    processor_ui_instance = launch_processor_ui(raw_dataframe)
else:
    print("Ошибка: 'raw_dataframe' не определен или пуст. Запустите сначала ячейку с загрузкой данных.")
```

## Интерфейс пользователя

UI организован с использованием вкладок для различных операций обработки:
- Вкладка "Индекс времени" для настройки временного индекса
- Вкладка "Пропуски" для обработки отсутствующих значений
- Вкладка "Типы данных" для преобразования типов
- Вкладка "Дубликаты" для работы с дублирующимися строками
- Вкладка "Переименовать" для переименования колонок
- Вкладка "Выбрать колонки" для выбора нужных колонок

Каждая вкладка содержит все необходимые настройки для соответствующей операции.

## Структура проекта

```
Data_prep/
├── README.md                              # Этот файл
├── PLAN.md                                # План разработки
├── setup.py                               # Установочный файл для pip
├── examples/                              # Примеры использования
│   └── example_usage.ipynb                # Пример использования в Jupyter Notebook
└── dataframe_processor_framework/         # Основной фреймворк
    ├── __init__.py                        # Основной модуль и функция запуска
    ├── data_processor.py                  # Логика обработки данных
    ├── drive_io_handler.py                # Сохранение на Google Drive
    └── colab_processor_ui.py              # Интерактивный UI
```

## Требования

* Python 3.6 или выше
* pandas
* ipywidgets
* Google Colab (для полной функциональности)

## Лицензия

MIT

## Автор

Имя Автора