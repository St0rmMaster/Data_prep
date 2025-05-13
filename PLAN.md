# План Разработки DataFrameProcessorFramework в Colab

**Цель:** Создать Python-библиотеку и интерактивный UI в Colab для обработки "сырого" Pandas DataFrame. Фреймворк должен позволять пользователю выбирать различные шаги предобработки данных (очистка, преобразование типов, обработка пропусков и т.д.), применять их, сохранять обработанный DataFrame на Google Drive и передавать его для дальнейшего использования в следующих ячейках ноутбука.

**Технологический стек:**

* **Язык:** Python
* **Обработка данных:** pandas
* **Интерфейс в Colab:** ipywidgets
* **Работа с Google Drive:** (Если потребуется прямое управление монтированием, можно использовать `google.colab.drive`, но предполагается, что диск может быть уже смонтирован предыдущим фреймворком. Основное - сохранение файлов).
* **Даты/Время:** datetime

---

**Шаги Разработки:**

### Шаг 1: Настройка Окружения и Зависимостей

1.  **Инициализация проекта:**
    * Создать структуру папок для библиотеки (например, `dataframe_processor_framework/`).
    * Внутри `dataframe_processor_framework/` создать подпапку с таким же именем для исходного кода (например, `dataframe_processor_framework/dataframe_processor_framework/`).
2.  **Установка библиотек (если нужно, обычно pandas и ipywidgets есть в Colab):**
    * Убедиться, что `pandas` и `ipywidgets` доступны.
3.  **Импорты:**
    * Продумать стандартные импорты для каждого модуля.

---

### Шаг 2: Модуль Обработки Данных (`data_processor.py`)

Этот модуль будет содержать всю логику выполнения операций над DataFrame.

* **Класс `DataFrameProcessor`**:
    * **`__init__(self, df: pd.DataFrame)`**:
        * Принимает "сырой" DataFrame.
        * Создает рабочую копию DataFrame (`self.df = df.copy()`), чтобы не изменять оригинал.
    * **`set_datetime_index(self, time_col: str, errors: str = 'raise', infer_datetime_format: bool = True) -> None`**:
        * Преобразует указанный столбец `time_col` в `datetime`.
        * Параметры `errors`, `infer_datetime_format` для `pd.to_datetime`.
        * Устанавливает этот столбец в качестве индекса DataFrame (`self.df.set_index`).
        * Логирует результат операции.
    * **`handle_missing_values(self, strategy: str = 'ffill', subset_cols: Optional[List[str]] = None, fill_value: Any = None) -> None`**:
        * Обрабатывает пропуски (`NaN`) в DataFrame.
        * `strategy`: Метод обработки ('ffill', 'bfill', 'dropna_rows', 'dropna_cols', 'interpolate', 'fill_constant', 'fill_mean', 'fill_median', 'fill_zero').
            * Для 'interpolate' можно добавить параметр `method` (e.g., 'linear', 'time').
            * Для 'fill_constant' используется `fill_value`.
            * 'fill_mean', 'fill_median' вычисляются по выбранным колонкам.
        * `subset_cols`: Список колонок, к которым применяется стратегия. Если `None`, то ко всем.
        * Логирует количество измененных/удаленных значений.
    * **`convert_data_types(self, column_types: Dict[str, str], errors: str = 'raise') -> None`**:
        * Преобразует типы данных указанных колонок.
        * `column_types`: Словарь, где ключ - имя колонки, значение - целевой тип (например, `'float'`, `'int'`, `'str'`, `'category'`).
        * Параметр `errors` для `astype`.
        * Логирует изменения типов.
    * **`remove_duplicate_rows(self, subset: Optional[List[str]] = None, keep: str = 'first') -> None`**:
        * Удаляет дублирующиеся строки.
        * Параметры `subset`, `keep` для `pd.DataFrame.drop_duplicates`.
        * Логирует количество удаленных дубликатов.
    * **`rename_columns(self, rename_map: Dict[str, str]) -> None`**:
        * Переименовывает колонки согласно словарю `rename_map`.
        * Логирует изменения.
    * **`select_columns(self, columns_to_keep: List[str]) -> None`**:
        * Оставляет только указанные колонки в `columns_to_keep`.
        * Логирует список оставленных и удаленных колонок.
    * **`apply_custom_function(self, func: Callable[[pd.DataFrame], pd.DataFrame], *args, **kwargs) -> None`**:
        * Позволяет применить пользовательскую функцию к DataFrame.
        * `func` должна принимать DataFrame и возвращать DataFrame.
        * Логирует применение кастомной функции.
    * **`get_processed_df(self) -> pd.DataFrame`**:
        * Возвращает обработанный DataFrame (`self.df`).

---

### Шаг 3: Модуль Сохранения на Google Drive (`drive_io_handler.py`)

Этот модуль будет отвечать за сохранение обработанного DataFrame.

* **Класс `DriveIOHandler`**:
    * **`__init__(self, base_save_path: str = '/content/drive/MyDrive/processed_data/')`**:
        * `base_save_path`: Путь по умолчанию для сохранения на Google Drive.
        * Проверяет и создает `base_save_path`, если его нет (`os.makedirs(exist_ok=True)`).
    * **`_ensure_drive_mounted(self) -> None` (Приватный метод)**:
        * Проверяет, смонтирован ли Google Drive.
        * Если нет, пытается смонтировать (`google.colab.drive.mount('/content/drive', force_remount=True)`).
        * Обрабатывает возможные исключения при монтировании.
    * **`save_dataframe(self, df: pd.DataFrame, filename_prefix: str = 'processed_df', file_format: str = 'parquet', include_timestamp: bool = True, custom_metadata: Optional[Dict[str, Any]] = None) -> str`**:
        * Сохраняет DataFrame на Google Drive.
        * `df`: DataFrame для сохранения.
        * `filename_prefix`: Префикс для имени файла.
        * `file_format`: Формат сохранения ('parquet', 'csv', 'feather', 'pickle').
        * `include_timestamp`: Если `True`, добавляет временную метку к имени файла для уникальности.
        * `custom_metadata`: Опционально, метаданные о датафрейме (например, какие шаги обработки были применены), которые можно сохранить в отдельный JSON-файл рядом или использовать для именования.
        * Вызывает `_ensure_drive_mounted()`.
        * Формирует полное имя файла.
        * Сохраняет DataFrame (например, `df.to_parquet()`, `df.to_csv()`).
        * Возвращает полный путь к сохраненному файлу.
        * Логирует путь к сохраненному файлу.

---

### Шаг 4: Модуль Интерактивного Интерфейса (`colab_processor_ui.py`)

Этот модуль создаст UI для пользователя в Colab.

* **Класс `DataProcessorUI`**:
    * **`__init__(self, input_df: pd.DataFrame)`**:
        * Принимает `input_df` (сырой DataFrame).
        * Сохраняет его (`self.raw_df = input_df`).
        * Инициализирует `self.processed_df = None`.
        * Создает экземпляры `DataFrameProcessor(self.raw_df)` и `DriveIOHandler()`.
        * Вызывает `_create_widgets()` для создания всех элементов UI.
    * **`_create_widgets(self) -> None` (Приватный метод)**:
        * **Область информации о DataFrame:**
            * `self.df_info_output = widgets.Output()`: Для отображения `raw_df.info()` и `raw_df.head()`.
        * **Виджеты для выбора операций обработки:**
            * `self.cb_set_datetime_index = widgets.Checkbox(description='Установить индекс времени')`
            * `self.text_time_col = widgets.Text(description='Колонка времени:', value='timestamp')`
            * `self.cb_handle_missing = widgets.Checkbox(description='Обработать пропуски')`
            * `self.dropdown_missing_strategy = widgets.Dropdown(description='Стратегия:', options=['ffill', 'bfill', 'dropna_rows', 'interpolate', 'fill_constant', 'fill_mean', 'fill_median', 'fill_zero'], value='ffill')`
            * `self.text_missing_fill_value = widgets.Text(description='Значение для fill_constant:', value='0')`
            * `self.text_missing_subset_cols = widgets.Text(description='Колонки (через запятую, пусто=все):')`
            * `self.cb_convert_types = widgets.Checkbox(description='Преобразовать типы данных')`
            * `self.textarea_column_types = widgets.Textarea(description='Типы колонок (JSON, {"col": "type"}):', value='{}')`
            * `self.cb_remove_duplicates = widgets.Checkbox(description='Удалить дубликаты строк')`
            * `self.dropdown_duplicates_keep = widgets.Dropdown(description='Оставить:', options=['first', 'last', False], value='first')`
            * `self.cb_rename_columns = widgets.Checkbox(description='Переименовать колонки')`
            * `self.textarea_rename_map = widgets.Textarea(description='Словарь переименования (JSON):', value='{}')`
            * `self.cb_select_columns = widgets.Checkbox(description='Выбрать нужные колонки')`
            * `self.text_columns_to_keep = widgets.Text(description='Колонки для сохранения (через запятую):', value='open,high,low,close,volume')`
            * (По аналогии можно добавить другие опции, если нужно).
        * **Виджеты для сохранения:**
            * `self.text_save_filename_prefix = widgets.Text(description='Префикс имени файла:', value='processed_data')`
            * `self.dropdown_save_format = widgets.Dropdown(description='Формат сохранения:', options=['parquet', 'csv', 'feather'], value='parquet')`
        * **Кнопки:**
            * `self.process_button = widgets.Button(description='Обработать и сохранить', button_style='primary', icon='cogs')`
        * **Область вывода логов и результатов:**
            * `self.log_output = widgets.Output(layout={'border': '1px solid black', 'max_height': '200px', 'overflow_y': 'auto'})`
            * `self.processed_df_preview_output = widgets.Output()`
        * **Привязка обработчиков:**
            * `self.process_button.on_click(self._on_process_button_clicked)`
        * **Динамическое отображение виджетов-параметров:**
            * Написать логику, чтобы, например, `self.text_time_col` отображался только если `self.cb_set_datetime_index.value == True`. Это можно сделать через `widgets.interactive_output` или прямым управлением `layout.display`.
    * **`_on_process_button_clicked(self, b: widgets.Button) -> None` (Приватный метод)**:
        * Очищает `self.log_output` и `self.processed_df_preview_output`.
        * Создает новый экземпляр `self.processor = DataFrameProcessor(self.raw_df.copy())`.
        * **Сбор конфигурации из UI:**
            * Проверяет состояние каждого чекбокса. Если активен, считывает значения из соответствующих виджетов (текстовых полей, дропдаунов).
        * **Последовательный вызов методов `DataFrameProcessor`:**
            * В блоке `with self.log_output:` логировать каждый шаг.
            * `if self.cb_set_datetime_index.value: self.processor.set_datetime_index(self.text_time_col.value)`
            * `if self.cb_handle_missing.value: self.processor.handle_missing_values(strategy=self.dropdown_missing_strategy.value, ...)`
            * И так далее для всех выбранных операций.
        * Получает обработанный DataFrame: `self.processed_df = self.processor.get_processed_df()`.
        * **Сохранение DataFrame:**
            * Вызывает `self.drive_saver.save_dataframe(self.processed_df, filename_prefix=self.text_save_filename_prefix.value, file_format=self.dropdown_save_format.value)`.
            * Логирует путь к файлу.
        * **Отображение результатов:**
            * В `self.processed_df_preview_output` отобразить `self.processed_df.info()` и `self.processed_df.head()`.
            * Сообщить пользователю об успешном завершении.
    * **`display(self) -> None`**:
        * Отображает начальную информацию (`self.raw_df.info()`, `self.raw_df.head()`) в `self.df_info_output`.
        * Собирает все виджеты в логическую структуру (например, с использованием `widgets.VBox`, `widgets.HBox`, `widgets.Accordion`, `widgets.Tab`).
        * Пример структуры:
            * Заголовок "Исходный DataFrame"
            * `self.df_info_output`
            * Заголовок "Параметры Обработки"
            * Аккордеон или табы для группировки опций (Очистка, Преобразование, и т.д.)
                * Внутри каждой секции – соответствующие чекбоксы и поля ввода.
            * Заголовок "Параметры Сохранения"
            * Виджеты для сохранения.
            * `self.process_button`
            * Заголовок "Лог Обработки"
            * `self.log_output`
            * Заголовок "Результат (Processed DataFrame)"
            * `self.processed_df_preview_output`
        * Использует `IPython.display.display(...)` для отображения корневого виджета.
    * **`get_final_dataframe(self) -> Optional[pd.DataFrame]`**:
        * Возвращает `self.processed_df`. Этот метод позволит следующей ячейке Colab получить результат.

---

### Шаг 5: Основной Модуль/Функция Запуска (`__init__.py` и функция `launch_processor_ui`)

* **`dataframe_processor_framework/__init__.py`**:
    * Импортирует основные классы/функции для удобства использования:
        ```python
        from .colab_processor_ui import DataProcessorUI
        from .data_processor import DataFrameProcessor # Если нужен прямой доступ
        from .drive_io_handler import DriveIOHandler   # Если нужен прямой доступ
        from typing import Optional, List, Dict, Any, Callable # Добавьте нужные типы

        import pandas as pd # Убедитесь, что pandas импортирован
        import ipywidgets as widgets # Для типов виджетов
        from IPython.display import display # Для display

        def launch_processor_ui(input_df: pd.DataFrame) -> DataProcessorUI:
            """
            Инициализирует и отображает DataProcessorUI в среде Google Colab.
            Принимает входной DataFrame и возвращает экземпляр UI,
            который после обработки будет содержать processed_df.
            """
            try:
                from google.colab import output
                output.enable_custom_widget_manager()
            except ImportError:
                print("Warning: 'google.colab.output' could not be imported. Custom widget manager not enabled. This UI is designed for Colab.")
            
            ui_instance = DataProcessorUI(input_df=input_df)
            ui_instance.display()
            return ui_instance
        
        __all__ = ['DataProcessorUI', 'DataFrameProcessor', 'DriveIOHandler', 'launch_processor_ui']
        ```

---

### Шаг 6: Реализация Логики Обработки (детали в `data_processor.py`)

* Тщательно реализовать каждый метод в `DataFrameProcessor`, включая обработку ошибок (например, если указана несуществующая колонка) и логирование внутри методов (можно передавать логгер или использовать print для простоты в Colab UI).
* Обеспечить, чтобы методы изменяли `self.df` внутри класса `DataFrameProcessor`.

---

### Шаг 7: Вывод Информации и Логирование в UI (детали в `colab_processor_ui.py`)

* Использовать `widgets.Output` для отображения логов в реальном времени (насколько это возможно с ipywidgets) или по завершении каждого шага.
* Предоставлять четкую обратную связь пользователю: что происходит, какие параметры применяются, какие результаты получены.

---

### Шаг 8: Упаковка в Библиотеку

* **Структура:**
    ```
    dataframe_processor_framework/
    ├── dataframe_processor_framework/
    │   ├── __init__.py
    │   ├── colab_processor_ui.py
    │   ├── data_processor.py
    │   └── drive_io_handler.py
    ├── setup.py  (Опционально, если нужна формальная установка)
    └── README.md
    ```
* **`setup.py` (опционально):** Если фреймворк будет устанавливаться через `pip`. Для простого использования в Colab путем загрузки папки или git clone, `setup.py` может не понадобиться на первом этапе.

---

### Шаг 9: Документация и Примеры

* **Docstrings:** Добавить строки документации ко всем классам и методам.
* **`README.md`:**
    * Описание фреймворка.
    * Инструкции по использованию в Colab (как передать DataFrame, как запустить UI).
    * Описание каждой опции обработки в UI.
    * Пример, как получить обработанный DataFrame в следующей ячейке.
* **Пример использования в Colab ноутбуке:**
    * Ячейка 1: Код первого фреймворка (загрузчика данных), который создает `raw_dataframe`.
        ```python
        # Пример:
        # from download_mod_data_framework import launch_ui_downloader
        # downloader_ui = launch_ui_downloader()
        # # ... работа с UI загрузчика ...
        # raw_dataframe = downloader_ui.get_some_loaded_dataframe() # Метод зависит от реализации загрузчика
        ```
    * Ячейка 2:
        ```python
        # Предполагается, что raw_dataframe доступен из предыдущей ячейки
        # Если фреймворк находится в папке:
        # import sys
        # sys.path.append('path_to_parent_of_dataframe_processor_framework_folder') # Укажите правильный путь
        
        # Убедитесь, что raw_dataframe определен и не None
        if 'raw_dataframe' in locals() and raw_dataframe is not None:
            from dataframe_processor_framework import launch_processor_ui
            processor_ui_instance = launch_processor_ui(raw_dataframe)
        else:
            print("Ошибка: 'raw_dataframe' не определен или пуст. Запустите сначала ячейку с загрузкой данных.")
        ```
    * Ячейка 3:
        ```python
        # После работы с UI во второй ячейке
        if 'processor_ui_instance' in locals() and processor_ui_instance is not None:
            processed_df = processor_ui_instance.get_final_dataframe()
            if processed_df is not None:
                print("Обработанный DataFrame готов к использованию:")
                display(processed_df.head())
            else:
                print("Обработка не была завершена или DataFrame не доступен.")
        else:
            print("Экземпляр UI обработчика не найден. Убедитесь, что ячейка с UI была успешно выполнена.")

        ```

---