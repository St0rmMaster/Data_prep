#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль интерактивного интерфейса в Colab для фреймворка DataFrameProcessorFramework.
"""

import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML
import json
from typing import Optional, List, Dict, Any, Callable

from .data_processor import DataFrameProcessor
from .drive_io_handler import DriveIOHandler


class DataProcessorUI:
    """
    Класс для создания интерактивного UI в Colab для обработки DataFrame.
    """
    
    def __init__(self, input_df: pd.DataFrame):
        """
        Инициализирует UI для обработки DataFrame.
        
        Args:
            input_df (pd.DataFrame): Исходный "сырой" DataFrame для обработки.
        """
        # Сохраняем исходные данные
        self.raw_df = input_df
        self.processed_df = None
        
        # Создаем экземпляры обработчика и сохранения
        self.processor = DataFrameProcessor(self.raw_df)
        self.drive_saver = DriveIOHandler()
        
        # Создаем виджеты UI
        self._create_widgets()
        
    def _create_widgets(self) -> None:
        """
        Создает все виджеты UI и устанавливает их поведение.
        """
        # Область для отображения информации о DataFrame
        self.df_info_output = widgets.Output(layout={'border': '1px solid #ddd', 'padding': '10px'})
        
        # =================== ВИДЖЕТЫ ДЛЯ ОПЕРАЦИЙ ОБРАБОТКИ ===================
        
        # --- Установка временного индекса ---
        self.cb_set_datetime_index = widgets.Checkbox(
            value=False,
            description='Установить индекс времени',
            disabled=False
        )
        
        self.text_time_col = widgets.Text(
            value='timestamp',
            placeholder='Имя столбца',
            description='Колонка времени:',
            disabled=False
        )
        
        self.dropdown_time_errors = widgets.Dropdown(
            options=['raise', 'coerce', 'ignore'],
            value='raise',
            description='При ошибках:',
            disabled=False,
        )
        
        self.cb_infer_datetime_format = widgets.Checkbox(
            value=True,
            description='Автоматически определять формат',
            disabled=False
        )
        
        # --- Обработка пропусков ---
        self.cb_handle_missing = widgets.Checkbox(
            value=False,
            description='Обработать пропуски',
            disabled=False
        )
        
        self.dropdown_missing_strategy = widgets.Dropdown(
            options=['ffill', 'bfill', 'dropna_rows', 'dropna_cols', 'interpolate', 
                    'fill_constant', 'fill_mean', 'fill_median', 'fill_zero'],
            value='ffill',
            description='Стратегия:',
            disabled=False,
        )
        
        self.text_missing_fill_value = widgets.Text(
            value='0',
            placeholder='Значение',
            description='Значение для fill_constant:',
            disabled=False
        )
        
        self.dropdown_interpolate_method = widgets.Dropdown(
            options=['linear', 'time', 'index', 'pad', 'nearest', 'zero', 
                    'slinear', 'quadratic', 'cubic', 'spline', 'barycentric', 
                    'polynomial', 'krogh', 'piecewise_polynomial', 'pchip', 'akima'],
            value='linear',
            description='Метод интерполяции:',
            disabled=False,
        )
        
        self.text_missing_subset_cols = widgets.Text(
            value='',
            placeholder='col1,col2,col3',
            description='Колонки (через запятую, пусто=все):',
            disabled=False
        )
        
        # --- Преобразование типов ---
        self.cb_convert_types = widgets.Checkbox(
            value=False,
            description='Преобразовать типы данных',
            disabled=False
        )
        
        self.textarea_column_types = widgets.Textarea(
            value='{}',
            placeholder='{"col1": "float", "col2": "int", "col3": "str"}',
            description='Типы (JSON):',
            disabled=False,
            layout={'width': '400px', 'height': '80px'}
        )
        
        self.dropdown_convert_errors = widgets.Dropdown(
            options=['raise', 'ignore'],
            value='raise',
            description='При ошибках:',
            disabled=False,
        )
        
        # --- Удаление дубликатов ---
        self.cb_remove_duplicates = widgets.Checkbox(
            value=False,
            description='Удалить дубликаты строк',
            disabled=False
        )
        
        self.dropdown_duplicates_keep = widgets.Dropdown(
            options=[('Первое вхождение', 'first'), 
                    ('Последнее вхождение', 'last'), 
                    ('Удалить все дубликаты', False)],
            value='first',
            description='Сохранить:',
            disabled=False,
        )
        
        self.text_duplicates_subset = widgets.Text(
            value='',
            placeholder='col1,col2,col3',
            description='Колонки для определения дубликатов:',
            disabled=False
        )
        
        # --- Переименование колонок ---
        self.cb_rename_columns = widgets.Checkbox(
            value=False,
            description='Переименовать колонки',
            disabled=False
        )
        
        self.textarea_rename_map = widgets.Textarea(
            value='{}',
            placeholder='{"old_name1": "new_name1", "old_name2": "new_name2"}',
            description='Карта переименования:',
            disabled=False,
            layout={'width': '400px', 'height': '80px'}
        )
        
        # --- Выбор колонок ---
        self.cb_select_columns = widgets.Checkbox(
            value=False,
            description='Выбрать нужные колонки',
            disabled=False
        )
        
        self.text_columns_to_keep = widgets.Text(
            value='',
            placeholder='col1,col2,col3',
            description='Колонки для сохранения:',
            disabled=False,
            layout={'width': '400px'}
        )
        
        # =================== ВИДЖЕТЫ ДЛЯ СОХРАНЕНИЯ ===================
        self.text_save_filename_prefix = widgets.Text(
            value='processed_data',
            placeholder='Префикс',
            description='Префикс имени файла:',
            disabled=False
        )
        
        self.dropdown_save_format = widgets.Dropdown(
            options=['parquet', 'csv', 'feather', 'pickle'],
            value='parquet',
            description='Формат:',
            disabled=False,
        )
        
        self.cb_include_timestamp = widgets.Checkbox(
            value=True,
            description='Добавить временную метку к имени файла',
            disabled=False
        )
        
        # =================== КНОПКИ ===================
        self.process_button = widgets.Button(
            description='Обработать и сохранить',
            disabled=False,
            button_style='primary',
            tooltip='Нажмите для обработки данных',
            icon='cogs'
        )
        
        # =================== ОБЛАСТЬ ВЫВОДА ЛОГОВ И РЕЗУЛЬТАТОВ ===================
        self.log_output = widgets.Output(
            layout={'border': '1px solid #ddd', 'max_height': '200px', 
                   'overflow_y': 'auto', 'padding': '10px'}
        )
        
        self.processed_df_preview_output = widgets.Output(
            layout={'border': '1px solid #ddd', 'padding': '10px'}
        )
        
        # =================== ПРИВЯЗКА ОБРАБОТЧИКОВ ===================
        self.process_button.on_click(self._on_process_button_clicked)
        
        # =================== ДИНАМИЧЕСКОЕ ОТОБРАЖЕНИЕ ВИДЖЕТОВ ===================
        # Обработчики для отображения/скрытия виджетов в зависимости от выбора
        self._setup_dynamic_widgets()
        
    def _setup_dynamic_widgets(self) -> None:
        """
        Настраивает динамическое отображение виджетов в зависимости от выбранных опций.
        """
        # Обработчики для динамического отображения виджетов
        def on_datetime_checkbox_change(change):
            if change['new']:
                self.text_time_col.layout.display = 'block'
                self.dropdown_time_errors.layout.display = 'block'
                self.cb_infer_datetime_format.layout.display = 'block'
            else:
                self.text_time_col.layout.display = 'none'
                self.dropdown_time_errors.layout.display = 'none'
                self.cb_infer_datetime_format.layout.display = 'none'
        
        def on_missing_checkbox_change(change):
            if change['new']:
                self.dropdown_missing_strategy.layout.display = 'block'
                self.text_missing_subset_cols.layout.display = 'block'
                
                # При изменении стратегии также показываем/скрываем связанные виджеты
                strategy = self.dropdown_missing_strategy.value
                if strategy == 'fill_constant':
                    self.text_missing_fill_value.layout.display = 'block'
                else:
                    self.text_missing_fill_value.layout.display = 'none'
                    
                if strategy == 'interpolate':
                    self.dropdown_interpolate_method.layout.display = 'block'
                else:
                    self.dropdown_interpolate_method.layout.display = 'none'
            else:
                self.dropdown_missing_strategy.layout.display = 'none'
                self.text_missing_fill_value.layout.display = 'none'
                self.dropdown_interpolate_method.layout.display = 'none'
                self.text_missing_subset_cols.layout.display = 'none'
        
        def on_missing_strategy_change(change):
            strategy = change['new']
            if strategy == 'fill_constant':
                self.text_missing_fill_value.layout.display = 'block'
            else:
                self.text_missing_fill_value.layout.display = 'none'
                
            if strategy == 'interpolate':
                self.dropdown_interpolate_method.layout.display = 'block'
            else:
                self.dropdown_interpolate_method.layout.display = 'none'
        
        def on_convert_types_checkbox_change(change):
            if change['new']:
                self.textarea_column_types.layout.display = 'block'
                self.dropdown_convert_errors.layout.display = 'block'
            else:
                self.textarea_column_types.layout.display = 'none'
                self.dropdown_convert_errors.layout.display = 'none'
        
        def on_remove_duplicates_checkbox_change(change):
            if change['new']:
                self.dropdown_duplicates_keep.layout.display = 'block'
                self.text_duplicates_subset.layout.display = 'block'
            else:
                self.dropdown_duplicates_keep.layout.display = 'none'
                self.text_duplicates_subset.layout.display = 'none'
        
        def on_rename_columns_checkbox_change(change):
            if change['new']:
                self.textarea_rename_map.layout.display = 'block'
            else:
                self.textarea_rename_map.layout.display = 'none'
        
        def on_select_columns_checkbox_change(change):
            if change['new']:
                self.text_columns_to_keep.layout.display = 'block'
            else:
                self.text_columns_to_keep.layout.display = 'none'
        
        # Привязываем обработчики
        self.cb_set_datetime_index.observe(on_datetime_checkbox_change, names='value')
        self.cb_handle_missing.observe(on_missing_checkbox_change, names='value')
        self.dropdown_missing_strategy.observe(on_missing_strategy_change, names='value')
        self.cb_convert_types.observe(on_convert_types_checkbox_change, names='value')
        self.cb_remove_duplicates.observe(on_remove_duplicates_checkbox_change, names='value')
        self.cb_rename_columns.observe(on_rename_columns_checkbox_change, names='value')
        self.cb_select_columns.observe(on_select_columns_checkbox_change, names='value')
        
        # Инициализируем скрытие виджетов
        self.text_time_col.layout.display = 'none'
        self.dropdown_time_errors.layout.display = 'none'
        self.cb_infer_datetime_format.layout.display = 'none'
        
        self.dropdown_missing_strategy.layout.display = 'none'
        self.text_missing_fill_value.layout.display = 'none'
        self.dropdown_interpolate_method.layout.display = 'none'
        self.text_missing_subset_cols.layout.display = 'none'
        
        self.textarea_column_types.layout.display = 'none'
        self.dropdown_convert_errors.layout.display = 'none'
        
        self.dropdown_duplicates_keep.layout.display = 'none'
        self.text_duplicates_subset.layout.display = 'none'
        
        self.textarea_rename_map.layout.display = 'none'
        
        self.text_columns_to_keep.layout.display = 'none'
        
    def _on_process_button_clicked(self, b: widgets.Button) -> None:
        """
        Обработчик нажатия на кнопку "Обработать и сохранить".
        
        Args:
            b (widgets.Button): Объект кнопки, вызвавшей обработчик.
        """
        # Очищаем область вывода
        self.log_output.clear_output()
        self.processed_df_preview_output.clear_output()
        
        with self.log_output:
            print("Начинаю обработку данных...")
            
            # Создаем новый экземпляр обработчика с копией исходных данных
            self.processor = DataFrameProcessor(self.raw_df.copy())
            
            # --- Установка временного индекса ---
            if self.cb_set_datetime_index.value:
                print("\n--- Установка временного индекса ---")
                time_col = self.text_time_col.value.strip()
                errors = self.dropdown_time_errors.value
                infer_datetime_format = self.cb_infer_datetime_format.value
                
                try:
                    self.processor.set_datetime_index(
                        time_col=time_col,
                        errors=errors,
                        infer_datetime_format=infer_datetime_format
                    )
                except Exception as e:
                    print(f"Ошибка при установке временного индекса: {str(e)}")
            
            # --- Обработка пропусков ---
            if self.cb_handle_missing.value:
                print("\n--- Обработка пропущенных значений ---")
                strategy = self.dropdown_missing_strategy.value
                
                # Обрабатываем список колонок
                subset_cols_text = self.text_missing_subset_cols.value.strip()
                subset_cols = [col.strip() for col in subset_cols_text.split(',')] if subset_cols_text else None
                
                # Для стратегии fill_constant получаем значение
                fill_value = None
                if strategy == 'fill_constant':
                    # Пытаемся преобразовать значение в нужный тип
                    fill_value_text = self.text_missing_fill_value.value.strip()
                    try:
                        # Пробуем как число
                        if '.' in fill_value_text:
                            fill_value = float(fill_value_text)
                        else:
                            fill_value = int(fill_value_text)
                    except ValueError:
                        # Если не число, оставляем как строку
                        fill_value = fill_value_text
                
                # Для interpolate получаем метод
                method = 'linear'
                if strategy == 'interpolate':
                    method = self.dropdown_interpolate_method.value
                
                try:
                    self.processor.handle_missing_values(
                        strategy=strategy,
                        subset_cols=subset_cols,
                        fill_value=fill_value,
                        method=method
                    )
                except Exception as e:
                    print(f"Ошибка при обработке пропущенных значений: {str(e)}")
            
            # --- Преобразование типов данных ---
            if self.cb_convert_types.value:
                print("\n--- Преобразование типов данных ---")
                try:
                    column_types = json.loads(self.textarea_column_types.value)
                    errors = self.dropdown_convert_errors.value
                    
                    self.processor.convert_data_types(
                        column_types=column_types,
                        errors=errors
                    )
                except json.JSONDecodeError as e:
                    print(f"Ошибка в формате JSON для типов колонок: {str(e)}")
                except Exception as e:
                    print(f"Ошибка при преобразовании типов данных: {str(e)}")
                    
            # --- Удаление дубликатов ---
            if self.cb_remove_duplicates.value:
                print("\n--- Удаление дубликатов строк ---")
                # Обрабатываем список колонок для определения дубликатов
                subset_text = self.text_duplicates_subset.value.strip()
                subset = [col.strip() for col in subset_text.split(',')] if subset_text else None
                
                keep = self.dropdown_duplicates_keep.value
                
                try:
                    self.processor.remove_duplicate_rows(
                        subset=subset,
                        keep=keep
                    )
                except Exception as e:
                    print(f"Ошибка при удалении дубликатов: {str(e)}")
            
            # --- Переименование колонок ---
            if self.cb_rename_columns.value:
                print("\n--- Переименование колонок ---")
                try:
                    rename_map = json.loads(self.textarea_rename_map.value)
                    self.processor.rename_columns(rename_map=rename_map)
                except json.JSONDecodeError as e:
                    print(f"Ошибка в формате JSON для переименования колонок: {str(e)}")
                except Exception as e:
                    print(f"Ошибка при переименовании колонок: {str(e)}")
            
            # --- Выбор колонок ---
            if self.cb_select_columns.value:
                print("\n--- Выбор колонок ---")
                # Обрабатываем список колонок для сохранения
                columns_text = self.text_columns_to_keep.value.strip()
                if columns_text:
                    columns_to_keep = [col.strip() for col in columns_text.split(',')]
                    try:
                        self.processor.select_columns(columns_to_keep=columns_to_keep)
                    except Exception as e:
                        print(f"Ошибка при выборе колонок: {str(e)}")
                else:
                    print("Не указаны колонки для сохранения. Пропускаю этот шаг.")
            
            # Получаем обработанный DataFrame
            self.processed_df = self.processor.get_processed_df()
            
            # --- Сохранение DataFrame ---
            print("\n--- Сохранение обработанного DataFrame ---")
            filename_prefix = self.text_save_filename_prefix.value.strip()
            file_format = self.dropdown_save_format.value
            include_timestamp = self.cb_include_timestamp.value
            
            try:
                file_path = self.drive_saver.save_dataframe(
                    df=self.processed_df,
                    filename_prefix=filename_prefix,
                    file_format=file_format,
                    include_timestamp=include_timestamp
                )
                if file_path:
                    print(f"DataFrame успешно сохранен: {file_path}")
            except Exception as e:
                print(f"Ошибка при сохранении DataFrame: {str(e)}")
            
            print("\nОбработка данных завершена!")
        
        # Отображаем результат в области предпросмотра
        with self.processed_df_preview_output:
            if self.processed_df is not None:
                print("Обработанный DataFrame:")
                display(HTML("<h4>Сводная информация</h4>"))
                self.processed_df.info()
                display(HTML("<h4>Первые 5 строк</h4>"))
                display(self.processed_df.head())
            else:
                print("Обработка данных не выполнена или завершилась с ошибкой.")
        
    def display(self) -> None:
        """
        Отображает UI в ячейке Jupyter/Colab ноутбука.
        """
        # Отображаем информацию о исходном DataFrame
        with self.df_info_output:
            display(HTML("<h3>Исходный DataFrame</h3>"))
            display(HTML("<h4>Сводная информация</h4>"))
            self.raw_df.info()
            display(HTML("<h4>Первые 5 строк</h4>"))
            display(self.raw_df.head())
        
        # Создаем структуру UI
        
        # --- Блок для базовой информации
        info_block = widgets.VBox([
            widgets.HTML("<h3>Исходный DataFrame</h3>"),
            self.df_info_output
        ])
        
        # --- Блок настроек индекса времени
        datetime_index_block = widgets.VBox([
            self.cb_set_datetime_index,
            self.text_time_col,
            self.dropdown_time_errors,
            self.cb_infer_datetime_format
        ], layout={'border': '1px solid #ddd', 'padding': '10px', 'margin': '5px'})
        
        # --- Блок настроек обработки пропусков
        missing_values_block = widgets.VBox([
            self.cb_handle_missing,
            self.dropdown_missing_strategy,
            self.text_missing_fill_value,
            self.dropdown_interpolate_method,
            self.text_missing_subset_cols
        ], layout={'border': '1px solid #ddd', 'padding': '10px', 'margin': '5px'})
        
        # --- Блок настроек преобразования типов
        convert_types_block = widgets.VBox([
            self.cb_convert_types,
            self.textarea_column_types,
            self.dropdown_convert_errors
        ], layout={'border': '1px solid #ddd', 'padding': '10px', 'margin': '5px'})
        
        # --- Блок настроек удаления дубликатов
        duplicates_block = widgets.VBox([
            self.cb_remove_duplicates,
            self.dropdown_duplicates_keep,
            self.text_duplicates_subset
        ], layout={'border': '1px solid #ddd', 'padding': '10px', 'margin': '5px'})
        
        # --- Блок настроек переименования колонок
        rename_block = widgets.VBox([
            self.cb_rename_columns,
            self.textarea_rename_map
        ], layout={'border': '1px solid #ddd', 'padding': '10px', 'margin': '5px'})
        
        # --- Блок настроек выбора колонок
        select_columns_block = widgets.VBox([
            self.cb_select_columns,
            self.text_columns_to_keep
        ], layout={'border': '1px solid #ddd', 'padding': '10px', 'margin': '5px'})
        
        # --- Блок настроек сохранения
        save_block = widgets.VBox([
            widgets.HTML("<h4>Параметры сохранения</h4>"),
            self.text_save_filename_prefix,
            self.dropdown_save_format,
            self.cb_include_timestamp
        ], layout={'border': '1px solid #ddd', 'padding': '10px', 'margin': '5px'})
        
        # --- Кнопка обработки
        button_block = widgets.HBox([
            self.process_button
        ], layout={'justify_content': 'center', 'margin': '10px'})
        
        # --- Блок вывода логов и результата
        output_block = widgets.VBox([
            widgets.HTML("<h4>Лог обработки</h4>"),
            self.log_output,
            widgets.HTML("<h4>Результат (Processed DataFrame)</h4>"),
            self.processed_df_preview_output
        ])
        
        # Создаем вкладки для группировки операций
        tabs = widgets.Tab()
        tabs.children = [
            datetime_index_block,
            missing_values_block,
            convert_types_block,
            duplicates_block,
            rename_block,
            select_columns_block
        ]
        tabs.set_title(0, 'Индекс времени')
        tabs.set_title(1, 'Пропуски')
        tabs.set_title(2, 'Типы данных')
        tabs.set_title(3, 'Дубликаты')
        tabs.set_title(4, 'Переименовать')
        tabs.set_title(5, 'Выбрать колонки')
        
        # Собираем все блоки в единую структуру
        processing_block = widgets.VBox([
            widgets.HTML("<h3>Параметры обработки</h3>"),
            tabs
        ])
        
        # Основной контейнер
        main_container = widgets.VBox([
            info_block,
            processing_block,
            save_block,
            button_block,
            output_block
        ])
        
        # Отображаем UI
        display(main_container)
    
    def get_final_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Возвращает обработанный DataFrame.
        
        Returns:
            Optional[pd.DataFrame]: Обработанный DataFrame или None, если обработка не была выполнена.
        """
        return self.processed_df if self.processed_df is not None else None