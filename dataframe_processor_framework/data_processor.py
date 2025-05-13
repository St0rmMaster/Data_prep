#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль обработки DataFrame для фреймворка DataFrameProcessorFramework.
Содержит логику выполнения операций над DataFrame.
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Callable, Union
import json


class DataFrameProcessor:
    """
    Класс для обработки DataFrame. Предоставляет методы для различных
    операций предобработки данных, таких как обработка пропусков,
    установка временного индекса, преобразование типов данных и пр.
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Инициализирует обработчик DataFrame.
        
        Args:
            df (pd.DataFrame): Исходный "сырой" DataFrame для обработки.
        """
        # Создание рабочей копии DataFrame
        self.df = df.copy()
        self.log_messages = []
        self._log(f"Создана копия исходного DataFrame с размером {self.df.shape}")
    
    def _log(self, message: str) -> None:
        """
        Приватный метод для логирования операций.
        
        Args:
            message (str): Сообщение для логирования.
        """
        print(message)
        self.log_messages.append(message)
    
    def set_datetime_index(self, time_col: str, errors: str = 'raise', infer_datetime_format: bool = True) -> None:
        """
        Преобразует указанный столбец в datetime и устанавливает его в качестве индекса.
        
        Args:
            time_col (str): Имя столбца для преобразования в datetime.
            errors (str, optional): Как обрабатывать ошибки при преобразовании. По умолчанию 'raise'.
                'raise' - вызывать исключение, 'coerce' - устанавливать NaT для ошибочных значений.
            infer_datetime_format (bool, optional): Пытаться ли вывести формат даты/времени из строк.
                По умолчанию True.
        
        Raises:
            KeyError: Если столбец time_col не найден в DataFrame.
            ValueError: Если произошла ошибка при преобразовании в datetime и errors='raise'.
        """
        try:
            if time_col not in self.df.columns:
                raise KeyError(f"Столбец '{time_col}' не найден в DataFrame")
            
            # Сохраняем изначальный размер данных для отчета
            orig_shape = self.df.shape
            
            # Конвертируем столбец в datetime
            self.df[time_col] = pd.to_datetime(
                self.df[time_col], 
                errors=errors, 
                infer_datetime_format=infer_datetime_format
            )
            
            # Устанавливаем как индекс
            self.df.set_index(time_col, inplace=True)
            
            self._log(f"Столбец '{time_col}' преобразован в datetime и установлен как индекс DataFrame")
            
            # Если после преобразования изменилось количество строк, логируем это
            if orig_shape[0] != self.df.shape[0]:
                self._log(f"Внимание: размер DataFrame изменился с {orig_shape} на {self.df.shape}")
                
        except Exception as e:
            self._log(f"Ошибка при установке datetime индекса: {str(e)}")
            raise
            
    def handle_missing_values(
            self, 
            strategy: str = 'ffill', 
            subset_cols: Optional[List[str]] = None, 
            fill_value: Any = None,
            method: str = 'linear'
        ) -> None:
        """
        Обрабатывает пропуски (NaN) в DataFrame различными методами.
        
        Args:
            strategy (str, optional): Метод обработки пропусков. По умолчанию 'ffill'.
                Допустимые значения:
                - 'ffill': Заполнение вперед
                - 'bfill': Заполнение назад
                - 'dropna_rows': Удаление строк с пропусками
                - 'dropna_cols': Удаление столбцов с пропусками
                - 'interpolate': Линейная интерполяция
                - 'fill_constant': Заполнение константой из fill_value
                - 'fill_mean': Заполнение средним по столбцам
                - 'fill_median': Заполнение медианой по столбцам
                - 'fill_zero': Заполнение нулями
            subset_cols (List[str], optional): Список столбцов, к которым применить стратегию.
                Если None, стратегия применяется ко всем столбцам. По умолчанию None.
            fill_value (Any, optional): Значение для заполнения при strategy='fill_constant'.
                По умолчанию None.
            method (str, optional): Метод интерполяции для strategy='interpolate'.
                По умолчанию 'linear'.
                
        Raises:
            ValueError: Если указана неподдерживаемая стратегия или некорректные параметры.
        """
        try:
            # Проверка корректности стратегии
            valid_strategies = [
                'ffill', 'bfill', 'dropna_rows', 'dropna_cols', 'interpolate',
                'fill_constant', 'fill_mean', 'fill_median', 'fill_zero'
            ]
            
            if strategy not in valid_strategies:
                raise ValueError(f"Стратегия '{strategy}' не поддерживается. "
                              f"Допустимые стратегии: {', '.join(valid_strategies)}")
            
            # Проверка subset_cols
            if subset_cols:
                for col in subset_cols:
                    if col not in self.df.columns:
                        raise ValueError(f"Столбец '{col}' не найден в DataFrame")
                
                # Создаем временную копию для подсчета пропусков
                temp_df = self.df[subset_cols] if subset_cols else self.df
            else:
                temp_df = self.df
            
            # Подсчет пропущенных значений перед обработкой
            missing_before = temp_df.isna().sum().sum()
            
            # Применяем выбранную стратегию
            if strategy == 'ffill':
                if subset_cols:
                    self.df[subset_cols] = self.df[subset_cols].fillna(method='ffill')
                else:
                    self.df = self.df.fillna(method='ffill')
                strategy_desc = "заполнение вперед (ffill)"
                
            elif strategy == 'bfill':
                if subset_cols:
                    self.df[subset_cols] = self.df[subset_cols].fillna(method='bfill')
                else:
                    self.df = self.df.fillna(method='bfill')
                strategy_desc = "заполнение назад (bfill)"
                
            elif strategy == 'dropna_rows':
                orig_shape = self.df.shape
                if subset_cols:
                    self.df = self.df.dropna(subset=subset_cols)
                else:
                    self.df = self.df.dropna()
                strategy_desc = f"удаление строк с пропусками (удалено {orig_shape[0] - self.df.shape[0]} строк)"
                
            elif strategy == 'dropna_cols':
                orig_shape = self.df.shape
                if subset_cols:
                    # Для subset_cols удаляем только те колонки, в которых есть NaN
                    cols_to_keep = [col for col in self.df.columns if col not in subset_cols or not self.df[col].isna().any()]
                    self.df = self.df[cols_to_keep]
                else:
                    self.df = self.df.dropna(axis=1)
                strategy_desc = f"удаление столбцов с пропусками (удалено {orig_shape[1] - self.df.shape[1]} столбцов)"
                
            elif strategy == 'interpolate':
                if subset_cols:
                    self.df[subset_cols] = self.df[subset_cols].interpolate(method=method)
                else:
                    self.df = self.df.interpolate(method=method)
                strategy_desc = f"интерполяция методом '{method}'"
                
            elif strategy == 'fill_constant':
                if fill_value is None:
                    raise ValueError("При использовании стратегии 'fill_constant' необходимо указать значение fill_value")
                
                if subset_cols:
                    self.df[subset_cols] = self.df[subset_cols].fillna(fill_value)
                else:
                    self.df = self.df.fillna(fill_value)
                strategy_desc = f"заполнение константой {fill_value}"
                
            elif strategy == 'fill_mean':
                for col in (subset_cols if subset_cols else self.df.columns):
                    if pd.api.types.is_numeric_dtype(self.df[col]):
                        self.df[col] = self.df[col].fillna(self.df[col].mean())
                strategy_desc = "заполнение средним"
                
            elif strategy == 'fill_median':
                for col in (subset_cols if subset_cols else self.df.columns):
                    if pd.api.types.is_numeric_dtype(self.df[col]):
                        self.df[col] = self.df[col].fillna(self.df[col].median())
                strategy_desc = "заполнение медианой"
                
            elif strategy == 'fill_zero':
                if subset_cols:
                    self.df[subset_cols] = self.df[subset_cols].fillna(0)
                else:
                    self.df = self.df.fillna(0)
                strategy_desc = "заполнение нулями"
            
            # Подсчет пропущенных значений после обработки
            if strategy in ['dropna_rows', 'dropna_cols']:
                # В случае удаления строк или столбцов temp_df уже не актуален
                # поэтому просто подсчитаем пропуски в текущем DataFrame для тех же столбцов
                cols_to_check = subset_cols if subset_cols and all(col in self.df.columns for col in subset_cols) else self.df.columns
                missing_after = self.df[cols_to_check].isna().sum().sum()
            else:
                temp_after = self.df[subset_cols] if subset_cols else self.df
                missing_after = temp_after.isna().sum().sum()
            
            # Логирование результатов
            cols_desc = f" для столбцов {subset_cols}" if subset_cols else ""
            self._log(f"Обработаны пропуски{cols_desc} стратегией: {strategy_desc}")
            self._log(f"Количество пропусков до: {missing_before}, после: {missing_after}")
            
        except Exception as e:
            self._log(f"Ошибка при обработке пропущенных значений: {str(e)}")
            raise
    
    def convert_data_types(self, column_types: Dict[str, str], errors: str = 'raise') -> None:
        """
        Преобразует типы данных указанных колонок.
        
        Args:
            column_types (Dict[str, str]): Словарь, где ключ - имя колонки, значение - целевой тип данных.
                Поддерживаемые типы: 'float', 'int', 'str', 'category', 'bool', 'datetime'.
            errors (str, optional): Как обрабатывать ошибки при преобразовании.
                'raise' - вызывать исключение, 'ignore' - пропускать ошибочные колонки.
                По умолчанию 'raise'.
                
        Raises:
            KeyError: Если колонка не найдена в DataFrame и errors='raise'.
            ValueError: Если тип не поддерживается или возникла ошибка при преобразовании.
        """
        try:
            # Проверим, что все запрашиваемые колонки существуют
            missing_columns = [col for col in column_types.keys() if col not in self.df.columns]
            if missing_columns and errors == 'raise':
                raise KeyError(f"Следующие колонки не найдены в DataFrame: {missing_columns}")
            
            # Для каждой колонки применяем указанное преобразование типа
            for col, target_type in column_types.items():
                if col not in self.df.columns:
                    if errors == 'ignore':
                        self._log(f"Колонка '{col}' не найдена, пропускаю (errors=ignore)")
                        continue
                
                # Запоминаем исходный тип
                original_dtype = str(self.df[col].dtype)
                
                try:
                    # Обрабатываем по типам
                    if target_type == 'float':
                        self.df[col] = self.df[col].astype(float, errors=errors)
                    elif target_type == 'int':
                        # Для int подход немного сложнее из-за NaN
                        if self.df[col].isna().any():
                            # Int64 может содержать NaN
                            self.df[col] = self.df[col].astype('Int64', errors=errors)
                        else:
                            self.df[col] = self.df[col].astype(int, errors=errors)
                    elif target_type == 'str':
                        self.df[col] = self.df[col].astype(str, errors=errors)
                    elif target_type == 'category':
                        self.df[col] = self.df[col].astype('category', errors=errors)
                    elif target_type == 'bool':
                        self.df[col] = self.df[col].astype(bool, errors=errors)
                    elif target_type == 'datetime':
                        self.df[col] = pd.to_datetime(self.df[col], errors=errors)
                    else:
                        raise ValueError(f"Неподдерживаемый тип данных: '{target_type}'")
                    
                    new_dtype = str(self.df[col].dtype)
                    self._log(f"Преобразован тип колонки '{col}': {original_dtype} -> {new_dtype}")
                    
                except Exception as e:
                    if errors == 'raise':
                        raise ValueError(f"Ошибка при преобразовании колонки '{col}' в {target_type}: {str(e)}")
                    else:
                        self._log(f"Не удалось преобразовать '{col}' в {target_type}: {str(e)}. Пропускаю (errors=ignore)")
                
        except Exception as e:
            self._log(f"Ошибка при преобразовании типов данных: {str(e)}")
            raise
    
    def remove_duplicate_rows(self, subset: Optional[List[str]] = None, keep: str = 'first') -> None:
        """
        Удаляет дублирующиеся строки в DataFrame.
        
        Args:
            subset (List[str], optional): Список колонок, по которым определяются дубликаты.
                Если None, используются все колонки. По умолчанию None.
            keep (str, optional): Определяет, какие дубликаты оставлять.
                'first' - первый, 'last' - последний, False - удалять все дубликаты.
                По умолчанию 'first'.
                
        Raises:
            ValueError: Если указаны несуществующие колонки в subset.
        """
        try:
            # Проверка корректности subset
            if subset:
                for col in subset:
                    if col not in self.df.columns:
                        raise ValueError(f"Столбец '{col}' не найден в DataFrame")
            
            # Запоминаем исходное количество строк
            orig_count = len(self.df)
            
            # Удаляем дубликаты
            self.df = self.df.drop_duplicates(subset=subset, keep=keep)
            
            # Подсчитываем удаленные строки
            removed_count = orig_count - len(self.df)
            
            # Логируем результат
            subset_desc = f"по колонкам {subset}" if subset else "по всем колонкам"
            keep_desc = {
                'first': "с сохранением первого вхождения",
                'last': "с сохранением последнего вхождения",
                False: "с удалением всех дубликатов"
            }.get(keep, "")
            
            self._log(f"Удалено {removed_count} дублирующихся строк {subset_desc} {keep_desc}")
            
        except Exception as e:
            self._log(f"Ошибка при удалении дублирующихся строк: {str(e)}")
            raise
    
    def rename_columns(self, rename_map: Dict[str, str]) -> None:
        """
        Переименовывает колонки DataFrame согласно указанному словарю.
        
        Args:
            rename_map (Dict[str, str]): Словарь для переименования колонок,
                где ключи - текущие имена, значения - новые имена.
                
        Raises:
            KeyError: Если указаны несуществующие колонки для переименования.
        """
        try:
            # Проверим, что все переименовываемые колонки существуют
            missing_columns = [col for col in rename_map.keys() if col not in self.df.columns]
            if missing_columns:
                raise KeyError(f"Следующие колонки для переименования не найдены: {missing_columns}")
            
            # Переименовываем колонки
            self.df = self.df.rename(columns=rename_map)
            
            # Логируем результат
            renamed_pairs = [f"'{old}' -> '{new}'" for old, new in rename_map.items()]
            self._log(f"Переименованы колонки: {', '.join(renamed_pairs)}")
            
        except Exception as e:
            self._log(f"Ошибка при переименовании колонок: {str(e)}")
            raise
    
    def select_columns(self, columns_to_keep: List[str]) -> None:
        """
        Оставляет только указанные колонки в DataFrame.
        
        Args:
            columns_to_keep (List[str]): Список колонок, которые нужно оставить.
            
        Raises:
            ValueError: Если указаны несуществующие колонки.
        """
        try:
            # Проверим, что все запрашиваемые колонки существуют
            missing_columns = [col for col in columns_to_keep if col not in self.df.columns]
            if missing_columns:
                raise ValueError(f"Следующие колонки не найдены: {missing_columns}")
            
            # Запоминаем исходные колонки
            original_columns = set(self.df.columns)
            
            # Выбираем только нужные колонки
            self.df = self.df[columns_to_keep]
            
            # Логируем результат
            removed_columns = original_columns - set(columns_to_keep)
            self._log(f"Оставлены колонки: {columns_to_keep}")
            
            if removed_columns:
                self._log(f"Удалены колонки: {list(removed_columns)}")
            
        except Exception as e:
            self._log(f"Ошибка при выборе колонок: {str(e)}")
            raise

    def apply_custom_function(self, func: Callable[[pd.DataFrame], pd.DataFrame], *args, **kwargs) -> None:
        """
        Применяет пользовательскую функцию к DataFrame.
        
        Args:
            func (Callable[[pd.DataFrame], pd.DataFrame]): Функция, которая принимает DataFrame
                и возвращает обработанный DataFrame.
            *args, **kwargs: Дополнительные аргументы для передачи в функцию func.
                
        Raises:
            Exception: Если функция вызвала ошибку или вернула неверный тип.
        """
        try:
            # Запоминаем исходное состояние DataFrame
            orig_shape = self.df.shape
            orig_dtypes = self.df.dtypes.to_dict()
            
            # Применяем пользовательскую функцию
            result = func(self.df, *args, **kwargs)
            
            # Проверяем, что результат - DataFrame
            if not isinstance(result, pd.DataFrame):
                raise ValueError(f"Пользовательская функция вернула {type(result)}, "
                                f"ожидался pd.DataFrame")
            
            # Обновляем DataFrame
            self.df = result
            
            # Логируем изменения
            if orig_shape != self.df.shape:
                self._log(f"Размер DataFrame изменился после пользовательской функции: "
                       f"{orig_shape} -> {self.df.shape}")
            
            # Проверяем изменение типов колонок
            new_dtypes = self.df.dtypes.to_dict()
            changed_dtypes = {col: (orig_dtypes[col], new_dtypes[col]) 
                             for col in set(orig_dtypes).intersection(new_dtypes)
                             if orig_dtypes[col] != new_dtypes[col]}
            
            if changed_dtypes:
                changes = [f"'{col}': {old} -> {new}" 
                          for col, (old, new) in changed_dtypes.items()]
                self._log(f"Изменились типы следующих колонок: {', '.join(changes)}")
            
            # Проверяем новые и удаленные колонки
            new_columns = set(new_dtypes) - set(orig_dtypes)
            if new_columns:
                self._log(f"Добавлены колонки: {list(new_columns)}")
                
            removed_columns = set(orig_dtypes) - set(new_dtypes)
            if removed_columns:
                self._log(f"Удалены колонки: {list(removed_columns)}")
            
            self._log("Пользовательская функция успешно применена")
            
        except Exception as e:
            self._log(f"Ошибка при применении пользовательской функции: {str(e)}")
            raise
    
    def get_processed_df(self) -> pd.DataFrame:
        """
        Возвращает обработанный DataFrame.
        
        Returns:
            pd.DataFrame: Обработанный DataFrame.
        """
        self._log(f"Получен обработанный DataFrame размером {self.df.shape}")
        return self.df.copy()
    
    def get_log_messages(self) -> List[str]:
        """
        Возвращает список сообщений лога операций.
        
        Returns:
            List[str]: Список сообщений лога.
        """
        return self.log_messages.copy()