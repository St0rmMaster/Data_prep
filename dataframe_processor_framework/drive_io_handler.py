#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для сохранения DataFrame на Google Drive в фреймворке DataFrameProcessorFramework.
"""

import os
import pandas as pd
import json
from datetime import datetime
from typing import Optional, Dict, Any, Union


class DriveIOHandler:
    """
    Класс для управления сохранением DataFrame на Google Drive.
    """
    
    def __init__(self, base_save_path: str = '/content/drive/MyDrive/processed_data/'):
        """
        Инициализирует обработчик ввода-вывода для Google Drive.
        
        Args:
            base_save_path (str, optional): Базовый путь для сохранения файлов на Google Drive.
                По умолчанию '/content/drive/MyDrive/processed_data/'.
        """
        self.base_save_path = base_save_path
        # Проверяем и создаем директорию, если она не существует
        try:
            if not os.path.exists(self.base_save_path):
                os.makedirs(self.base_save_path, exist_ok=True)
                print(f"Создана директория для сохранения: {self.base_save_path}")
        except Exception as e:
            print(f"Предупреждение: не удалось создать директорию {self.base_save_path}: {str(e)}")

    def _ensure_drive_mounted(self) -> bool:
        """
        Проверяет, смонтирован ли Google Drive и пытается смонтировать его при необходимости.
        
        Returns:
            bool: True, если диск успешно смонтирован или уже был смонтирован, False в случае ошибки.
            
        Note:
            В среде вне Colab этот метод просто проверяет доступность base_save_path.
        """
        try:
            # Проверяем наличие пути Google Drive
            if '/content/drive' in self.base_save_path and not os.path.exists('/content/drive'):
                try:
                    # Импортируем библиотеку google.colab.drive только в среде Colab
                    from google.colab import drive
                    print("Монтирую Google Drive...")
                    drive.mount('/content/drive', force_remount=True)
                    print("Google Drive успешно смонтирован")
                except ImportError:
                    print("Предупреждение: google.colab.drive не найден. "
                         "Убедитесь, что вы работаете в Google Colab и диск смонтирован вручную.")
                    return False
                except Exception as e:
                    print(f"Ошибка при монтировании Google Drive: {str(e)}")
                    return False
                
            # Проверяем наличие директории для сохранения
            if not os.path.exists(self.base_save_path):
                try:
                    os.makedirs(self.base_save_path, exist_ok=True)
                    print(f"Создана директория для сохранения: {self.base_save_path}")
                except Exception as e:
                    print(f"Не удалось создать директорию {self.base_save_path}: {str(e)}")
                    return False
            
            return True
        
        except Exception as e:
            print(f"Ошибка при проверке/монтировании Google Drive: {str(e)}")
            return False

    def save_dataframe(
            self, 
            df: pd.DataFrame, 
            filename_prefix: str = 'processed_df',
            file_format: str = 'parquet',
            include_timestamp: bool = True,
            custom_metadata: Optional[Dict[str, Any]] = None
        ) -> str:
        """
        Сохраняет DataFrame на Google Drive.
        
        Args:
            df (pd.DataFrame): DataFrame для сохранения.
            filename_prefix (str, optional): Префикс имени файла. По умолчанию 'processed_df'.
            file_format (str, optional): Формат сохранения файла ('parquet', 'csv', 'feather', 'pickle').
                По умолчанию 'parquet'.
            include_timestamp (bool, optional): Добавлять ли временную метку к имени файла.
                По умолчанию True.
            custom_metadata (Dict[str, Any], optional): Дополнительные метаданные для сохранения
                в отдельный JSON-файл. По умолчанию None.
                
        Returns:
            str: Полный путь к сохраненному файлу или пустая строка в случае ошибки.
            
        Raises:
            ValueError: Если указан неподдерживаемый формат файла.
            IOError: Если возникла ошибка при сохранении файла.
        """
        try:
            # Проверяем, смонтирован ли Drive
            if not self._ensure_drive_mounted():
                print("Ошибка: Google Drive не смонтирован или недоступен")
                return ""
            
            # Проверяем поддерживаемые форматы
            valid_formats = ['parquet', 'csv', 'feather', 'pickle']
            if file_format not in valid_formats:
                raise ValueError(f"Неподдерживаемый формат файла: '{file_format}'. "
                              f"Поддерживаемые форматы: {', '.join(valid_formats)}")
            
            # Формируем имя файла
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') if include_timestamp else ''
            if timestamp:
                filename = f"{filename_prefix}_{timestamp}.{file_format}"
            else:
                filename = f"{filename_prefix}.{file_format}"
                
            # Формируем полный путь
            full_path = os.path.join(self.base_save_path, filename)
            
            # Сохраняем DataFrame в выбранном формате
            if file_format == 'parquet':
                df.to_parquet(full_path, index=True)
            elif file_format == 'csv':
                df.to_csv(full_path, index=True)
            elif file_format == 'feather':
                # Feather не поддерживает сохранение индекса напрямую
                df_with_idx = df.reset_index()
                df_with_idx.to_feather(full_path)
            elif file_format == 'pickle':
                df.to_pickle(full_path)
            
            print(f"DataFrame сохранен в файл: {full_path}")
            
            # Сохраняем метаданные, если они предоставлены
            if custom_metadata:
                metadata_filename = f"{filename_prefix}_{timestamp}_metadata.json" if timestamp else f"{filename_prefix}_metadata.json"
                metadata_path = os.path.join(self.base_save_path, metadata_filename)
                
                # Добавляем базовую информацию о DataFrame
                metadata = {
                    'file_format': file_format,
                    'timestamp': str(datetime.now()),
                    'shape': list(df.shape),
                    'columns': list(df.columns),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                    **custom_metadata
                }
                
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                    
                print(f"Метаданные сохранены в файл: {metadata_path}")
            
            return full_path
        
        except Exception as e:
            print(f"Ошибка при сохранении DataFrame: {str(e)}")
            return ""