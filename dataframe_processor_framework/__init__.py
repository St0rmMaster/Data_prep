#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DataFrameProcessorFramework
---------------------------
Фреймворк для интерактивной обработки Pandas DataFrame в среде Google Colab.

Основные компоненты:
- DataProcessorUI: Класс для создания интерактивного UI в Colab для обработки DataFrame.
- DataFrameProcessor: Класс с логикой обработки DataFrame.
- DriveIOHandler: Класс для сохранения DataFrame на Google Drive.
- launch_processor_ui: Функция для инициализации и отображения UI.
"""

from .colab_processor_ui import DataProcessorUI
from .data_processor import DataFrameProcessor
from .drive_io_handler import DriveIOHandler
from typing import Optional, List, Dict, Any, Callable

import pandas as pd
import ipywidgets as widgets
from IPython.display import display


def launch_processor_ui(input_df: pd.DataFrame) -> DataProcessorUI:
    """
    Инициализирует и отображает DataProcessorUI в среде Google Colab.
    
    Принимает входной DataFrame и возвращает экземпляр UI,
    который после обработки будет содержать processed_df.
    
    Args:
        input_df (pd.DataFrame): Исходный DataFrame для обработки.
        
    Returns:
        DataProcessorUI: Экземпляр интерфейса для обработки данных.
        
    Example:
        >>> import pandas as pd
        >>> from dataframe_processor_framework import launch_processor_ui
        >>> 
        >>> # Создаем тестовый DataFrame
        >>> df = pd.read_csv('my_data.csv')
        >>> 
        >>> # Запускаем UI обработчика
        >>> ui_instance = launch_processor_ui(df)
        >>> 
        >>> # После работы с UI и нажатия кнопки "Обработать и сохранить"
        >>> # можно получить обработанный DataFrame:
        >>> processed_df = ui_instance.get_final_dataframe()
    """
    try:
        # В Colab включаем поддержку кастомных виджетов
        from google.colab import output
        output.enable_custom_widget_manager()
    except ImportError:
        print("Warning: 'google.colab.output' could not be imported. "
              "Custom widget manager not enabled. "
              "This UI is designed for Colab environment.")
    
    # Создаем и отображаем UI
    ui_instance = DataProcessorUI(input_df=input_df)
    ui_instance.display()
    
    return ui_instance


# Определяем, какие имена будут доступны при импорте
__all__ = ['DataProcessorUI', 'DataFrameProcessor', 'DriveIOHandler', 'launch_processor_ui']