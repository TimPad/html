"""
DataCulture Unified Platform
Объединённое приложение для инструментов Data Culture @ HSE University
Автор: Тимошка
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
import json
from openai import OpenAI
import tempfile
import os
from typing import Dict, Tuple
from supabase import create_client, Client
from streamlit import navigation


# =============================================================================
# КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ
# =============================================================================

st.set_page_config(
    page_title="DataCulture Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# КОНСТАНТЫ
# =============================================================================

LOGO_URL = "https://raw.githubusercontent.com/TimPad/html/main/DC_green.svg"

HTML_EXAMPLE = f"""<div style="font-family: 'Inter', 'Segoe UI', Roboto, Arial, sans-serif; max-width: 860px; margin: 40px auto; background: #ffffff; border-radius: 16px; box-shadow: 0 4px 14px rgba(0,0,0,0.08); border: 1px solid #e5ebf8; overflow: hidden;">
    <div style="background: #00256c; color: white; padding: 28px 32px; text-align: center;">
        <img src="{LOGO_URL}" alt="Логотип Data Culture" style="height: 48px; margin-bottom: 16px;">
        <p><span style="font-size: 1.6em; font-weight: 700;">ЗАГОЛОВОК ОБЪЯВЛЕНИЯ</span></p>
        <p style="margin-top: 8px; line-height: 1.5;">Краткое введение или контекст.</p>
    </div>
    <div style="padding: 28px 32px; color: #111827; line-height: 1.65;">
        <p>Основной текст объявления...</p>
        <h3 style="color: #00256c;">Подзаголовок</h3>
        <ul style="margin: 12px 0 22px 22px;">
            <li>Пункт списка</li>
        </ul>
        <div style="background: #f4f6fb; border-radius: 10px; padding: 16px 20px; margin: 16px 0;">
            <p>Информационный блок</p>
        </div>
        <div style="background: #fff8e1; border-left: 4px solid #f59e0b; padding: 14px 18px; border-radius: 8px; margin-bottom: 20px;">
            <p style="margin: 0; font-weight: 600; color: #92400e;">⚠️ Внимание! Важное уточнение.</p>
        </div>
        <div style="background: #f0fdf4; border-left: 4px solid #16a34a; padding: 16px 20px; border-radius: 8px;">
            <p style="margin: 4px 0 0;"><strong>Удачи!</strong> 🚀</p>
        </div>
    </div>
</div>"""

SYSTEM_MESSAGE = (
    "Вы — эксперт по оформлению официальных рассылок НИУ ВШЭ. "
    "Ваша задача — преобразовать входной текст объявления в HTML-карточку, строго следуя фирменному стилю. "
    "В шапке обязательно должен быть логотип по ссылке: " + LOGO_URL + ". "
    "Используйте структуру и CSS-стили из приведённого ниже примера. "
    "Не добавляйте пояснений, комментариев или лишних тегов. "
    "Верните ТОЛЬКО корректный JSON в формате: {\"type\": \"HTML\", \"content\": \"<div>...</div>\"}.\n\n"
    "Пример корректного вывода:\n"
    + json.dumps({"type": "HTML", "content": HTML_EXAMPLE}, ensure_ascii=False, indent=2)
)

# =============================================================================
# ФУНКЦИИ ДЛЯ МОДУЛЯ 1: ПЕРЕЗАЧЕТ ОЦЕНОК
# =============================================================================

def process_grade_recalculation(df: pd.DataFrame, use_dynamics: bool) -> pd.DataFrame:
    """
    Обработка данных для перезачета оценок
    
    Args:
        df: DataFrame с данными студентов
        use_dynamics: Учитывать ли динамику оценок
        
    Returns:
        Обработанный DataFrame с колонками ДПР_итог и НЭ_итог
    """
    processed_df = df.copy()

    required_columns = [
        'Наименование НЭ', 'Оценка НЭ', 'Оценка дисциплины-пререквизита',
        'Внешнее измерение цифровых компетенций. Входной контроль',
        'Внешнее измерение цифровых компетенций. Промежуточный контроль',
        'Внешнее измерение цифровых компетенций. Итоговый контроль'
    ]
    
    for col in required_columns:
        if col not in processed_df.columns:
            raise KeyError(f"Отсутствует обязательный столбец: '{col}'")

    processed_df['Оценка дисциплины-пререквизита'] = processed_df['Оценка дисциплины-пререквизита'].apply(
        lambda x: 8 if x >= 9 else x
    )

    processed_df['Этап'] = 1
    processed_df.loc[processed_df['Наименование НЭ'].str.contains('анализу данных', case=False, na=False), 'Этап'] = 3
    processed_df.loc[processed_df['Наименование НЭ'].str.contains('программированию', case=False, na=False), 'Этап'] = 2

    dpr_results = []
    ie_results = []

    for index, row in processed_df.iterrows():
        if row['Этап'] == 1:
            innopolis_grade = row['Внешнее измерение цифровых компетенций. Входной контроль']
        elif row['Этап'] == 2:
            innopolis_grade = row['Внешнее измерение цифровых компетенций. Промежуточный контроль']
        else:
            innopolis_grade = row['Внешнее измерение цифровых компетенций. Итоговый контроль']

        if use_dynamics:
            vhod = row['Внешнее измерение цифровых компетенций. Входной контроль']
            prom = row['Внешнее измерение цифровых компетенций. Промежуточный контроль']
            itog = row['Внешнее измерение цифровых компетенций. Итоговый контроль']
            
            if (vhod - prom > 1) or (vhod - itog > 1) or (prom - itog > 1):
                dpr_results.append(np.nan)
                ie_results.append(np.nan)
                continue

        ne_grade = row['Оценка НЭ']
        dpr_grade = row['Оценка дисциплины-пререквизита']
        
        ne_grade = 0 if pd.isna(ne_grade) else ne_grade
        dpr_grade = 0 if pd.isna(dpr_grade) else dpr_grade
        innopolis_grade = 0 if pd.isna(innopolis_grade) else innopolis_grade

        max_grade = max(ne_grade, dpr_grade, innopolis_grade)
        
        # Расчет ДПР_итог
        dpr_final = np.nan
        if ne_grade < 4:
            dpr_final = np.nan
        elif max_grade == innopolis_grade and innopolis_grade > 3 and innopolis_grade != dpr_grade and innopolis_grade != ne_grade:
            dpr_final = innopolis_grade
        elif ne_grade == dpr_grade:
            dpr_final = np.nan
        elif dpr_grade < 4:
            dpr_final = ne_grade if ne_grade >= 4 else np.nan
        elif max_grade == dpr_grade and dpr_grade >= 4:
            dpr_final = np.nan
        else:
            dpr_final = ne_grade
        dpr_results.append(dpr_final)

        # Расчет НЭ_итог
        ie_final = np.nan
        if ne_grade < 4:
            ie_final = np.nan
        elif max_grade == innopolis_grade and innopolis_grade > 3 and innopolis_grade != dpr_grade and innopolis_grade != ne_grade:
            ie_final = innopolis_grade
        elif ne_grade == dpr_grade:
            ie_final = np.nan
        elif max_grade == dpr_grade and dpr_grade >= 4:
            if ne_grade >= 8:
                ie_final = np.nan
            elif dpr_grade >= 8:
                ie_final = 8
            else:
                ie_final = dpr_grade
        elif ne_grade < 4 and innopolis_grade > 3 and use_dynamics:
             ie_final = innopolis_grade
        else:
            ie_final = np.nan
        ie_results.append(ie_final)

    processed_df['ДПР_итог'] = dpr_results
    processed_df['НЭ_итог'] = ie_results
    
    return processed_df

# =============================================================================
# ФУНКЦИИ ДЛЯ МОДУЛЯ 2: ГЕНЕРАТОР HTML-КАРТОЧЕК
# =============================================================================

@st.cache_resource
def get_nebius_client():
    """Получить клиент Nebius API"""
    if "NEBIUS_API_KEY" not in st.secrets:
        raise ValueError("NEBIUS_API_KEY не найден в secrets.")
    return OpenAI(
        base_url="https://api.studio.nebius.com/v1/",
        api_key=st.secrets["NEBIUS_API_KEY"]
    )

def generate_hse_html(client: OpenAI, user_text: str) -> str:
    """
    Генерация HTML-карточки через Nebius API
    
    Args:
        client: OpenAI клиент
        user_text: Текст объявления
        
    Returns:
        HTML-код карточки
    """
    response = client.chat.completions.create(
        model="Qwen/Qwen3-Coder-30B-A3B-Instruct",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_MESSAGE},
            {"role": "user", "content": user_text}
        ],
        timeout=60.0
    )

    raw_content = response.choices[0].message.content.strip()

    try:
        parsed = json.loads(raw_content)
    except json.JSONDecodeError:
        raise ValueError(f"Модель вернула не-JSON. Ответ:\n{raw_content[:500]}")

    if not isinstance(parsed, dict):
        raise ValueError("Ответ не является объектом JSON.")

    if parsed.get("type") != "HTML":
        raise ValueError("Поле 'type' должно быть 'HTML'.")

    content = parsed.get("content")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("Поле 'content' отсутствует или пустое.")

    return content.strip()

# =============================================================================
# ФУНКЦИИ ДЛЯ МОДУЛЯ 3: ОБРАБОТКА СЕРТИФИКАТОВ
# =============================================================================

def deduplicate_lines(text):
    """Удаляет дублирующиеся строки из текста"""
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    lines = text.split('\n')
    seen_lines = set()
    unique_lines = []
    
    for line in lines:
        line_clean = line.strip()
        if line_clean and line_clean not in seen_lines:
            seen_lines.add(line_clean)
            unique_lines.append(line)
    
    return '\n'.join(unique_lines)

@st.cache_data
def load_reference_data(skills_content: bytes) -> Dict[str, str]:
    """Загрузка справочных данных из файла навыков"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
        tmp_file.write(skills_content)
        tmp_file_path = tmp_file.name
    
    try:
        skills_df = pd.read_excel(tmp_file_path)
        
        grade_mapping = {}
        for _, row in skills_df.iterrows():
            discipline = row['Дисциплина']
            level = row['Уровень_оценки']
            description = row['Описание_навыков']
            clean_description = deduplicate_lines(description)
            composite_key = f"{discipline}—{level}"
            grade_mapping[composite_key] = clean_description
        
        return grade_mapping
    finally:
        os.unlink(tmp_file_path)

# =============================================================================
# ФУНКЦИИ ДЛЯ МОДУЛЯ 4: ОБРАБОТКА ПЕРЕСДАЧ ВНЕШНЕЙ ОЦЕНКИ
# =============================================================================

@st.cache_resource
def get_supabase_client() -> Client:
    """Получить клиент Supabase"""
    if "url" not in st.secrets or "key" not in st.secrets:
        raise ValueError("Supabase URL и KEY не найдены в secrets.toml")
    return create_client(st.secrets["url"], st.secrets["key"])

def load_students_from_supabase() -> pd.DataFrame:
    """
    Загрузка списка студентов из Supabase (все записи с пагинацией)
    Автоматически переименовывает колонки в требуемый формат
    Фильтрует студентов по курсу = "Курс 4"
    
    Returns:
        DataFrame со студентами с переименованными колонками и фильтром по курсу
    """
    try:
        supabase = get_supabase_client()
        
        # Загружаем все записи с пагинацией и фильтром по курсу
        all_data = []
        page_size = 1000
        offset = 0
        
        while True:
            # Добавляем фильтр по курсу = "Курс 4"
            response = supabase.table('students').select('*').eq('курс', 'Курс 4').range(offset, offset + page_size - 1).execute()
            
            if response.data:
                all_data.extend(response.data)
                
                # Если получили меньше записей, чем page_size, значит это последняя страница
                if len(response.data) < page_size:
                    break
                    
                offset += page_size
            else:
                break
        
        if all_data:
            df = pd.DataFrame(all_data)
            
            # Переименование колонок из формата Supabase в требуемый формат
            column_mapping = {
                'корпоративная_почта': 'Адрес электронной почты',
                'фио': 'ФИО',
                'филиал_кампус': 'Филиал (кампус)',
                'факультет': 'Факультет',
                'образовательная_программа': 'Образовательная программа',
                'версия_образовательной_программы': 'Версия образовательной программы',
                'группа': 'Группа',
                'курс': 'Курс'
            }
            
            # Переименовываем только те колонки, которые существуют
            existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=existing_columns)
            
            return df
        else:
            st.warning("⚠️ Таблица students пуста или нет студентов на Курсе 4 в Supabase")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Ошибка при загрузке студентов из Supabase: {str(e)}")
        return pd.DataFrame()

def create_peresdachi_table_if_not_exists():
    """
    Создание таблицы peresdachi в Supabase (если не существует)
    Примечание: Таблица должна быть создана вручную в Supabase Dashboard
    """
    pass  # Таблица создается вручную в Supabase

def load_existing_peresdachi() -> pd.DataFrame:
    """
    Загрузка существующих записей из таблицы peresdachi (все записи с пагинацией)
    
    Returns:
        DataFrame с существующими пересдачами
    """
    try:
        supabase = get_supabase_client()
        
        # Загружаем все записи с пагинацией
        all_data = []
        page_size = 1000
        offset = 0
        
        while True:
            response = supabase.table('peresdachi').select('*').range(offset, offset + page_size - 1).execute()
            
            if response.data:
                all_data.extend(response.data)
                
                # Если получили меньше записей, чем page_size, значит это последняя страница
                if len(response.data) < page_size:
                    break
                    
                offset += page_size
            else:
                break
        
        if all_data:
            return pd.DataFrame(all_data)
        else:
            return pd.DataFrame()
    except Exception as e:
        # Если таблица не существует, возвращаем пустой DataFrame
        st.warning(f"⚠️ Таблица peresdachi не найдена или пуста: {str(e)}")
        return pd.DataFrame()

def save_to_supabase(df: pd.DataFrame) -> Tuple[int, int]:
    """
    Сохранение данных в таблицу peresdachi в Supabase
    
    Args:
        df: DataFrame с новыми данными
        
    Returns:
        Tuple (количество новых записей, общее количество записей)
    """
    try:
        supabase = get_supabase_client()
        
        # Загружаем существующие записи
        existing_df = load_existing_peresdachi()
        
        if existing_df.empty:
            # Если таблица пуста, все записи новые
            new_records = df.to_dict('records')
            new_count = len(new_records)
        else:
            # Определяем новые записи (по комбинации email + дисциплина + оценка)
            merge_cols = ['Адрес электронной почты', 'Наименование дисциплины']
            
            # Проверяем наличие необходимых колонок
            if all(col in existing_df.columns for col in merge_cols) and all(col in df.columns for col in merge_cols):
                # Находим новые записи
                merged = df.merge(
                    existing_df[merge_cols],
                    on=merge_cols,
                    how='left',
                    indicator=True
                )
                new_df = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
                new_records = new_df.to_dict('records')
                new_count = len(new_records)
            else:
                # Если колонки не совпадают, сохраняем все как новые
                new_records = df.to_dict('records')
                new_count = len(new_records)
        
        # Вставляем новые записи
        if new_records:
            # Удаляем пустые строки и NaN значения для корректной вставки
            cleaned_records = []
            for record in new_records:
                cleaned_record = {k: (v if pd.notna(v) else None) for k, v in record.items()}
                cleaned_records.append(cleaned_record)
            
            response = supabase.table('peresdachi').insert(cleaned_records).execute()
            
        return new_count, len(df)
        
    except Exception as e:
        st.error(f"❌ Ошибка при сохранении в Supabase: {str(e)}")
        raise e

def get_new_records(all_df: pd.DataFrame) -> pd.DataFrame:
    """
    Получить только новые записи, которых еще нет в базе
    
    Args:
        all_df: DataFrame со всеми обработанными записями
        
    Returns:
        DataFrame только с новыми записями
    """
    try:
        existing_df = load_existing_peresdachi()
        
        if existing_df.empty:
            return all_df
        
        merge_cols = ['Адрес электронной почты', 'Наименование дисциплины']
        
        if all(col in existing_df.columns for col in merge_cols) and all(col in all_df.columns for col in merge_cols):
            merged = all_df.merge(
                existing_df[merge_cols],
                on=merge_cols,
                how='left',
                indicator=True
            )
            new_df = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
            return new_df
        else:
            return all_df
            
    except Exception as e:
        st.warning(f"⚠️ Ошибка при определении новых записей: {str(e)}")
        return all_df

def process_external_assessment(grades_df: pd.DataFrame, students_df: pd.DataFrame) -> pd.DataFrame:
    """
    Обработка пересдач внешней оценки
    
    Args:
        grades_df: DataFrame с оценками из внешней системы
        students_df: DataFrame со списком студентов
        
    Returns:
        Обработанный DataFrame с итоговыми данными
    """
    # Шаг 1: Очистка данных - удаление "-" и лишних пробелов
    for col in grades_df.columns:
        if grades_df[col].dtype == 'object':
            grades_df[col] = grades_df[col].astype(str).str.replace('-', '', regex=False).str.strip()
    
    # Шаг 2: Переименование колонок
    column_mapping = {
        'Тест:Входное тестирование (Значение)': 'Внешнее измерение цифровых компетенций. Входной контроль',
        'Тест:Промежуточное тестирование (Значение)': 'Внешнее измерение цифровых компетенций. Промежуточный контроль',
        'Тест:Итоговое тестирование (Значение)': 'Внешнее измерение цифровых компетенций. Итоговый контроль'
    }
    
    grades_df = grades_df.rename(columns=column_mapping)
    
    # Шаг 3: Melt - преобразование колонок в строки
    value_columns = [
        'Внешнее измерение цифровых компетенций. Входной контроль',
        'Внешнее измерение цифровых компетенций. Промежуточный контроль',
        'Внешнее измерение цифровых компетенций. Итоговый контроль'
    ]
    
    # Определяем ID колонки для сохранения
    id_cols = ['Адрес электронной почты']
    if 'Адрес электронной почты' not in grades_df.columns:
        st.error("Колонка 'Адрес электронной почты' не найдена в файле оценок")
        return pd.DataFrame()
    
    melted_df = pd.melt(
        grades_df,
        id_vars=id_cols,
        value_vars=value_columns,
        var_name='Наименование дисциплины',
        value_name='Оценка'
    )
    
    # Шаг 4: Присоединение данных студентов по email
    students_cols = ['ФИО', 'Адрес электронной почты', 'Филиал (кампус)', 
                     'Факультет', 'Образовательная программа', 'Группа', 'Курс']
    
    # Проверка наличия колонок
    missing_cols = [col for col in students_cols if col not in students_df.columns]
    if missing_cols:
        st.warning(f"Отсутствуют колонки в файле студентов: {missing_cols}")
        available_cols = [col for col in students_cols if col in students_df.columns]
    else:
        available_cols = students_cols
    
    students_subset = students_df[available_cols].copy()
    
    # Очистка email в обоих файлах для корректного соединения
    melted_df['Адрес электронной почты'] = melted_df['Адрес электронной почты'].astype(str).str.strip().str.lower()
    students_subset['Адрес электронной почты'] = students_subset['Адрес электронной почты'].astype(str).str.strip().str.lower()
    
    result_df = melted_df.merge(
        students_subset,
        on='Адрес электронной почты',
        how='left'
    )
    
    # Шаг 5: Добавление пустых колонок
    result_df['ID дисциплины'] = ''
    result_df['Период аттестации'] = ''
    
    # Шаг 6: Переименование колонок согласно требованиям
    if 'Филиал (кампус)' in result_df.columns:
        result_df = result_df.rename(columns={'Филиал (кампус)': 'Кампус'})
    
    # Шаг 7: Упорядочивание колонок
    output_columns = [
        'ФИО', 'Адрес электронной почты', 'Кампус', 'Факультет',
        'Образовательная программа', 'Группа', 'Курс', 'ID дисциплины',
        'Наименование дисциплины', 'Период аттестации', 'Оценка'
    ]
    
    # Оставляем только существующие колонки
    final_columns = [col for col in output_columns if col in result_df.columns]
    result_df = result_df[final_columns]
    
    # Удаление строк с пустыми оценками или некорректными значениями
    result_df = result_df[result_df['Оценка'].notna()]
    result_df = result_df[result_df['Оценка'].astype(str).str.strip() != '']
    result_df = result_df[result_df['Оценка'].astype(str).str.strip() != 'nan']
    
    return result_df

def process_student_data(df: pd.DataFrame, grade_mapping: Dict[str, str]) -> Tuple[pd.DataFrame, list]:
    """Обработка данных студентов для сертификатов"""
    results = []
    processing_log = []
    
    processing_log.append(f"📊 Обрабатываем {len(df)} студентов")
    
    for index, row in df.iterrows():
        student_results = []
        processed_keys = set()
        
        for discipline_num in range(1, 4):
            discipline_col = f"Дисциплина {discipline_num}"
            grade_5_col = f"Оценка 5 баллов Дисциплина {discipline_num}"
            
            if discipline_col not in df.columns or grade_5_col not in df.columns:
                continue
                
            discipline_value = str(row[discipline_col]).strip()
            grade_value = str(row[grade_5_col]).strip()
            
            if pd.isna(discipline_value) or pd.isna(grade_value) or discipline_value == 'nan' or grade_value == 'nan':
                continue
            
            lookup_key = f"{discipline_value}—{grade_value}"
            
            if lookup_key in processed_keys:
                continue
            
            if lookup_key in grade_mapping:
                skill_description = grade_mapping[lookup_key]
                
                short_name_col = f"Название Дисциплины {discipline_num}"
                if short_name_col in df.columns:
                    display_name = str(row[short_name_col]).strip()
                    formatted_discipline = display_name.capitalize() if display_name != 'nan' and display_name else discipline_value
                else:
                    formatted_discipline = discipline_value
                
                formatted_result = f"📚 {formatted_discipline}:\n{skill_description}"
                student_results.append(formatted_result)
                processed_keys.add(lookup_key)
        
        final_result = "\n\n".join(student_results) if student_results else "Навыки не найдены."
        results.append(final_result)
    
    processing_log.append(f"✅ Успешно обработано")
    
    df_result = df.copy()
    df_result['Итоговый результат'] = results
    
    columns_to_remove = [col for col in df_result.columns if col.startswith("Название Дисциплины ")]
    if columns_to_remove:
        df_result = df_result.drop(columns=columns_to_remove)
    
    return df_result, processing_log

# =============================================================================
# ФУНКЦИИ ДЛЯ МОДУЛЯ 5: АНАЛИТИКА КУРСОВ
# =============================================================================

from io import StringIO
import time

def upload_students_to_supabase(supabase, student_data):
    """
    Загрузка данных студентов в таблицу students с использованием оптимизированного UPSERT
    """
    try:
        st.info("👥 Загрузка данных студентов (UPSERT)...")
        records_for_upsert = []
        processed_emails = set()
        
        for _, row in student_data.iterrows():
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email or '@edu.hse.ru' not in email:
                continue
            if email in processed_emails:
                continue
            processed_emails.add(email)
                
            student_record = {
                'корпоративная_почта': email,
                'фио': str(row.get('ФИО', 'Неизвестно')).strip() or 'Неизвестно',
                'филиал_кампус': str(row.get('Филиал (кампус)', '')) if pd.notna(row.get('Филиал (кампус)')) and str(row.get('Филиал (кампус)', '')).strip() else None,
                'факультет': str(row.get('Факультет', '')) if pd.notna(row.get('Факультет')) and str(row.get('Факультет', '')).strip() else None,
                'образовательная_программа': str(row.get('Образовательная программа', '')) if pd.notna(row.get('Образовательная программа')) and str(row.get('Образовательная программа', '')).strip() else None,
                'версия_образовательной_программы': str(row.get('Версия образовательной программы', '')) if pd.notna(row.get('Версия образовательной программы')) and str(row.get('Версия образовательной программы', '')).strip() else None,
                'группа': str(row.get('Группа', '')) if pd.notna(row.get('Группа')) and str(row.get('Группа', '')).strip() else None,
                'курс': str(row.get('Курс', '')) if pd.notna(row.get('Курс')) and str(row.get('Курс', '')).strip() else None,
            }
            records_for_upsert.append(student_record)
        
        if not records_for_upsert:
            st.info("📋 Нет записей для обработки")
            return True
        
        st.info(f"📋 Подготовлено {len(records_for_upsert)} записей для UPSERT")
        batch_size = 200
        total_processed = 0
        
        for i in range(0, len(records_for_upsert), batch_size):
            batch = records_for_upsert[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = ((len(records_for_upsert) - 1) // batch_size) + 1
            
            try:
                result = supabase.table('students').upsert(
                    batch,
                    on_conflict='корпоративная_почта',
                    ignore_duplicates=False,
                    returning='minimal'
                ).execute()
                total_processed += len(batch)
                st.success(f"✅ Батч {batch_num}/{total_batches}: обработано {len(batch)} записей")
            except Exception as e:
                error_str = str(e)
                if any(pat in error_str.lower() for pat in ["connection", "timeout", "ssl", "eof"]):
                    st.warning(f"⚠️ Сетевая ошибка в батче {batch_num}, повтор...")
                    time.sleep(2)
                    try:
                        result = supabase.table('students').upsert(batch, on_conflict='корпоративная_почта').execute()
                        total_processed += len(batch)
                        st.success(f"✅ Батч {batch_num} (после повтора)")
                    except Exception as retry_error:
                        st.error(f"❌ Батч {batch_num} не удался после повтора: {retry_error}")
                        return False
                else:
                    st.error(f"❌ Ошибка в батче {batch_num}: {e}")
                    return False
        
        st.success(f"🎉 UPSERT завершён! Обработано {total_processed} записей")
        return True
    except Exception as e:
        st.error(f"❌ Критическая ошибка UPSERT студентов: {e}")
        return False

def load_student_list_file(uploaded_file) -> pd.DataFrame:
    """
    Загрузка списка студентов из файла Excel или CSV
    """
    try:
        file_name = uploaded_file.name.lower()
        if file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif file_name.endswith('.csv'):
            content = uploaded_file.getvalue()
            try:
                df = pd.read_csv(StringIO(content.decode('utf-16')), sep='\t')
            except (UnicodeDecodeError, pd.errors.ParserError):
                try:
                    df = pd.read_csv(StringIO(content.decode('utf-8')))
                except UnicodeDecodeError:
                    df = pd.read_csv(StringIO(content.decode('cp1251')))
        else:
            st.error("Неподдерживаемый формат файла")
            return pd.DataFrame()

        required_columns = {
            'ФИО': ['фио', 'фio', 'имя', 'name'],
            'Корпоративная почта': ['адрес электронной почты', 'корпоративная почта', 'email', 'почта', 'e-mail'],
            'Филиал (кампус)': ['филиал', 'кампус', 'campus'],
            'Факультет': ['факультет', 'faculty'],
            'Образовательная программа': ['образовательная программа', 'программа', 'educational program'],
            'Версия образовательной программы': ['версия образовательной программы', 'версия программы', 'program version', 'version'],
            'Группа': ['группа', 'group'],
            'Курс': ['курс', 'course']
        }

        found_columns = {}
        df_columns_lower = [str(col).lower().strip() for col in df.columns]
        for target_col, possible_names in required_columns.items():
            for col_idx, col_name in enumerate(df_columns_lower):
                if any(possible_name in col_name for possible_name in possible_names):
                    found_columns[target_col] = df.columns[col_idx]
                    break

        result_df = pd.DataFrame()
        for target_col, source_col in found_columns.items():
            if source_col in df.columns:
                result_df[target_col] = df[source_col]

        if 'Данные о пользователе' in df.columns:
            user_data = df['Данные о пользователе'].astype(str)
            parsed_data = user_data.str.split(';', expand=True)
            if len(parsed_data.columns) >= 4:
                result_df['Факультет'] = parsed_data[0]
                result_df['Образовательная программа'] = parsed_data[1] 
                result_df['Курс'] = parsed_data[2]
                result_df['Группа'] = parsed_data[3]

        for required_col in required_columns.keys():
            if required_col not in result_df.columns:
                if required_col == 'ФИО':
                    result_df[required_col] = None
                else:
                    result_df[required_col] = ''

        if 'Корпоративная почта' in result_df.columns:
            result_df = result_df[result_df['Корпоративная почта'].astype(str).str.contains('@edu.hse.ru', na=False)]
            result_df['Корпоративная почта'] = pd.Series(result_df['Корпоративная почта']).astype(str).str.lower().str.strip()
        return result_df
    except Exception as e:
        st.error(f"Ошибка загрузки списка студентов: {e}")
        return pd.DataFrame()

def upload_course_data_to_supabase(supabase, course_data, course_name):
    """Загрузка данных одного курса в соответствующую таблицу"""
    try:
        table_mapping = {'ЦГ': 'course_cg', 'Питон': 'course_python', 'Андан': 'course_analysis'}
        table_name = table_mapping.get(course_name)
        if not table_name:
            st.error(f"❌ Неизвестный курс: {course_name}")
            return False
            
        st.info(f"📈 Загрузка курса {course_name} в {table_name}...")
        if course_data is None or course_data.empty:
            st.warning(f"⚠️ Нет данных для курса {course_name}")
            return True

        records_for_upsert = []
        processed_emails = set()
        
        for _, row in course_data.iterrows():
            email = str(row.get('Корпоративная почта', '')).strip().lower()
            if not email or '@edu.hse.ru' not in email:
                continue
            if email in processed_emails:
                continue
                
            processed_emails.add(email)
            
            percent_col = f'Процент_{course_name}'
            progress_value = None
            if percent_col in row and pd.notna(row[percent_col]) and row[percent_col] != '':
                try:
                    progress_value = float(row[percent_col])
                except (ValueError, TypeError):
                    progress_value = None
            
            records_for_upsert.append({
                'корпоративная_почта': email,
                'процент_завершения': progress_value
            })
        
        if not records_for_upsert:
            st.info(f"📋 Нет записей для курса {course_name}")
            return True

        batch_size = 200
        total_processed = 0
        for i in range(0, len(records_for_upsert), batch_size):
            batch = records_for_upsert[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            try:
                supabase.table(table_name).upsert(batch, on_conflict='корпоративная_почта').execute()
                total_processed += len(batch)
                st.success(f"✅ Курс {course_name} - батч {batch_num}: {len(batch)} записей")
            except Exception as e:
                st.error(f"❌ Ошибка загрузки курса {course_name}, батч {batch_num}: {e}")
                return False

        st.success(f"🎉 Курс {course_name}: {total_processed} записей загружено")
        return True
    except Exception as e:
        st.error(f"❌ Ошибка загрузки курса {course_name}: {e}")
        return False

def extract_course_data(uploaded_file, course_name):
    """Извлечение данных курса из файла"""
    try:
        file_name = uploaded_file.name.lower()
        if file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif file_name.endswith('.csv'):
            content = uploaded_file.getvalue()
            try:
                df = pd.read_csv(StringIO(content.decode('utf-16')), sep='\t')
            except (UnicodeDecodeError, pd.errors.ParserError):
                try:
                    df = pd.read_csv(StringIO(content.decode('utf-8')))
                except UnicodeDecodeError:
                    df = pd.read_csv(StringIO(content.decode('cp1251')))
        else:
            st.error(f"Неподдерживаемый формат файла для курса {course_name}")
            return None

        email_column = None
        possible_email_names = ['Адрес электронной почты', 'Корпоративная почта', 'Email', 'Почта', 'E-mail']
        for col_name in possible_email_names:
            if col_name in df.columns:
                email_column = col_name
                break
        if email_column is None:
            st.error(f"Столбец с email не найден в файле {course_name}")
            return None

        cg_excluded_keywords = [
            'take away', 'шпаргалка', 'консультация', 'общая информация', 'промо-ролик',
            'поддержка студентов', 'пояснение', 'случайный вариант для студентов с овз',
            'материалы по модулю', 'копия', 'демонстрационный вариант', 'спецификация',
            'демо-версия', 'правила проведения независимого экзамена',
            'порядок организации и проведения независимых экзаменов',
            'интерактивный тренажер правил нэ', 'пересдачи в сентябре', 'незрячих и слабовидящих',
            'проекты с использование tei', 'тренировочный тест', 'ключевые принципы tei',
            'базовые возможности tie', 'специальные модули tei', 'будут идентичными',
            'опрос', 'тест по модулю', 'анкета', 'user information', 'страна', 'user_id', 'данные о пользователе'
        ]

        excluded_count = 0
        completed_columns = []
        timestamp_columns = []

        for col in df.columns:
            if col not in ['Unnamed: 0', email_column, 'Данные о пользователе', 'User information', 'Страна']:
                if course_name == 'ЦГ':
                    should_exclude = False
                    col_str = str(col).strip().lower()
                    for excluded_keyword in cg_excluded_keywords:
                        if excluded_keyword.lower() in col_str:
                            should_exclude = True
                            excluded_count += 1
                            break
                    if should_exclude:
                        continue

                if not col.startswith('Unnamed:') and len(str(col).strip()) > 0:
                    sample_values = df[col].dropna().astype(str).head(100)
                    if any('Выполнено' in str(val) or 'выполнено' in str(val).lower() for val in sample_values):
                        if not all(str(val) == 'Не выполнено' for val in sample_values if pd.notna(val)):
                            completed_columns.append(col)
                elif col.startswith('Unnamed:') and col != 'Unnamed: 0':
                    sample_values = df[col].dropna().astype(str).head(20)
                    for val in sample_values:
                        val_str = str(val).strip()
                        if any(pattern in val_str for pattern in ['2020', '2021', '2022', '2023', '2024']) and ':' in val_str:
                            timestamp_columns.append(col)
                            break

        if timestamp_columns:
            completion_data = []
            for idx, row in df.iterrows():
                email_val = row[email_column]
                if pd.isna(email_val) or '@edu.hse.ru' not in str(email_val).lower():
                    continue
                total_tasks = len(timestamp_columns)
                completed_tasks = 0
                for col in timestamp_columns:
                    cell_val = row[col]
                    val_str = str(cell_val).strip() if not pd.isna(cell_val) else ''
                    if val_str and val_str != 'nan' and val_str != '':
                        if any(pattern in val_str for pattern in ['2020', '2021', '2022', '2023', '2024']) and ':' in val_str:
                            completed_tasks += 1
                percentage = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                completion_data.append({'email': str(email_val).lower().strip(), 'percentage': percentage})
            if completion_data:
                result_df = pd.DataFrame(completion_data)
                result_df.columns = ['Корпоративная почта', f'Процент_{course_name}']
                st.success(f"✅ Рассчитан процент завершения для {len(result_df)} студентов курса {course_name}")
                return result_df

        elif completed_columns:
            completion_data = []
            for idx, row in df.iterrows():
                email_val = row[email_column]
                if pd.isna(email_val) or '@edu.hse.ru' not in str(email_val).lower():
                    continue
                total_tasks = 0
                completed_tasks = 0
                for col in completed_columns:
                    cell_val = row[col]
                    val = str(cell_val).strip() if not pd.isna(cell_val) else ''
                    if val and val != 'nan':
                        total_tasks += 1
                        if 'Выполнено' in val or 'выполнено' in val.lower():
                            completed_tasks += 1
                percentage = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                completion_data.append({'email': str(email_val).lower().strip(), 'percentage': percentage})
            if completion_data:
                result_df = pd.DataFrame(completion_data)
                result_df.columns = ['Корпоративная почта', f'Процент_{course_name}']
                st.success(f"✅ Рассчитан процент завершения для {len(result_df)} студентов курса {course_name}")
                return result_df

        st.warning(f"Не найдено данных о завершении для курса {course_name}")
        return None
    except Exception as e:
        st.error(f"Ошибка обработки данных курса {course_name}: {e}")
        return None

# =============================================================================
# ОСНОВНОЕ ПРИЛОЖЕНИЕ
# =============================================================================

def main():
    """Главная функция приложения"""
    
    # =============================================================================
    # APPLE-СОВМЕСТИМЫЙ UI/UX LAYER (БЕЗ ИЗМЕНЕНИЯ ЛОГИКИ)
    # =============================================================================
    st.markdown("""
    <style>
        /* Apple-style typography */
        h1, h2, h3, h4 {
            font-weight: 600;
            letter-spacing: -0.02em;
            line-height: 1.2;
            color: #e0e0e6;
        }
        h1 {
            font-size: 2.25rem;
            margin-bottom: 0.5rem;
        }
        h2 {
            font-size: 1.5rem;
            margin-top: 1.5rem;
            margin-bottom: 1rem;
        }
        p, li, .stMarkdown {
            color: #c6c6cc;
            line-height: 1.6;
        }
        /* Apple-style buttons */
        .stButton > button {
            border-radius: 12px;
            padding: 8px 20px;
            font-weight: 500;
            border: none;
            background-color: #4a86e8;
            color: white;
            transition: background-color 0.2s ease;
        }
        .stButton > button:hover {
            background-color: #5a96f8;
        }
        /* Apple-style info boxes */
        .stAlert {
            border-radius: 12px;
            padding: 14px 18px;
            margin: 16px 0;
        }
        /* Sidebar refinement */
        [data-testid="stSidebar"] {
            background-color: #2a2a30 !important;
        }
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] label {
            color: #e0e0e6 !important;
        }
        /* Consistent spacing */
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        hr {
            margin: 1.5rem 0;
            border-color: #3a3a42;
        }
        /* Footer */
        footer {
            visibility: hidden;
        }
        .custom-footer {
            text-align: center;
            color: #888892;
            font-size: 0.85rem;
            margin-top: 2rem;
            padding-top: 1.5rem;
            border-top: 1px solid #3a3a42;
        }
    </style>
    """, unsafe_allow_html=True)

    # Заголовок (Apple-style: без эмодзи, с caption)
    st.title("DataCulture Platform")
    st.caption("Объединённая платформа инструментов Data Culture @ HSE University")
    st.caption("Для самых лучших сотрудников проекта от Тимошки")
    st.markdown("---")

    # Боковая панель навигации
    with st.sidebar:
        st.image(LOGO_URL, width=160)  # уменьшено до 160px для лучшей пропорции
        st.markdown("<br>", unsafe_allow_html=True)
        tool = st.navigation(
            "Инструмент",
            [
                "Перезачет оценок",
                "Генератор HTML-карточек",
                "Генератор сертификатов",
                "Обработка пересдач внешней оценки",
                "Аналитика курсов",
                "Обновление списка студентов"
            ],
            index=0
        )
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("### ℹ️ О платформе")
        st.info("""
        **DataCulture Platform** объединяет шесть ключевых инструментов:
        1. **Перезачет оценок** — автоматический расчет итоговых оценок  
        2. **Генератор карточек** — создание HTML-рассылок в фирменном стиле ВШЭ  
        3. **Сертификаты** — обработка данных для выдачи сертификатов  
        4. **Пересдачи** — обработка пересдач внешней оценки  
        5. **Аналитика курсов** — обработка и загрузка аналитики курсов в Supabase  
        6. **Обновление студентов** — загрузка и обновление списка студентов в Supabase  
        """)

    # =============================================================================
    # МОДУЛИ (без изменений в логике — только оформление уже учтено выше)
    # =============================================================================
    if tool == "Перезачет оценок":
        st.header("📊 Сервис перезачета оценок")
        st.markdown("""
        Загрузите Excel или CSV файл с данными студентов для автоматического расчета итоговых оценок.
        **Требуемые колонки:**
        - Наименование НЭ
        - Оценка НЭ
        - Оценка дисциплины-пререквизита
        - Внешнее измерение цифровых компетенций (Входной, Промежуточный, Итоговый контроль)
        """)
        uploaded_file = st.file_uploader(
            "Выберите файл для обработки",
            type=['xlsx', 'csv'],
            key="grade_file"
        )
        if uploaded_file is not None:
            file_name = uploaded_file.name
            processing_mode = st.radio(
                "Режим обработки:",
                ("Перезачет БЕЗ динамики", "Перезачет С динамикой"),
                help="""
                - **БЕЗ динамики**: Стандартный перезачет по максимальной оценке.
                - **С динамикой**: Если оценка падает более чем на 1 балл между этапами, перезачет блокируется.
                """
            )
            if st.button("🚀 Обработать файл", type="primary"):
                with st.spinner("Обработка данных…"):
                    try:
                        if file_name.endswith('.xlsx'):
                            df_initial = pd.read_excel(uploaded_file, engine='openpyxl')
                        else:
                            df_initial = pd.read_csv(uploaded_file)
                        use_dynamics_flag = (processing_mode == "Перезачет С динамикой")
                        result_df = process_grade_recalculation(df_initial, use_dynamics=use_dynamics_flag)
                        st.success("✅ Обработка успешно завершена!")
                        st.subheader("📊 Предварительный просмотр")
                        st.dataframe(result_df.head(10), use_container_width=True)
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            result_df.to_excel(writer, index=False, sheet_name='Результат')
                        excel_data = output.getvalue()
                        current_date = datetime.now().strftime('%d-%m-%y')
                        download_filename = f"Результат_{file_name.split('.')[0]}_{current_date}.xlsx"
                        st.download_button(
                            label="📥 Скачать результат",
                            data=excel_data,
                            file_name=download_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except KeyError as e:
                        st.error(f"❌ Ошибка в структуре файла: {e}")
                    except Exception as e:
                        st.error(f"❌ Произошла ошибка: {e}")

    elif tool == "Генератор HTML-карточек":
        st.header("🎓 Генератор карточек НИУ ВШЭ")
        st.markdown("""
        Создайте HTML-карточку рассылки в фирменном стиле ВШЭ с помощью искусственного интеллекта.
        **Как использовать:**
        1. Введите текст объявления или новости
        2. Нажмите кнопку генерации
        3. Получите готовый HTML-код и предпросмотр
        """)
        try:
            has_api_key = "NEBIUS_API_KEY" in st.secrets
        except FileNotFoundError:
            has_api_key = False
        if not has_api_key:
            st.error("❌ NEBIUS_API_KEY не настроен. Обратитесь к администратору.")
            st.info("💡 Создайте файл `.streamlit/secrets.toml` с вашим API ключом")
            st.stop()
        user_text = st.text_area(
            "Введите текст объявления:",
            height=250,
            placeholder="Вставьте сюда текст письма или новости..."
        )
        if st.button("✨ Сформировать HTML", type="primary"):
            if not user_text.strip():
                st.warning("⚠️ Введите текст для генерации")
            else:
                with st.spinner("Генерация карточки…"):
                    try:
                        client = get_nebius_client()
                        html_code = generate_hse_html(client, user_text)
                        st.success("✅ Карточка успешно создана!")
                        col1, col2 = st.columns([1, 1])
                        with col1:
                            st.subheader("📄 HTML-код")
                            st.code(html_code, language="html")
                            st.download_button(
                                label="💾 Скачать HTML",
                                data=html_code.encode("utf-8"),
                                file_name="hse_card.html",
                                mime="text/html"
                            )
                        with col2:
                            st.subheader("🌐 Предпросмотр")
                            import streamlit.components.v1 as components
                            components.html(html_code, height=800, scrolling=True)
                    except Exception as e:
                        st.error(f"❌ Ошибка: {e}")

    elif tool == "Генератор сертификатов":
        st.header("📜 Система обработки сертификатов")
        st.markdown("""
        Автоматическая обработка данных экзаменов студентов и генерация текста для сертификатов.
        **Требуется два файла:**
        1. Excel с данными студентов (колонки: Учащийся, Дисциплина 1/2/3, Оценка 5 баллов)
        2. Excel со справочником навыков (колонки: Дисциплина, Уровень_оценки, Описание_навыков)
        """)
        with st.sidebar:
            st.markdown("---")
            st.markdown("### 📥 Скачать примеры")
            current_dir = os.path.dirname(os.path.abspath(__file__))
            excel_example_path = os.path.join(current_dir, 'Сертификаты пример.xlsx')
            if os.path.exists(excel_example_path):
                with open(excel_example_path, 'rb') as example_file:
                    excel_example_data = example_file.read()
                st.download_button(
                    label="📊 Пример данных студентов",
                    data=excel_example_data,
                    file_name="Сертификаты_пример.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            skills_example_path = os.path.join(current_dir, 'агрегированные_навыки.xlsx')
            if os.path.exists(skills_example_path):
                with open(skills_example_path, 'rb') as skills_file:
                    skills_data = skills_file.read()
                st.download_button(
                    label="📄 Справочник навыков",
                    data=skills_data,
                    file_name="агрегированные_навыки.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            st.markdown("---")
            st.markdown("### 📋 Требования к файлам")
            st.markdown("""
            **📊 Данные студентов:**
            - `Учащийся`
            - `Название Дисциплины 1/2/3`
            - `Дисциплина 1/2/3`
            - `Оценка 5 баллов Дисциплина 1/2/3`
            **📄 Справочник навыков:**
            - `Дисциплина`
            - `Уровень_оценки`
            - `Описание_навыков`
            💡 **Используйте примеры выше!**
            """)
        col1, col2 = st.columns([1, 1])
        with col1:
            st.subheader("📊 Данные студентов")
            excel_file = st.file_uploader(
                "Выберите Excel файл",
                type=['xlsx', 'xls'],
                key="students_file"
            )
        with col2:
            st.subheader("📄 Справочник навыков")
            skills_file = st.file_uploader(
                "Выберите Excel файл",
                type=['xlsx', 'xls'],
                key="skills_file"
            )
        if excel_file and skills_file:
            try:
                with st.spinner("📥 Загрузка файлов…"):
                    df = pd.read_excel(excel_file)
                    skills_content = skills_file.read()
                    grade_mapping = load_reference_data(skills_content)
                st.success("✅ Файлы успешно загружены!")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Студентов", len(df))
                with col2:
                    st.metric("Колонок", len(df.columns))
                with col3:
                    st.metric("Навыков в справочнике", len(grade_mapping))
                with st.expander("👀 Предпросмотр данных"):
                    st.dataframe(df.head(), use_container_width=True)
                if st.button("🚀 Обработать данные", type="primary"):
                    with st.spinner("⚙️ Обработка…"):
                        result_df, processing_log = process_student_data(df, grade_mapping)
                    st.success("✅ Обработка завершена!")
                    st.subheader("📊 Результаты")
                    st.dataframe(result_df, use_container_width=True)
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl', mode='w') as writer:
                        result_df.to_excel(writer, index=False)
                    output.seek(0)
                    st.download_button(
                        label="📥 Скачать результаты",
                        data=output.getvalue(),
                        file_name="Сертификаты_с_результатами.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            except Exception as e:
                st.error(f"❌ Ошибка: {str(e)}")
        elif excel_file:
            st.info("📄 Загрузите также файл со справочником навыков")
        elif skills_file:
            st.info("📊 Загрузите также файл с данными студентов")
        else:
            st.info("📁 Загрузите оба файла для начала работы")

    elif tool == "Обработка пересдач внешней оценки":
        st.header("📝 Обработка пересдач внешней оценки")
        st.markdown("""
        Автоматическая обработка пересдач из внешней системы оценивания с интеграцией Supabase.
        **Требуется:**
        1. **Файл с оценками** — таблица из внешней системы с тестированиями
        2. **Список студентов** — загружается автоматически из Supabase (таблица `students`)
        """)
        try:
            supabase = get_supabase_client()
            st.success("✅ Подключение к Supabase установлено")
        except Exception as e:
            st.error(f"❌ Ошибка подключения к Supabase: {str(e)}")
            st.stop()
        st.markdown("---")
        st.subheader("📊 Загрузка файла с оценками")
        grades_file = st.file_uploader(
            "Выберите файл с оценками (external_assessment)",
            type=['xlsx', 'xls'],
            key="external_grades_file"
        )
        if grades_file:
            try:
                with st.spinner("📥 Загрузка файла с оценками…"):
                    grades_df = pd.read_excel(grades_file)
                st.success("✅ Файл с оценками успешно загружен!")
                with st.spinner("📥 Загрузка списка студентов из Supabase…"):
                    students_df = load_students_from_supabase()
                if students_df.empty:
                    st.error("❌ Список студентов пуст. Загрузите данные в таблицу `students` в Supabase.")
                    st.stop()
                else:
                    st.success(f"✅ Загружено {len(students_df)} студентов из Supabase")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Записей с оценками", len(grades_df))
                with col2:
                    st.metric("Студентов в базе", len(students_df))
                with col3:
                    st.metric("Колонок в оценках", len(grades_df.columns))
                col_preview1, col_preview2 = st.columns(2)
                with col_preview1:
                    with st.expander("👀 Предпросмотр файла с оценками"):
                        st.dataframe(grades_df.head(), use_container_width=True)
                with col_preview2:
                    with st.expander("👀 Предпросмотр списка студентов"):
                        st.dataframe(students_df.head(10), use_container_width=True)
                if st.button("🚀 Обработать данные", type="primary", key="process_btn"):
                    with st.spinner("⚙️ Обработка пересдач…"):
                        try:
                            result_df = process_external_assessment(grades_df, students_df)
                            if result_df.empty:
                                st.error("❌ Не удалось обработать данные. Проверьте структуру файла.")
                            else:
                                st.success("✅ Обработка успешно завершена!")
                                with st.spinner("💾 Сохранение в Supabase…"):
                                    try:
                                        new_count, total_count = save_to_supabase(result_df)
                                        st.success(f"✅ Сохранено в Supabase: {new_count} новых записей из {total_count}")
                                    except Exception as e:
                                        st.error(f"❌ Ошибка при сохранении: {str(e)}")
                                st.subheader("📊 Результаты обработки")
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Всего обработано записей", total_count)
                                with col2:
                                    st.metric("Новых записей", new_count)
                                with col3:
                                    st.metric("Уже существовало", total_count - new_count)
                                new_records_df = get_new_records(result_df)
                                tab1, tab2 = st.tabs(["📋 Все обработанные данные", "🆕 Только новые записи"])
                                with tab1:
                                    st.dataframe(result_df, use_container_width=True)
                                    output_all = io.BytesIO()
                                    with pd.ExcelWriter(output_all, engine='openpyxl') as writer:
                                        result_df.to_excel(writer, index=False, sheet_name='Все пересдачи')
                                    output_all.seek(0)
                                    current_date = datetime.now().strftime('%d-%m-%Y')
                                    st.download_button(
                                        label="📥 Скачать все записи (XLSX)",
                                        data=output_all.getvalue(),
                                        file_name=f"Пересдачи_все_{current_date}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key="download_all"
                                    )
                                with tab2:
                                    if new_records_df.empty:
                                        st.info("ℹ️ Новых записей нет. Все данные уже были в базе.")
                                    else:
                                        st.dataframe(new_records_df, use_container_width=True)
                                        output_new = io.BytesIO()
                                        with pd.ExcelWriter(output_new, engine='openpyxl') as writer:
                                            new_records_df.to_excel(writer, index=False, sheet_name='Новые пересдачи')
                                        output_new.seek(0)
                                        st.download_button(
                                            label="📥 Скачать только новые записи (XLSX)",
                                            data=output_new.getvalue(),
                                            file_name=f"Пересдачи_новые_{current_date}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key="download_new"
                                        )
                                with st.expander("📈 Статистика по обработке"):
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        if 'Наименование дисциплины' in result_df.columns:
                                            st.write("**Распределение по дисциплинам:**")
                                            discipline_counts = result_df['Наименование дисциплины'].value_counts()
                                            st.dataframe(discipline_counts)
                                    with col2:
                                        if 'ФИО' in result_df.columns:
                                            st.write("**Уникальные студенты:**")
                                            st.metric("Уникальных студентов", result_df['ФИО'].nunique())
                        except Exception as e:
                            st.error(f"❌ Ошибка при обработке: {str(e)}")
            except Exception as e:
                st.error(f"❌ Ошибка при загрузке файла: {str(e)}")
        else:
            st.info("📁 Загрузите файл с оценками для начала работы")
            with st.expander("📊 Текущее состояние базы данных"):
                existing_peresdachi = load_existing_peresdachi()
                if existing_peresdachi.empty:
                    st.info("ℹ️ Таблица peresdachi пуста или не создана")
                else:
                    st.metric("Записей в таблице peresdachi", len(existing_peresdachi))
                    st.dataframe(existing_peresdachi.head(10), use_container_width=True)

    elif tool == "Аналитика курсов":
        st.header("📊 Аналитика курсов")
        st.markdown("""
        Автоматическая обработка и загрузка аналитики курсов в Supabase.
        """)
        try:
            supabase = get_supabase_client()
            st.success("✅ Подключение к Supabase установлено")
        except Exception as e:
            st.error(f"❌ Ошибка подключения к Supabase: {str(e)}")
            st.stop()
        st.markdown("---")
        st.subheader("📁 Загрузка файлов курсов")
        col1, col2, col3 = st.columns(3)
        with col1:
            course_cg_file = st.file_uploader("📊 Курс ЦГ", type=['csv', 'xlsx', 'xls'], key="cg_file")
        with col2:
            course_python_file = st.file_uploader("🐍 Курс Python", type=['csv', 'xlsx', 'xls'], key="python_file")
        with col3:
            course_analysis_file = st.file_uploader("📈 Курс Анализ данных", type=['csv', 'xlsx', 'xls'], key="analysis_file")
        files_uploaded = all([course_cg_file, course_python_file, course_analysis_file])
        if not files_uploaded:
            st.info("📝 Пожалуйста, загрузите все три файла курсов:")
            file_status = {
                "Курс ЦГ": "✅" if course_cg_file else "❌",
                "Курс Python": "✅" if course_python_file else "❌",
                "Курс Анализ данных": "✅" if course_analysis_file else "❌"
            }
            st.table(pd.DataFrame([{"Файл": k, "Статус": v} for k, v in file_status.items()]))
        else:
            st.success("✅ Все файлы загружены! Готово к обработке.")
            if st.button("🚀 Обработать курсы", type="primary", key="process_courses_btn"):
                with st.spinner("🔄 Обработка данных…"):
                    try:
                        st.info("📊 Обработка файлов курсов…")
                        course_names = ['ЦГ', 'Питон', 'Андан']
                        course_files = [course_cg_file, course_python_file, course_analysis_file]
                        course_data_list = []
                        for course_file, course_name in zip(course_files, course_names):
                            course_data = extract_course_data(course_file, course_name)
                            if course_data is None:
                                st.error(f"❌ Ошибка обработки курса {course_name}")
                                st.stop()
                            course_data_list.append(course_data)
                            st.success(f"✅ Обработан курс {course_name}: {len(course_data)} записей")
                        st.info("💾 Обновление данных курсов в Supabase…")
                        success_count = 0
                        for course_data, course_name in zip(course_data_list, course_names):
                            if upload_course_data_to_supabase(supabase, course_data, course_name):
                                success_count += 1
                        if success_count == len(course_names):
                            st.success(f"🎉 Все {success_count} курса успешно загружены!")
                            st.subheader("📋 Сводная статистика")
                            summary_data = []
                            for course_data, course_name in zip(course_data_list, course_names):
                                col_name = f'Процент_{course_name}'
                                if col_name in course_data.columns:
                                    course_stats = course_data[col_name].dropna()
                                    if len(course_stats) > 0:
                                        avg_completion = course_stats.mean()
                                        students_100 = len(course_stats[course_stats == 100.0])
                                        students_0 = len(course_stats[course_stats == 0.0])
                                        total_students = len(course_stats)
                                        summary_data.append({
                                            'Курс': course_name,
                                            'Студентов всего': total_students,
                                            'Средний %': f"{avg_completion:.1f}%",
                                            '100%': students_100,
                                            '0%': students_0
                                        })
                            if summary_data:
                                st.table(pd.DataFrame(summary_data))
                            st.balloons()
                        else:
                            st.error(f"❌ Загружено только {success_count} из {len(course_names)} курсов")
                    except Exception as e:
                        st.error(f"❌ Ошибка при обработке: {str(e)}")

    else:  # Обновление списка студентов
        st.header("👥 Обновление списка студентов")
        st.markdown("""
        Загрузка и обновление списка студентов в базе данных Supabase.
        """)
        try:
            supabase = get_supabase_client()
            st.success("✅ Подключение к Supabase установлено")
        except Exception as e:
            st.error(f"❌ Ошибка подключения к Supabase: {str(e)}")
            st.stop()
        st.markdown("---")
        st.subheader("📁 Загрузка файла со студентами")
        students_file = st.file_uploader(
            "Выберите файл со списком студентов (Excel или CSV)",
            type=['xlsx', 'xls', 'csv'],
            key="students_upload_file"
        )
        if students_file:
            try:
                with st.spinner("📄 Загрузка файла…"):
                    students_df = load_student_list_file(students_file)
                if students_df.empty:
                    st.error("❌ Не удалось загрузить данные из файла. Проверьте формат файла.")
                    st.stop()
                st.success(f"✅ Файл успешно загружен!")
                st.subheader("📊 Предварительная информация")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Записей в файле", len(students_df))
                with col2:
                    st.metric("Уникальных email", students_df['Корпоративная почта'].nunique())
                with st.expander("👀 Предпросмотр данных"):
                    st.dataframe(students_df.head(20), use_container_width=True)
                if st.button("🚀 Обновить список студентов в Supabase", type="primary", key="update_students_btn"):
                    with st.spinner("🔄 Обновление базы данных…"):
                        try:
                            if upload_students_to_supabase(supabase, students_df):
                                st.success("✅ Список студентов обновлён!")
                                st.balloons()
                            else:
                                st.error("❌ Не удалось обновить список студентов")
                        except Exception as e:
                            st.error(f"❌ Ошибка при обновлении: {str(e)}")
            except Exception as e:
                st.error(f"❌ Ошибка при загрузке файла: {str(e)}")
        else:
            st.info("📁 Загрузите файл со списком студентов")
            with st.expander("📊 Текущее состояние базы данных"):
                try:
                    current_students = load_students_from_supabase()
                    if current_students.empty:
                        st.info("ℹ️ Таблица students пуста или не создана")
                    else:
                        st.success(f"✅ В базе данных: {len(current_students)} студентов")
                        st.dataframe(current_students.head(10), use_container_width=True)
                except Exception as e:
                    st.warning(f"⚠️ Не удалось загрузить данные: {str(e)}")

    # Футер (Apple-style)
    st.markdown("""
    <div class="custom-footer">
        DataCulture Platform v1.0 · Created by Тимошка
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
