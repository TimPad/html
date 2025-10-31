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
from typing import Dict, Tuple, List
from supabase import create_client, Client

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
    "Верните ТОЛЬКО корректный JSON в формате: {\"type\": \"HTML\", \"content\": \"<div>...</div>\"}."
    "Пример корректного вывода:"
    + json.dumps({"type": "HTML", "content": HTML_EXAMPLE}, ensure_ascii=False, indent=2)
)

# =============================================================================
# ФУНКЦИИ ДЛЯ МОДУЛЯ 1: ПЕРЕЗАЧЕТ ОЦЕНОК
# =============================================================================
# ... (остаётся без изменений)
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

def get_unique_courses_from_supabase() -> List[str]:
    """Получить уникальные значения из колонки 'курс' в таблице students"""
    try:
        supabase = get_supabase_client()
        response = supabase.table('students').select('курс').execute()
        if response.data:
            # Извлекаем значения, фильтруем None/пустые и делаем уникальными
            courses = {row['курс'] for row in response.data if row.get('курс')}
            return sorted(courses)
        else:
            return []
    except Exception as e:
        st.warning(f"⚠️ Не удалось загрузить список курсов: {str(e)}")
        return []

def load_students_from_supabase(selected_courses: List[str] = None) -> pd.DataFrame:
    """
    Загрузка списка студентов из Supabase с фильтрацией по выбранным курсам.
    Если selected_courses пустой или None — загружаются все студенты.
    """
    try:
        supabase = get_supabase_client()
        all_data = []
        page_size = 1000
        offset = 0

        while True:
            query = supabase.table('students').select('*')
            if selected_courses:
                query = query.in_('курс', selected_courses)
            response = query.range(offset, offset + page_size - 1).execute()

            if response.data:
                all_data.extend(response.data)
                if len(response.data) < page_size:
                    break
                offset += page_size
            else:
                break

        if all_data:
            df = pd.DataFrame(all_data)
            column_mapping = {
                'корпоративная_почта': 'Адрес электронной почты',
                'фио': 'ФИО',
                'филиал_кампус': 'Филиал (кампус)',
                'факультет': 'Факультет',
                'образовательная_программа': 'Образовательная программа',
                'группа': 'Группа',
                'курс': 'Курс'
            }
            existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=existing_columns)
            return df
        else:
            st.warning("⚠️ Таблица students пуста или не содержит записей по выбранным курсам")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Ошибка при загрузке студентов из Supabase: {str(e)}")
        return pd.DataFrame()

def create_peresdachi_table_if_not_exists():
    """Примечание: Таблица должна быть создана вручную в Supabase Dashboard"""
    pass

def load_existing_peresdachi() -> pd.DataFrame:
    """Загрузка существующих записей из таблицы peresdachi (все записи с пагинацией)"""
    try:
        supabase = get_supabase_client()
        all_data = []
        page_size = 1000
        offset = 0
        while True:
            response = supabase.table('peresdachi').select('*').range(offset, offset + page_size - 1).execute()
            if response.data:
                all_data.extend(response.data)
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
        st.warning(f"⚠️ Таблица peresdachi не найдена или пуста: {str(e)}")
        return pd.DataFrame()

def save_to_supabase(df: pd.DataFrame) -> Tuple[int, int]:
    """Сохранение данных в таблицу peresdachi в Supabase"""
    try:
        supabase = get_supabase_client()
        existing_df = load_existing_peresdachi()
        if existing_df.empty:
            new_records = df.to_dict('records')
            new_count = len(new_records)
        else:
            merge_cols = ['Адрес электронной почты', 'Наименование дисциплины']
            if all(col in existing_df.columns for col in merge_cols) and all(col in df.columns for col in merge_cols):
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
                new_records = df.to_dict('records')
                new_count = len(new_records)
        if new_records:
            cleaned_records = []
            for record in new_records:
                cleaned_record = {k: (v if pd.notna(v) else None) for k, v in record.items()}
                cleaned_records.append(cleaned_record)
            supabase.table('peresdachi').insert(cleaned_records).execute()
        return new_count, len(df)
    except Exception as e:
        st.error(f"❌ Ошибка при сохранении в Supabase: {str(e)}")
        raise e

def get_new_records(all_df: pd.DataFrame) -> pd.DataFrame:
    """Получить только новые записи, которых еще нет в базе"""
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
    """Обработка пересдач внешней оценки"""
    for col in grades_df.columns:
        if grades_df[col].dtype == 'object':
            grades_df[col] = grades_df[col].astype(str).str.replace('-', '', regex=False).str.strip()
    column_mapping = {
        'Тест:Входное тестирование (Значение)': 'Внешнее измерение цифровых компетенций. Входной контроль',
        'Тест:Промежуточное тестирование (Значение)': 'Внешнее измерение цифровых компетенций. Промежуточный контроль',
        'Тест:Итоговое тестирование (Значение)': 'Внешнее измерение цифровых компетенций. Итоговый контроль'
    }
    grades_df = grades_df.rename(columns=column_mapping)
    value_columns = [
        'Внешнее измерение цифровых компетенций. Входной контроль',
        'Внешнее измерение цифровых компетенций. Промежуточный контроль',
        'Внешнее измерение цифровых компетенций. Итоговый контроль'
    ]
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
    students_cols = ['ФИО', 'Адрес электронной почты', 'Филиал (кампус)', 
                     'Факультет', 'Образовательная программа', 'Группа', 'Курс']
    missing_cols = [col for col in students_cols if col not in students_df.columns]
    if missing_cols:
        st.warning(f"Отсутствуют колонки в файле студентов: {missing_cols}")
        available_cols = [col for col in students_cols if col in students_df.columns]
    else:
        available_cols = students_cols
    students_subset = students_df[available_cols].copy()
    melted_df['Адрес электронной почты'] = melted_df['Адрес электронной почты'].astype(str).str.strip().str.lower()
    students_subset['Адрес электронной почты'] = students_subset['Адрес электронной почты'].astype(str).str.strip().str.lower()
    result_df = melted_df.merge(
        students_subset,
        on='Адрес электронной почты',
        how='left'
    )
    result_df['ID дисциплины'] = ''
    result_df['Период аттестации'] = ''
    if 'Филиал (кампус)' in result_df.columns:
        result_df = result_df.rename(columns={'Филиал (кампус)': 'Кампус'})
    output_columns = [
        'ФИО', 'Адрес электронной почты', 'Кампус', 'Факультет',
        'Образовательная программа', 'Группа', 'Курс', 'ID дисциплины',
        'Наименование дисциплины', 'Период аттестации', 'Оценка'
    ]
    final_columns = [col for col in output_columns if col in result_df.columns]
    result_df = result_df[final_columns]
    result_df = result_df[result_df['Оценка'].notna()]
    result_df = result_df[result_df['Оценка'].astype(str).str.strip() != '']
    result_df = result_df[result_df['Оценка'].astype(str).str.strip() != 'nan']
    return result_df

# =============================================================================
# ОСНОВНОЕ ПРИЛОЖЕНИЕ
# =============================================================================
def main():
    """Главная функция приложения"""
    st.title("❤️🌸 DataCulture Platform 🌸❤️")
    st.markdown("**Объединённая платформа инструментов Data Culture @ HSE University**")
    st.markdown("*Для самых лучших сотрудников проекта от Тимошки!*")
    st.markdown("---")
    with st.sidebar:
        st.image(LOGO_URL, width=200)
        st.markdown("---")
        tool = st.radio(
            "🎯 Выберите инструмент:",
            [
                "📊 Перезачет оценок",
                "🎓 Генератор HTML-карточек",
                "📜 Обработка сертификатов",
                "📝 Обработка пересдач внешней оценки"
            ],
            index=0
        )
        st.markdown("---")
        st.markdown("### ℹ️ О платформе")
        st.info("""
        **DataCulture Platform** объединяет четыре ключевых инструмента:
        1. **Перезачет оценок** - автоматический расчет итоговых оценок
        2. **Генератор карточек** - создание HTML-рассылок в фирменном стиле ВШЭ
        3. **Сертификаты** - обработка данных для выдачи сертификатов
        4. **Пересдачи** - обработка пересдач внешней оценки
        """)

    # =============================================================================
    # МОДУЛЬ 1–3: без изменений
    # =============================================================================
    if tool == "📊 Перезачет оценок":
        # ... (остаётся без изменений)
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
                with st.spinner("Обработка данных..."):
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

    elif tool == "🎓 Генератор HTML-карточек":
        # ... (остаётся без изменений)
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
                with st.spinner("Генерация карточки..."):
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

    elif tool == "📜 Обработка сертификатов":
        # ... (остаётся без изменений)
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
                    help="Скачайте этот файл как шаблон для ваших данных студентов",
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
                    help="Скачайте справочник с описаниями навыков",
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
                with st.spinner("📥 Загрузка файлов..."):
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
                    with st.spinner("⚙️ Обработка..."):
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

    # =============================================================================
    # МОДУЛЬ 4: ОБРАБОТКА ПЕРЕСДАЧ ВНЕШНЕЙ ОЦЕНКИ — ОБНОВЛЁННЫЙ
    # =============================================================================
    else:  # Обработка пересдач внешней оценки
        st.header("📝 Обработка пересдач внешней оценки")
        st.markdown("""
        Автоматическая обработка пересдач из внешней системы оценивания с интеграцией Supabase.
        **Требуется:**
        1. **Файл с оценками** - таблица из внешней системы с тестированиями
        2. **Список студентов** - загружается автоматически из Supabase (таблица `students`)
        **Что делает инструмент:**
        - Очищает данные от лишних символов и пробелов
        - Переименовывает колонки в соответствии со стандартами
        - Преобразует данные из широкого в длинный формат (melt)
        - Объединяет данные с информацией о студентах из Supabase
        - Сохраняет результаты в таблицу `peresdachi` в Supabase
        - Позволяет скачать все данные или только новые записи
        """)

        try:
            supabase = get_supabase_client()
            st.success("✅ Подключение к Supabase установлено")
        except Exception as e:
            st.error(f"❌ Ошибка подключения к Supabase: {str(e)}")
            st.stop()

        st.markdown("---")

        # Загрузка списка курсов
        with st.spinner("📥 Получение списка курсов из Supabase..."):
            all_courses = get_unique_courses_from_supabase()

        if not all_courses:
            st.error("❌ Не удалось получить список курсов. Проверьте таблицу students в Supabase.")
            st.stop()

        # Выбор курсов
        st.subheader("🎓 Выберите курсы для обработки")
        selected_courses = st.multiselect(
            "Курс",
            options=all_courses,
            default=all_courses,
            placeholder="Выберите один или несколько курсов"
        )

        if not selected_courses:
            st.info("ℹ️ Пожалуйста, выберите хотя бы один курс для продолжения.")
            st.stop()

        # Загрузка файла с оценками
        st.subheader("📊 Загрузка файла с оценками")
        grades_file = st.file_uploader(
            "Выберите файл с оценками (external_assessment)",
            type=['xlsx', 'xls'],
            key="external_grades_file",
            help="Файл должен содержать колонки: Адрес электронной почты, Тест:Входное/Промежуточное/Итоговое тестирование (Значение)"
        )

        if grades_file:
            try:
                with st.spinner("📥 Загрузка файла с оценками..."):
                    grades_df = pd.read_excel(grades_file)
                st.success("✅ Файл с оценками успешно загружен!")

                # Загрузка студентов по выбранным курсам
                with st.spinner(f"📥 Загрузка студентов по {len(selected_courses)} курсам из Supabase..."):
                    students_df = load_students_from_supabase(selected_courses=selected_courses)

                if students_df.empty:
                    st.error("❌ Список студентов пуст. Проверьте данные в таблице `students` в Supabase.")
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
                    with st.spinner("⚙️ Обработка пересдач..."):
                        try:
                            result_df = process_external_assessment(grades_df, students_df)
                            if result_df.empty:
                                st.error("❌ Не удалось обработать данные. Проверьте структуру файла.")
                            else:
                                st.success("✅ Обработка успешно завершена!")

                                with st.spinner("💾 Сохранение в Supabase..."):
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
                                    existing_count = total_count - new_count
                                    st.metric("Уже существовало", existing_count)

                                new_records_df = get_new_records(result_df)

                                tab1, tab2 = st.tabs(["📋 Все обработанные данные", "🆕 Только новые записи"])
                                with tab1:
                                    st.dataframe(result_df, use_container_width=True)
                                    output_all = io.BytesIO()
                                    with pd.ExcelWriter(output_all, engine='openpyxl') as writer:
                                        result_df.to_excel(writer, index=False, sheet_name='Все пересдачи')
                                    output_all.seek(0)
                                    current_date = datetime.now().strftime('%d-%m-%Y')
                                    download_filename_all = f"Пересдачи_все_{current_date}.xlsx"
                                    st.download_button(
                                        label="📥 Скачать все записи (XLSX)",
                                        data=output_all.getvalue(),
                                        file_name=download_filename_all,
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
                                        download_filename_new = f"Пересдачи_новые_{current_date}.xlsx"
                                        st.download_button(
                                            label="📥 Скачать только новые записи (XLSX)",
                                            data=output_new.getvalue(),
                                            file_name=download_filename_new,
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key="download_new"
                                        )

                                with st.expander("📈 Статистика по обработке"):
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.write("**Распределение по дисциплинам:**")
                                        if 'Наименование дисциплины' in result_df.columns:
                                            discipline_counts = result_df['Наименование дисциплины'].value_counts()
                                            st.dataframe(discipline_counts)
                                    with col2:
                                        st.write("**Уникальные студенты:**")
                                        if 'ФИО' in result_df.columns:
                                            unique_students = result_df['ФИО'].nunique()
                                            st.metric("Уникальных студентов", unique_students)
                        except Exception as e:
                            st.error(f"❌ Ошибка при обработке: {str(e)}")
                            st.exception(e)
            except Exception as e:
                st.error(f"❌ Ошибка при загрузке файла: {str(e)}")
                st.exception(e)
        else:
            st.info("📁 Загрузите файл с оценками для начала работы")
            st.markdown("---")
            st.markdown("### 💡 Инструкция по использованию")
            st.markdown("""
            **Перед началом работы:**
            1. **Убедитесь, что в Supabase созданы таблицы:**
               - `students` - со списком студентов (колонки: ФИО, Адрес электронной почты, Филиал (кампус), Факультет, Образовательная программа, Группа, Курс)
               - `peresdachi` - для хранения пересдач
            **Процесс обработки:**
            1. Выберите нужные **курсы** из списка
            2. Загрузите файл с оценками
            3. Нажмите **"Обработать данные"**
            ✨ Все данные автоматически сохраняются в Supabase!
            """)
            existing_peresdachi = load_existing_peresdachi()
            with st.expander("📊 Текущее состояние базы данных"):
                if existing_peresdachi.empty:
                    st.info("ℹ️ Таблица peresdachi пуста или не создана")
                else:
                    st.metric("Записей в таблице peresdachi", len(existing_peresdachi))
                    st.dataframe(existing_peresdachi.head(10), use_container_width=True)

    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>DataCulture Platform v1.0 | Created with ❤️ by Тимошка | Powered by Streamlit 🚀</p>
        </div>
        """, 
        unsafe_allow_html=True
    )

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ МОДУЛЯ 3 (не забываем!)
# =============================================================================
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
        final_result = "\n".join(student_results) if student_results else "Навыки не найдены."
        results.append(final_result)
    processing_log.append(f"✅ Успешно обработано")
    df_result = df.copy()
    df_result['Итоговый результат'] = results
    columns_to_remove = [col for col in df_result.columns if col.startswith("Название Дисциплины ")]
    if columns_to_remove:
        df_result = df_result.drop(columns=columns_to_remove)
    return df_result, processing_log

# =============================================================================
if __name__ == "__main__":
    main()
