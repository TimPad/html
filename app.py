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
        model="Qwen/Qwen3-235B-A22B-Thinking-2507",
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
# ОСНОВНОЕ ПРИЛОЖЕНИЕ
# =============================================================================

def main():
    """Главная функция приложения"""
    
    # Заголовок
    st.title("❤️🌸 DataCulture Platform 🌸❤️")
    st.markdown("**Объединённая платформа инструментов Data Culture @ HSE University**")
    st.markdown("*Для самых лучших сотрудников проекта от Тимошки!*")
    st.markdown("---")
    
    # Боковая панель навигации
    with st.sidebar:
        st.image(LOGO_URL, width=200)
        st.markdown("---")
        
        tool = st.radio(
            "🎯 Выберите инструмент:",
            [
                "📊 Перезачет оценок",
                "🎓 Генератор HTML-карточек",
                "📜 Обработка сертификатов"
            ],
            index=0
        )
        
        st.markdown("---")
        st.markdown("### ℹ️ О платформе")
        st.info("""
        **DataCulture Platform** объединяет три ключевых инструмента:
        
        1. **Перезачет оценок** - автоматический расчет итоговых оценок
        2. **Генератор карточек** - создание HTML-рассылок в фирменном стиле ВШЭ
        3. **Сертификаты** - обработка данных для выдачи сертификатов
        """)
    
    # =============================================================================
    # МОДУЛЬ 1: ПЕРЕЗАЧЕТ ОЦЕНОК
    # =============================================================================
    
    if tool == "📊 Перезачет оценок":
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
    
    # =============================================================================
    # МОДУЛЬ 2: ГЕНЕРАТОР HTML-КАРТОЧЕК
    # =============================================================================
    
    elif tool == "🎓 Генератор HTML-карточек":
        st.header("🎓 Генератор карточек НИУ ВШЭ")
        
        st.markdown("""
        Создайте HTML-карточку рассылки в фирменном стиле ВШЭ с помощью искусственного интеллекта.
        
        **Как использовать:**
        1. Введите текст объявления или новости
        2. Нажмите кнопку генерации
        3. Получите готовый HTML-код и предпросмотр
        """)

        # Проверка наличия API ключа
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
    
    # =============================================================================
    # МОДУЛЬ 3: ОБРАБОТКА СЕРТИФИКАТОВ
    # =============================================================================
    
    else:
        st.header("📜 Система обработки сертификатов")
        
        st.markdown("""
        Автоматическая обработка данных экзаменов студентов и генерация текста для сертификатов.
        
        **Требуется два файла:**
        1. Excel с данными студентов (колонки: Учащийся, Дисциплина 1/2/3, Оценка 5 баллов)
        2. Excel со справочником навыков (колонки: Дисциплина, Уровень_оценки, Описание_навыков)
        """)
        
        # Боковая панель с примерами файлов
        with st.sidebar:
            st.markdown("---")
            st.markdown("### 📥 Скачать примеры")
            
            # Проверяем наличие файлов примеров
            current_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Excel пример студентов
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
            
            # Excel справочник навыков
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
    
    # Футер
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>DataCulture Platform v1.0 | Created with ❤️ by Тимошка | Powered by Streamlit 🚀</p>
        </div>
        """, 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
