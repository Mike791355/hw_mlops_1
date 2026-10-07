import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from kafka import KafkaProducer
import json
import time
import os
import uuid

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

# Конфигурация PostgreSQL
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud"),
}
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "scores")


def get_pg_connection():
    """Подключение к PostgreSQL."""
    return psycopg2.connect(**POSTGRES_CONFIG)


def _query_df(query, params, columns):
    """Выполнить запрос и вернуть результат в виде DataFrame."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=columns)


def fetch_last_frauds(limit=10):
    """Последние записи с флагом фрода (fraud_flag == 1)."""
    query = (
        f"SELECT transaction_id, score, fraud_flag, created_at "
        f"FROM {POSTGRES_TABLE} WHERE fraud_flag = 1 "
        f"ORDER BY id DESC LIMIT %s"
    )
    return _query_df(query, (limit,), ["transaction_id", "score", "fraud_flag", "created_at"])


def fetch_last_scores(limit=100):
    """Скоры последних транзакций для построения гистограммы."""
    query = f"SELECT score FROM {POSTGRES_TABLE} ORDER BY id DESC LIMIT %s"
    return _query_df(query, (limit,), ["score"])

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

# ---------------------------------------------------------------------------
# Раздел просмотра результатов скоринга из PostgreSQL
# ---------------------------------------------------------------------------
st.divider()
st.header("📊 Результаты скоринга")

if st.button("Посмотреть результаты"):
    try:
        # 1. Последние 10 фродовых транзакций
        st.subheader("🚨 Последние 10 фродовых транзакций (fraud_flag == 1)")
        frauds = fetch_last_frauds(limit=10)
        if frauds.empty:
            st.info("Фродовых транзакций пока нет в базе.")
        else:
            st.dataframe(frauds, use_container_width=True)

        # 2. Гистограмма скоров последних 100 транзакций
        st.subheader("📈 Распределение скоров последних 100 транзакций")
        scores = fetch_last_scores(limit=100)
        if scores.empty:
            st.info("В базе пока нет результатов скоринга.")
        else:
            fig, ax = plt.subplots()
            ax.hist(scores["score"], bins=20, color="#4C72B0", edgecolor="black")
            ax.set_xlabel("Скор модели")
            ax.set_ylabel("Количество транзакций")
            ax.set_title(f"Гистограмма скоров (последние {len(scores)} транзакций)")
            st.pyplot(fig)
    except Exception as e:
        st.error(f"Не удалось получить результаты из базы: {e}")