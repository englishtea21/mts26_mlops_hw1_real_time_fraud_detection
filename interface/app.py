import json
import os
import time
import uuid

import altair as alt
import pandas as pd
import streamlit as st
from kafka import KafkaProducer

try:
    from psycopg import connect as pg_connect
    from psycopg.errors import OperationalError as PgOperationalError

    PG_AVAILABLE = True
except ImportError:  # локальный запуск без psycopg
    PG_AVAILABLE = False
    PgOperationalError = Exception

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS"),
    "topic": os.getenv("KAFKA_TRANSACTIONS_TOPIC"),
}

# Конфигурация PostgreSQL (витрина scores, пишет postgres-logger).
PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT")),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "dbname": os.getenv("POSTGRES_DB"),
}


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
            security_protocol="PLAINTEXT",
        )

        # Генерация уникальных ID для всех транзакций
        df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        progress_bar = st.progress(0)
        total_rows = len(df)

        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic,
                value={
                    "transaction_id": row["transaction_id"],
                    "data": row.drop("transaction_id").to_dict(),
                },
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
if "results" not in st.session_state:
    st.session_state.results = None  # {'fraud': df, 'hist': df}


def fetch_results():
    """10 последних фродовых + скоры последних 100 транзакций из витрины."""
    if not PG_AVAILABLE:
        st.error("Драйвер psycopg не установлен в этом окружении")
        return None
    try:
        conn = pg_connect(
            host=PG_CONFIG["host"],
            port=PG_CONFIG["port"],
            user=PG_CONFIG["user"],
            password=PG_CONFIG["password"],
            dbname=PG_CONFIG["dbname"],
            connect_timeout=5,
        )
    except Exception as e:
        st.error(f"Нет подключения к PostgreSQL: {e}")
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT transaction_id, score, fraud_flag, created_at
                FROM scores
                WHERE fraud_flag = 1
                ORDER BY created_at DESC
                LIMIT 10
                """
            )
            fraud_rows = cur.fetchall()
            fraud_cols = [d[0] for d in cur.description]
            cur.execute(
                """
                SELECT score
                FROM scores
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
            hist_rows = cur.fetchall()
    except Exception as e:
        st.error(f"Ошибка чтения витрины scores: {e}")
        return None
    finally:
        conn.close()
    return {
        "fraud": pd.DataFrame(fraud_rows, columns=fraud_cols),
        "hist": pd.DataFrame(hist_rows, columns=["score"]),
    }


def render_results_tab():
    st.header("📊 Результаты скоринга")

    if st.button("Посмотреть результаты"):
        with st.spinner("Читаю витрину scores..."):
            st.session_state.results = fetch_results()

    res = st.session_state.results
    if res is None:
        st.info("Нажмите «Посмотреть результаты», чтобы загрузить данные из PostgreSQL")
        return

    st.subheader("🚨 10 последних транзакций с fraud_flag == 1")
    if res["fraud"].empty:
        st.info("Фродовых транзакций в базе пока нет")
    else:
        st.dataframe(res["fraud"], use_container_width=True)

    st.subheader("📈 Распределение скоров последних 100 транзакций")
    if res["hist"].empty:
        st.info("В базе пока нет ни одной транзакции")
    else:
        st.caption(f"Построено по {len(res['hist'])} транзакциям")
        chart = (
            alt.Chart(res["hist"])
            .mark_bar()
            .encode(
                x=alt.X("score:Q", bin=alt.Bin(maxbins=20), title="score"),
                y=alt.Y("count()", title="количество"),
            )
        )
        st.altair_chart(chart, use_container_width=True)


# Интерфейс
st.title("💳 Fraud Detection")
tab_send, tab_results = st.tabs(["📤 Отправка данных", "📊 Результаты"])

with tab_send:
    st.header("📤 Отправка данных в Kafka")

    # Блок загрузки файлов
    uploaded_file = st.file_uploader("Загрузите CSV файл с транзакциями", type=["csv"])

    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        # Добавляем файл в состояние
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file),
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
                                KAFKA_CONFIG["bootstrap_servers"],
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = (
                                    "Отправлен"
                                )
                                st.rerun()
                    else:
                        st.error("Файл не содержит данных")

with tab_results:
    render_results_tab()
