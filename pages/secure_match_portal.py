import json
from secrets import token_hex

import streamlit as st

from privacy import (
    compare_private_fingerprint_files,
    compare_raw_dataframes,
    infer_columns_from_prompt,
    infer_match_mode_from_prompt,
    load_table,
)

st.set_page_config(page_title="LeadRadar - Private Match Portal", layout="wide")


def _csv_bytes(frame):
    return frame.to_csv(index=False).encode("utf-8")


def _mode_index(mode: str):
    options = ["hybrid", "all", "any"]
    try:
        return options.index(mode)
    except Exception:
        return 0


st.title("Portal Privado de Comparacion de Bases de Datos")
st.caption(
    "Compara la base de datos del cliente con tu lista de leads usando huellas hash. "
    "La app no muestra contenido de filas, solo metadatos y resultados de matching."
)

col_nav_1, col_nav_2 = st.columns([1, 3])
with col_nav_1:
    if st.button("Volver al buscador"):
        try:
            st.switch_page("app.py")
        except Exception:
            st.info("Abre `app.py` desde el menu de paginas de Streamlit.")

st.info(
    "Privacidad operativa: los datos se procesan en memoria de la sesion actual, no se renderizan filas "
    "ni se guardan automaticamente en disco."
)

portal_mode = st.radio(
    "Flujo",
    options=[
        "Comparar archivos originales (hash interno)",
        "Comparar archivos ya anonimizados (fingerprints)",
    ],
    horizontal=True,
)

if portal_mode == "Comparar archivos originales (hash interno)":
    st.subheader("Carga de archivos")
    col1, col2 = st.columns(2)
    with col1:
        client_file = st.file_uploader(
            "Base de datos del cliente (CSV/XLSX)",
            type=["csv", "xlsx", "xls"],
            key="client_raw_file",
        )
    with col2:
        lead_file = st.file_uploader(
            "Lista de leads (CSV/XLSX)",
            type=["csv", "xlsx", "xls"],
            key="leads_raw_file",
        )

    if client_file and lead_file:
        try:
            client_df = load_table(client_file)
            leads_df = load_table(lead_file)
        except Exception as error:
            st.error(f"No se pudieron leer los archivos: {error}")
            st.stop()

        with st.expander("Metadatos detectados (sin contenido de filas)", expanded=True):
            left, right = st.columns(2)
            with left:
                st.write(f"Registros cliente: {len(client_df)}")
                st.write(f"Columnas cliente: {', '.join(client_df.columns.tolist())}")
            with right:
                st.write(f"Registros leads: {len(leads_df)}")
                st.write(f"Columnas leads: {', '.join(leads_df.columns.tolist())}")

        st.subheader("Regla de matching")
        prompt = st.text_area(
            "Prompt/regla de comparacion",
            value=(
                "Compara por email y dominio en modo exacto. "
                "Si falta alguno, usa nombre de empresa y telefono."
            ),
            height=110,
            help="Describe que columnas deben compararse y si quieres modo exacto (all) o flexible (any).",
        )

        suggested_client_cols = infer_columns_from_prompt(client_df.columns.tolist(), prompt)
        suggested_lead_cols = infer_columns_from_prompt(leads_df.columns.tolist(), prompt)
        inferred_mode = infer_match_mode_from_prompt(prompt)

        col_rule_1, col_rule_2 = st.columns(2)
        with col_rule_1:
            client_cols = st.multiselect(
                "Columnas cliente para comparar",
                options=client_df.columns.tolist(),
                default=[col for col in suggested_client_cols if col in client_df.columns.tolist()],
            )
            client_id_col = st.selectbox(
                "Columna ID cliente (opcional)",
                options=["(auto)"] + client_df.columns.tolist(),
                index=0,
            )
        with col_rule_2:
            lead_cols = st.multiselect(
                "Columnas leads para comparar",
                options=leads_df.columns.tolist(),
                default=[col for col in suggested_lead_cols if col in leads_df.columns.tolist()],
            )
            lead_id_col = st.selectbox(
                "Columna ID lead (opcional)",
                options=["(auto)"] + leads_df.columns.tolist(),
                index=0,
            )

        mode = st.selectbox(
            "Modo de coincidencia",
            options=["hybrid", "all", "any"],
            index=_mode_index(inferred_mode),
            help="hybrid: compuesto + campos individuales, all: coincidencia exacta de todos los campos, any: cualquiera.",
        )
        shared_secret = st.text_input(
            "Clave secreta compartida para hash (recomendada)",
            type="password",
            help="Usa la misma clave para ambos conjuntos para poder compararlos.",
        )

        if st.button("Ejecutar comparacion privada", type="primary"):
            if not client_cols or not lead_cols:
                st.error("Selecciona al menos una columna en cada archivo.")
                st.stop()

            secret = shared_secret.strip() or token_hex(16)
            if not shared_secret.strip():
                st.warning("No se indico clave secreta. Se uso una clave efimera para esta sesion.")

            try:
                result = compare_raw_dataframes(
                    client_df=client_df,
                    leads_df=leads_df,
                    client_columns=client_cols,
                    lead_columns=lead_cols,
                    secret=secret,
                    mode=mode,
                    client_id_column=None if client_id_col == "(auto)" else client_id_col,
                    lead_id_column=None if lead_id_col == "(auto)" else lead_id_col,
                )
            except Exception as error:
                st.error(f"Error en comparacion: {error}")
                st.stop()

            summary = result["summary"]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Registros cliente", summary["client_records_compared"])
            m2.metric("Registros lead", summary["lead_records_compared"])
            m3.metric("Ya existentes", summary["existing_clients_found"])
            m4.metric("Leads nuevos", summary["new_leads_found"])

            st.subheader("Estado de leads")
            st.dataframe(result["lead_status"], use_container_width=True, hide_index=True)

            st.download_button(
                "Descargar estado de leads (CSV)",
                data=_csv_bytes(result["lead_status"]),
                file_name="lead_match_status.csv",
                mime="text/csv",
            )
            st.download_button(
                "Descargar resumen (JSON)",
                data=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
                file_name="lead_match_summary.json",
                mime="application/json",
            )
            st.download_button(
                "Descargar huellas cliente (CSV)",
                data=_csv_bytes(result["client_private"]),
                file_name="client_private_fingerprints.csv",
                mime="text/csv",
            )
            st.download_button(
                "Descargar huellas leads (CSV)",
                data=_csv_bytes(result["leads_private"]),
                file_name="leads_private_fingerprints.csv",
                mime="text/csv",
            )

else:
    st.subheader("Comparacion de archivos anonimizados")
    st.write(
        "Usa este flujo cuando cada parte ya genero su archivo privado con columnas `row_id` y `fingerprints`."
    )

    col1, col2 = st.columns(2)
    with col1:
        client_private_file = st.file_uploader(
            "Archivo privado cliente (CSV)",
            type=["csv"],
            key="client_private_file",
        )
    with col2:
        leads_private_file = st.file_uploader(
            "Archivo privado leads (CSV)",
            type=["csv"],
            key="leads_private_file",
        )

    if client_private_file and leads_private_file:
        try:
            client_private = load_table(client_private_file)
            leads_private = load_table(leads_private_file)
        except Exception as error:
            st.error(f"No se pudieron leer los archivos privados: {error}")
            st.stop()

        required_columns = {"row_id", "fingerprints"}
        if not required_columns.issubset(set(client_private.columns)) or not required_columns.issubset(set(leads_private.columns)):
            st.error("Ambos archivos deben incluir `row_id` y `fingerprints`.")
            st.stop()

        if st.button("Comparar fingerprints", type="primary"):
            result = compare_private_fingerprint_files(
                client_private=client_private,
                leads_private=leads_private,
            )
            summary = result["summary"]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Registros cliente", summary["client_records_compared"])
            m2.metric("Registros lead", summary["lead_records_compared"])
            m3.metric("Ya existentes", summary["existing_clients_found"])
            m4.metric("Leads nuevos", summary["new_leads_found"])

            st.dataframe(result["lead_status"], use_container_width=True, hide_index=True)
            st.download_button(
                "Descargar estado de leads (CSV)",
                data=_csv_bytes(result["lead_status"]),
                file_name="lead_match_status.csv",
                mime="text/csv",
            )
            st.download_button(
                "Descargar resumen (JSON)",
                data=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
                file_name="lead_match_summary.json",
                mime="application/json",
            )
