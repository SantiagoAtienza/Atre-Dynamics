import json

import pandas as pd
import streamlit as st

try:
    import plotly.express as px
except Exception:
    px = None

from config import settings
from config.keywords import KEYWORDS
from pipeline.lead_config import LeadSearchConfig
from pipeline.lead_pipeline import run_lead_search
from store.store import save_many_leads

st.set_page_config(page_title="LeadRadar", layout="wide")

st.title("LeadRadar - Buyer Need Intelligence")
st.caption(
    "Scraping multi-fuente + analisis IA para detectar empresas con necesidad real de compra B2B."
)

SOURCE_LABELS = {
    "web": "Webs",
    "forum": "Foros",
    "review": "Reviews",
    "social": "Redes sociales",
}

INTENT_LABELS = {
    "problema": "Problema",
    "mantenimiento": "Mantenimiento",
    "parada_produccion": "Parada de produccion",
    "compra": "Compra",
}

COUNTRY_OPTIONS = ["es", "mx", "ar", "co", "cl", "pe", "us", "all"]

with st.sidebar:
    st.subheader("Configuracion del lead")
    product_need = st.text_input(
        "Producto o necesidad",
        value="armarios electricos",
        help="Ejemplo: armarios electricos, cuadros de control, retrofit electrico",
    )
    country = st.selectbox("Pais objetivo", options=COUNTRY_OPTIONS, index=0)
    language = st.selectbox("Idioma", options=["es", "en"], index=0)
    desired_leads = st.slider("Numero de leads deseados", min_value=1, max_value=30, value=6, step=1)
    source_types = st.multiselect(
        "Fuentes a usar",
        options=list(SOURCE_LABELS.keys()),
        default=["web"],
        format_func=lambda key: SOURCE_LABELS.get(key, key),
    )
    intent_type = st.selectbox(
        "Tipo de intencion buscada",
        options=list(INTENT_LABELS.keys()),
        index=0,
        format_func=lambda key: INTENT_LABELS.get(key, key),
    )

    st.divider()
    st.subheader("Busqueda avanzada")
    keyword_text = st.text_area(
        "Keywords (coma separadas)",
        value="panel electrico industrial, cuadro electrico, armario electrico",
        height=100,
    )
    max_seed_urls = st.slider(
        "Maximo de paginas semilla",
        min_value=20,
        max_value=220,
        value=90,
        step=10,
    )
    concurrency = st.slider("Concurrencia", min_value=2, max_value=14, value=6, step=1)
    extended_scrape = st.checkbox("Scraping extenso (mas lento, mayor cobertura)", value=True)
    link_depth = st.slider(
        "Profundidad de expansion de enlaces",
        min_value=1,
        max_value=4,
        value=settings.DISCOVERY_LINK_EXPANSION_DEPTH,
        step=1,
    )
    links_per_page = st.slider(
        "Links maximos por pagina",
        min_value=5,
        max_value=40,
        value=settings.DISCOVERY_LINKS_PER_PAGE,
        step=1,
    )
    min_need_score = st.slider(
        "Umbral IA necesidad (0-100)",
        min_value=20,
        max_value=90,
        value=settings.MIN_NEED_SCORE,
    )
    target_min_leads = st.number_input(
        "Objetivo minimo de leads (fallback)",
        min_value=1,
        max_value=40,
        value=max(1, int(desired_leads)),
        step=1,
    )
    include_suppliers = st.checkbox("Incluir posibles proveedores", value=False)
    use_sample_seeds = st.checkbox("Usar seeds locales (mas estable, menos cobertura)", value=False)
    save_results = st.checkbox("Guardar en leads.jsonl", value=True)
    run_button = st.button("Ejecutar analisis", type="primary")

    st.divider()
    st.subheader("Comparacion privada")
    if st.button("Abrir portal privado de matching"):
        try:
            st.switch_page("pages/secure_match_portal.py")
        except Exception:
            st.info("Abre la pagina 'secure_match_portal.py' desde el menu de paginas de Streamlit.")


def _to_dataframe(leads: list[dict]):
    rows = []
    for lead in leads:
        signal = lead["signal"]
        page = lead["page"]
        rows.append(
            {
                "priority": signal.get("priority"),
                "score": signal.get("score"),
                "need_score": signal.get("ai_need_score", 0),
                "buyer_prob": signal.get("ai_buyer_probability", 0),
                "supplier_prob": signal.get("ai_supplier_probability", 0),
                "real_need": signal.get("ai_need_detected", False),
                "intent_type": signal.get("ai_intent_type", "problema"),
                "need_stage": signal.get("ai_need_stage", "unknown"),
                "source_type": signal.get("source_type", "web"),
                "detection_mode": signal.get("detection_mode", "strict"),
                "company_candidate": signal.get("ai_company_candidate", ""),
                "classification": signal.get("ai_classification", "neutral"),
                "title": page.get("title", ""),
                "url": lead["url"],
                "query": signal.get("source_query", ""),
                "keywords": ", ".join(signal.get("matched_keywords", [])),
                "reasons": ", ".join(signal.get("reasons", [])),
                "problem_summary": signal.get("ai_summary", ""),
            }
        )
    return pd.DataFrame(rows)


def _load_seed_sites():
    try:
        with open("seeds_sample.txt", "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip() and not line.startswith("#")]
    except Exception:
        return None


def _render_bar(title: str, x_values: list[str], y_values: list[int], colors=None):
    if not x_values:
        st.info(f"Sin datos para: {title}")
        return
    if px is not None:
        fig = px.bar(
            x=x_values,
            y=y_values,
            title=title,
            labels={"x": "", "y": "Cantidad"},
            color=x_values,
            color_discrete_sequence=colors,
        )
        st.plotly_chart(fig, use_container_width=True)
        return
    frame = pd.DataFrame({"label": x_values, "count": y_values}).set_index("label")
    st.write(title)
    st.bar_chart(frame)


if run_button:
    raw_keywords = [item.strip() for item in keyword_text.split(",") if item.strip()]
    if raw_keywords:
        base_keywords = raw_keywords
    elif product_need.strip():
        base_keywords = [product_need.strip()]
    else:
        base_keywords = KEYWORDS[:3]
    selected_sources = source_types or ["web"]
    seed_sites = _load_seed_sites() if use_sample_seeds else None

    lead_config = LeadSearchConfig.from_inputs(
        product_need=product_need,
        country=country,
        language=language,
        target_leads=int(desired_leads),
        source_types=selected_sources,
        intent_type=intent_type,
        keywords=base_keywords,
        min_need_score=min_need_score,
        include_suppliers=include_suppliers,
        extended_scrape=extended_scrape,
        link_expansion_depth=link_depth,
        links_per_page=links_per_page,
    )

    with st.spinner(
        "Fases: configuracion -> generacion de queries -> descubrimiento -> filtrado -> scraping -> analisis IA -> scoring..."
    ):
        result = run_lead_search(
            lead_config=lead_config,
            max_results=int(max_seed_urls),
            country=country,
            language=language,
            concurrency=concurrency,
            seed_sites=seed_sites,
            include_suppliers=include_suppliers,
            min_need_score=min_need_score,
            extended_scrape=extended_scrape,
            target_min_leads=int(target_min_leads),
            link_expansion_depth=link_depth,
            links_per_page=links_per_page,
        )

    leads = result["leads"]
    summary = result["summary"]

    if save_results:
        saved_count = save_many_leads(leads, output_file="leads.jsonl")
        st.info(f"Se guardaron {saved_count} leads nuevos en leads.jsonl")

    if settings.USE_OPENAI:
        st.success(f"IA generativa activa con modelo: {settings.OPENAI_MODEL}")
    else:
        st.warning("OPENAI_API_KEY no detectada. Se usa modelo heuristico local.")
    if px is None:
        st.warning("Plotly no esta instalado. Se usan graficos basicos de Streamlit.")

    blocked = summary.get("blocked_403_429", {})
    blocked_total = sum(int(value) for value in blocked.values()) if blocked else 0
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("URLs analizadas", summary.get("searched_urls", 0))
    col2.metric("Leads", summary.get("total_leads", 0))
    col3.metric("Need validada", summary.get("need_validated", 0))
    col4.metric("Avg score", summary.get("avg_score", 0))
    col5.metric("Avg need score", summary.get("avg_need_score", 0))
    col6.metric("Bloqueos 403/429", blocked_total)

    queries = result.get("queries", [])
    if queries:
        with st.expander("Queries generadas"):
            st.write("\n".join(f"- {query}" for query in queries))

    phase_metrics = result.get("phase_metrics", {})
    if phase_metrics:
        with st.expander("Metricas por fase"):
            st.json(phase_metrics)

    if not leads:
        st.warning(
            "No se encontraron leads con necesidad clara. Prueba ampliar fuentes, bajar umbral o cambiar la intencion."
        )
        st.stop()

    frame = _to_dataframe(leads)

    left, right = st.columns(2)
    with left:
        source_counts = summary.get("by_source_type", {})
        _render_bar(
            title="Leads por fuente",
            x_values=list(source_counts.keys()),
            y_values=list(source_counts.values()),
            colors=["#1D3557", "#2A9D8F", "#E9C46A", "#E76F51", "#457B9D"],
        )
    with right:
        intent_counts = summary.get("by_intent_type", {})
        _render_bar(
            title="Leads por intencion",
            x_values=list(intent_counts.keys()),
            y_values=list(intent_counts.values()),
            colors=["#264653", "#2A9D8F", "#E9C46A", "#F4A261"],
        )

    st.subheader("Leads detectados")
    st.dataframe(
        frame[
            [
                "priority",
                "score",
                "need_score",
                "real_need",
                "intent_type",
                "buyer_prob",
                "supplier_prob",
                "need_stage",
                "source_type",
                "detection_mode",
                "company_candidate",
                "classification",
                "title",
                "url",
                "query",
                "keywords",
                "reasons",
                "problem_summary",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "Descargar tabla CSV",
        data=frame.to_csv(index=False).encode("utf-8"),
        file_name="lead_report.csv",
        mime="text/csv",
    )
    st.download_button(
        "Descargar reporte JSON",
        data=json.dumps(leads, ensure_ascii=False, indent=2),
        file_name="lead_report.json",
        mime="application/json",
    )

    st.subheader("Detalle de evidencias")
    for lead in leads[:25]:
        signal = lead["signal"]
        label = (
            f"{signal.get('priority','C')} | score={signal.get('score',0)} | "
            f"need={signal.get('ai_need_score',0)} | intent={signal.get('ai_intent_type','problema')} | {lead['url']}"
        )
        with st.expander(label):
            st.write(f"Empresa candidata: {signal.get('ai_company_candidate', 'N/A')}")
            st.write(f"Fuente: {signal.get('source_type', 'web')} via {signal.get('source_name', '')}")
            st.write(f"Clasificacion IA: {signal.get('ai_classification', 'neutral')}")
            st.write(f"Intencion IA: {signal.get('ai_intent_type', 'problema')}")
            st.write(f"Resumen problema: {signal.get('ai_summary', 'N/A')}")
            st.write(f"Evidencia IA: {', '.join(signal.get('ai_evidence', [])) or 'N/A'}")
            st.write(f"Snippet: {lead.get('snippet', '')}")
