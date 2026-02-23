import argparse
import subprocess
import sys
import webbrowser

from config import settings
from config.keywords import KEYWORDS
from pipeline.lead_pipeline import run_lead_search
from store.store import save_many_leads


def _load_seed_file(seed_file: str | None):
    if not seed_file:
        return None
    try:
        with open(seed_file, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip() and not line.startswith("#")]
    except Exception as error:
        print(f"[SEED FILE ERROR] {error}")
        return None


def _parse_keywords(raw: str | None):
    if not raw:
        return KEYWORDS[:3]
    return [item.strip() for item in raw.split(",") if item.strip()]


def _parse_sources(raw: str | None):
    if not raw:
        return ["web"]
    mapping = {
        "webs": "web",
        "foros": "forum",
        "reviews": "review",
        "redes": "social",
        "social_media": "social",
    }
    parsed = []
    for value in raw.split(","):
        source = (value or "").strip().lower()
        source = mapping.get(source, source)
        if source in {"web", "forum", "review", "social"}:
            parsed.append(source)
    return parsed or ["web"]


def _launch_gui():
    url = "http://localhost:8501"
    try:
        webbrowser.open(url)
    except Exception:
        pass
    cmd = [sys.executable, "-m", "streamlit", "run", "app.py"]
    subprocess.run(cmd, check=False)


def _run_cli(args):
    keywords = _parse_keywords(args.keywords)
    source_types = _parse_sources(args.sources)
    seed_sites = _load_seed_file(args.seed_file)
    if args.use_sample_seeds and not seed_sites:
        seed_sites = _load_seed_file("seeds_sample.txt")
    product_need = (args.product or "").strip() or ", ".join(keywords[:2])

    result = run_lead_search(
        keywords=keywords,
        max_results=args.max,
        desired_leads=args.desired_leads,
        product_need=product_need,
        country=args.country,
        language=args.language,
        source_types=source_types,
        intent_type=args.intent,
        concurrency=args.concurrency,
        seed_sites=seed_sites,
        include_suppliers=args.include_suppliers,
        min_need_score=args.min_need_score,
        extended_scrape=not args.fast_mode,
        target_min_leads=args.target_min_leads,
        link_expansion_depth=args.link_depth,
        links_per_page=args.links_per_page,
    )

    saved = save_many_leads(result["leads"], output_file=args.output_file)

    print(f"[SUMMARY] Total leads: {result['summary']['total_leads']}")
    print(f"[SUMMARY] Searched URLs: {result['summary']['searched_urls']}")
    print(f"[SUMMARY] Avg score: {result['summary']['avg_score']}")
    print(f"[SUMMARY] Avg need score: {result['summary']['avg_need_score']}")
    print(f"[SUMMARY] By strength: {result['summary']['by_strength']}")
    print(f"[SUMMARY] By priority: {result['summary']['by_priority']}")
    print(f"[SUMMARY] By source: {result['summary'].get('by_source_type', {})}")
    print(f"[SUMMARY] By intent: {result['summary'].get('by_intent_type', {})}")
    print(f"[SUMMARY] By detection mode: {result['summary'].get('by_detection_mode', {})}")
    print(f"[SUMMARY] Strict leads: {result['summary'].get('strict_leads', 0)}")
    print(f"[SUMMARY] Relaxed leads added: {result['summary'].get('relaxed_leads_added', 0)}")
    print(f"[SUMMARY] AI enabled: {settings.USE_OPENAI} model={settings.OPENAI_MODEL}")
    print(f"[SAVED] {saved} new leads in {args.output_file}")

    top = result["leads"][:10]
    for lead in top:
        signal = lead["signal"]
        print(
            f"- {signal['priority']} | {signal['strength']} | score={signal['score']} "
            f"| need={signal.get('ai_need_score', 0)}"
            f"| buyer_p={signal.get('ai_buyer_probability', 0)}"
            f"| src={signal.get('source_type', 'web')}"
            f"| mode={signal.get('detection_mode', 'strict')}"
            f"| company={signal.get('ai_company_candidate', '')}"
            f" | {lead['url']}"
        )

    if not result["leads"]:
        print("[TIP] No leads found. Try --country all, broader keywords, or lower --min-need-score.")


def main():
    parser = argparse.ArgumentParser(
        description="LeadRadar: discover and classify industrial leads."
    )
    parser.add_argument("--gui", action="store_true", help="Open graphical interface")
    parser.add_argument("--product", type=str, help="Product or need, e.g. armarios electricos")
    parser.add_argument("--keywords", type=str, help="Comma-separated keywords")
    parser.add_argument("--country", type=str, default="es", help="Country focus: es or all")
    parser.add_argument("--language", type=str, default="es", help="Language: es or en")
    parser.add_argument("--intent", type=str, default="problema", help="Intent: problema|mantenimiento|parada_produccion|compra")
    parser.add_argument("--sources", type=str, default="web", help="Sources: comma-separated web,forum,review,social")
    parser.add_argument("--desired-leads", type=int, default=6, help="Final number of leads to return")
    parser.add_argument("--max", type=int, default=30, help="Maximum seed URLs (link expansion may evaluate more)")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent page fetches")
    parser.add_argument("--min-need-score", type=int, default=settings.MIN_NEED_SCORE, help="Minimum AI need score")
    parser.add_argument("--include-suppliers", action="store_true", help="Include supplier/vendor pages")
    parser.add_argument("--fast-mode", action="store_true", help="Reduce scraping depth and run faster")
    parser.add_argument("--target-min-leads", type=int, default=settings.TARGET_MIN_LEADS, help="Minimum leads to return using fallback mode")
    parser.add_argument("--link-depth", type=int, default=settings.DISCOVERY_LINK_EXPANSION_DEPTH, help="Link expansion depth for deep scraping")
    parser.add_argument("--links-per-page", type=int, default=settings.DISCOVERY_LINKS_PER_PAGE, help="Maximum links extracted per page during expansion")
    parser.add_argument("--seed-file", type=str, help="File containing seed URLs (one per line)")
    parser.add_argument(
        "--use-sample-seeds",
        action="store_true",
        help="Use seeds_sample.txt to avoid search-engine timeouts",
    )
    parser.add_argument("--output-file", type=str, default="leads.jsonl", help="Output report path")
    args = parser.parse_args()

    # By default open GUI when no explicit CLI params are given.
    no_cli_params = len(sys.argv) == 1
    if args.gui or no_cli_params:
        _launch_gui()
        return

    _run_cli(args)


if __name__ == "__main__":
    main()
