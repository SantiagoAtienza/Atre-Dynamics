LeadRadar MVP - Buyer Need Detection

Quick start
1. pip install -r requirements.txt
2. python main.py

What this version does
- Multi-source discovery: web, forums, social-like sources, reviews, news.
- Company-first mode by default: starts from web sources and prioritizes corporate/blog/news paths over community pages.
- AI lead model focused on buyer need detection (not supplier offer pages).
- Supplier filtering enabled by default.
- Extended scraping mode with deep link expansion and fallback relaxed detection.
- Graphical UI with source mix, need stage, and AI evidence.
- Private DB matching portal to compare client DB vs lead list using hashed fingerprints.

GUI
- `python main.py` opens Streamlit automatically.
- Recommended first run:
  - Disable "Incluir posibles proveedores"
  - Keep min need score around 40-45
  - Keep "Scraping extenso" enabled
  - Increase max pages to 80+
  - Use target min leads = 1
- Private match portal:
  - Open button "Abrir portal privado de matching" in sidebar
  - Upload both files, define prompt + columns, run private comparison

CLI examples
- Buyer-only (default):
  `python main.py --keywords "panel electrico industrial,cuadro electrico" --country es --max 100 --min-need-score 42 --target-min-leads 1`
- Include supplier pages for comparison:
  `python main.py --keywords "panel electrico" --include-suppliers --max 60`
- Faster/less deep run:
  `python main.py --keywords "panel electrico" --max 50 --fast-mode`
- Stable fallback mode:
  `python main.py --keywords "panel electrico" --use-sample-seeds --max 40`

AI mode (OpenAI)
1. Set API key (PowerShell):
   `$env:OPENAI_API_KEY="your_key_here"`
2. Optional model:
   `$env:OPENAI_MODEL="gpt-4o-mini"`

Output
- Leads saved in `leads.jsonl`
- JSON report downloadable from UI
- Private match report downloadable from private portal page
