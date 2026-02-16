# Sound of the Seasons - Data Warehouse

Analyse des Einflusses von Wetter und Jahreszeiten auf Spotify Charts (Deutschland, 2020-2026). Erneuert sich Wöchentlich über Git Action. 

---

## Quick Start

Vor der Durchführung muss **Python 3.11 oder höher** installiert sein und die benötigten Pakete aus `requirements.txt`:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 1. ETL ausführen
```bash
python etl/run_etl.py
```
**Config:** `etl/config.py` (API Keys, Pfade, Einstellungen)

### 2. Dashboard generieren
```bash
python visualization/generate_dashboard.py
```
**Output:** `docs/index.html`

### 3. Lokal testen
```bash
python -m http.server --directory docs 8000
```
Öffne: http://localhost:8000

---

**Queries:** Alle SQL-Abfragen in `visualization/stats.py`

