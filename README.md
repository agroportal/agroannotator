# AgroAnnotator for AgrospAI

AgroAnnotator is a Compute-to-Data (CtD) algorithm for **AgrospAI** that:

* extracts text from **PDF / HTML / TXT / DOCX**
* calls the **AgroPortal Annotator API**
* handles large documents via **chunking**
* merges/deduplicates annotations across chunks
* generates **JSON + CSV summaries** with concept scores

This project was designed to work both:

1. **Locally** (terminal, file path passed as argument)
2. In **Pontus-X / AgrospAI** CtD runtime (where input files are staged under `/data/inputs` and outputs are collected from `/data/outputs`)

---

## Key behaviors

### Pontus-X compatibility

In AgrospAI/Pontus-X, algorithms are often run as:

```bash
python $ALGO
```

Meaning the platform runs your code **without any CLI arguments**.

AgroAnnotator supports this by:

* making the input argument optional
* auto-discovering the dataset input file under:

```
/data/inputs/<something>/0
```

(typical staging pattern)

* writing outputs by default to:

```
/data/outputs
```

(typical Pontus-X outputs collection path)

---

## Supported inputs

The script accepts **one input file** (or stdin).

### File formats

* **PDF (.pdf)** — extracted via PyPDF2
* **HTML (.html, .htm)** — visible text extracted (scripts/styles removed) using BeautifulSoup library (or a fallback parser if the library isn't available at the run time).
* **Plain text (.txt, .md)**
* **DOCX (.docx)** — extracted via python-docx
* **No-extension files** (Pontus-X staging)

Pontus-X may stage the input as a file named `0` with **no extension**.
AgroAnnotator detects this by sniffing content (e.g., `%PDF-` header for PDFs).

---

## Configuration

### AgroPortal API key

The algorithm uses a **dedicated API key embedded in code by default**.

You can override via environment variable:

```
AGROPORTAL_API_KEY
```

---

### Default ontology

If no ontology is provided, the default is:

```
AGROPORTAL_DEFAULT_ONTOLOGY
```

Default value:

```
AGROVOC
```

---

### Label language

If label resolution is enabled, you can request a preferred label language:

```
AGROPORTAL_LABEL_LANG
```

Default:

```
en
```

> Not all ontologies provide labels for all languages. Resolution is best-effort.

---

## Output files

All outputs are written to:

* `--out` (local), or
* `/data/outputs` (Pontus-X default)

### Files produced

#### Chunk-level raw responses

```
chunk_0001.json
chunk_0002.json
...
```

Raw AgroPortal Annotator API responses for each chunk.

---

#### Combined chunk metadata + chunck response

```
combined.json
```

Links each chunk’s metadata (start/end offsets) to the chunk response.

---

#### Deduplicated annotations

```
merged_annotations.json
```

Contains global offsets:

* `_global_from`
* `_global_to`
* `_chunk_index`

---

#### Aggregated concept summary



Includes:

* ontology URL
* concept URI
* label (best-effort)
* count
* example matches

Exports:

```
concepts_summary.csv
```
```
concepts_summary.json
```

---

#### Run metadata

```
run_metadata.json
```

Contains:

* input info
* chunking settings
* ontology selection
* counts
* run details

---

## Local usage (terminal)

### 1) Basic run (PDF)

```bash
python algo.py "/path/to/file.pdf" --out out_pdf
```

### 2) Choose ontologies

```bash
python algo.py "/path/to/file.pdf" --out out_pdf --ontologies AGROVOC APTO
```

### 3) HTML file

```bash
python algo.py "/path/to/file.html" --out out_html
```

### 4) Plain text

```bash
python algo.py "/path/to/file.txt" --out out_txt
```

---

## Pontus-X / AgrospAI usage

### Option B — `$ALGO` execution

Typical Pontus-X flow:

* pull a **Docker image**
* download algorithm from **Algorithm URL**
* run with:

```bash
python $ALGO
```

#### AgrospAI algorithm registration

* **Docker image:** setecores96/humble-annotator:0.1.1
* **Entrypoint:** `python $ALGO`
* **Algorithm URL:** `algo.py`

Dataset is staged under:

```
/data/inputs/...
```

Script auto-detects it.

> Dataset policy note: datasets must **allow/trust** the algorithm to run.

---

## Chunking

Large documents are split into **chunks**:

* `--chunk-size` (default ≈ 8k chars)
* `--overlap` (default ≈ 200 chars)

Benefits:

* smaller API requests
* higher reliability
* preserves matches near chunk boundaries

---

## Development notes

### Dependencies

#### Core

* `requests`

#### Format handlers

* `PyPDF2` (PDF)
* `beautifulsoup4` (HTML extraction)
* `python-docx` (DOCX)

---

### Suggested local sanity checks

```bash
python -m py_compile algo.py
python -c "import requests; print('requests OK')"
python -c "import PyPDF2; print('PyPDF2 OK')"
python -c "import bs4; print('beautifulsoup4 OK')"
python -c "import docx; print('python-docx OK')"
```

---

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en)

---

