#!/usr/bin/env python3
"""
AgroAnnotator / HumbleAnnotator (Pontus-X compatible)

Key fix for AgrospAI/Pontus-X:
- If executed as `python $ALGO` with no CLI args, auto-detect the dataset file
  from /data/inputs/... and write outputs to /data/outputs by default.

Supported inputs:
- .pdf, .html/.htm, .txt/.md, .docx, or "-" for stdin (local usage)
"""

from __future__ import annotations

import argparse
import csv
import html as html_lib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests

DEFAULT_BASE_URL = "https://data.agroportal.lirmm.fr"
DEFAULT_API_KEY = os.environ.get("AGROPORTAL_API_KEY", "2ae6878b-a599-4cee-8224-e8efaf6f610e")
DEFAULT_ONTOLOGY = (os.environ.get("AGROPORTAL_DEFAULT_ONTOLOGY") or "AGROVOC").strip() or "AGROVOC"
DEFAULT_LABEL_LANG = (os.environ.get("AGROPORTAL_LABEL_LANG") or "en").strip() or "en"

DEFAULT_CHUNK_SIZE = 8000
DEFAULT_OVERLAP = 200
DEFAULT_TIMEOUT_S = 45
DEFAULT_SLEEP_S = 0.15
DEFAULT_LABELS_SLEEP_S = 0.05
MAX_RETRIES = 5


@dataclass(frozen=True)
class Chunk:
    index: int
    start: int
    end: int
    text: str


class InputError(Exception):
    pass


class ApiError(Exception):
    pass


def _normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _read_text_file(path: Path, encoding: str) -> str:
    try:
        return path.read_text(encoding=encoding, errors="replace")
    except Exception as e:
        raise InputError(f"Failed to read: {path} ({e})") from e


def _read_pdf(path: Path) -> str:
    try:
        from PyPDF2 import PdfReader  # type: ignore
    except Exception as e:
        raise InputError("Missing dependency for PDF: pip install PyPDF2") from e

    try:
        reader = PdfReader(str(path))
        parts: List[str] = []
        for page in reader.pages:
            t = page.extract_text() or ""
            if t.strip():
                parts.append(t)
        return "\n\n".join(parts)
    except Exception as e:
        raise InputError(f"Failed to read PDF: {path} ({e})") from e


def _read_docx(path: Path) -> str:
    try:
        from docx import Document  # python-docx
    except Exception as e:
        raise InputError("Missing dependency for DOCX: pip install python-docx") from e

    try:
        doc = Document(str(path))
        parts = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]
        return "\n".join(parts)
    except Exception as e:
        raise InputError(f"Failed to read DOCX: {path} ({e})") from e


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: List[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        t = tag.lower()
        if t in {"script", "style", "noscript"}:
            self._skip += 1
        if t in {"p", "br", "div", "li", "tr", "td", "th", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self._chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in {"script", "style", "noscript"} and self._skip > 0:
            self._skip -= 1
        if t in {"p", "div", "li", "tr"}:
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip > 0:
            return
        if data and data.strip():
            self._chunks.append(data)

    def get_text(self) -> str:
        raw = " ".join(self._chunks)
        raw = html_lib.unescape(raw)
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def _html_to_text(html_str: str) -> str:
    try:
        from bs4 import BeautifulSoup  # type: ignore

        soup = BeautifulSoup(html_str, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        return _normalize_text(html_lib.unescape(text))
    except Exception:
        parser = _HTMLTextExtractor()
        parser.feed(html_str)
        return _normalize_text(parser.get_text())


def _sniff_input_type(path: Path) -> str:
    """
    For Pontus-X inputs that come without extensions (e.g. .../0),
    sniff content to decide how to parse.
    """
    try:
        head = path.read_bytes()[:4096]
    except Exception:
        return "text"

    if head.startswith(b"%PDF-"):
        return "pdf"

    # cheap HTML sniff
    low = head.lower()
    if b"<html" in low or b"<!doctype html" in low or b"<body" in low:
        return "html"

    # otherwise treat as text
    return "text"


def load_input_text(input_arg: str, encoding: str) -> str:
    if input_arg == "-":
        return sys.stdin.read()

    p = Path(input_arg)
    if not p.exists():
        raise InputError(f"Input path does not exist: {p}")

    if p.is_dir():
        # If a directory is passed, pick first file inside.
        files = [f for f in p.rglob("*") if f.is_file()]
        if not files:
            raise InputError(f"Input directory is empty: {p}")
        p = files[0]

    ext = p.suffix.lower()
    if ext in {".txt", ".md"}:
        return _read_text_file(p, encoding)
    if ext in {".html", ".htm"}:
        return _html_to_text(_read_text_file(p, encoding))
    if ext == ".pdf":
        return _read_pdf(p)
    if ext == ".docx":
        return _read_docx(p)

    # No/unknown extension -> sniff (Pontus-X typical)
    kind = _sniff_input_type(p)
    if kind == "pdf":
        return _read_pdf(p)
    if kind == "html":
        return _html_to_text(_read_text_file(p, encoding))
    return _read_text_file(p, encoding)


def chunk_text(text: str, max_chars: int, overlap: int) -> List[Chunk]:
    if max_chars <= 0:
        raise ValueError("chunk_size must be > 0")
    if overlap < 0 or overlap >= max_chars:
        raise ValueError("overlap must be >=0 and < chunk_size")

    if len(text) <= max_chars:
        return [Chunk(index=1, start=0, end=len(text), text=text)]

    chunks: List[Chunk] = []
    i = 0
    idx = 1
    n = len(text)

    while i < n:
        end = min(i + max_chars, n)

        if end < n:
            window_start = max(i, end - 500)
            window = text[window_start:end]
            m = re.search(r"\s(?!.*\s)", window)
            if m:
                end = window_start + m.start()

        if end <= i:
            end = min(i + max_chars, n)

        chunk_str = text[i:end].strip()
        if chunk_str:
            chunks.append(Chunk(index=idx, start=i, end=end, text=chunk_str))
            idx += 1

        if end >= n:
            break

        i = max(0, end - overlap)

    return chunks


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Dict[str, Any],
    data: Optional[Dict[str, Any]],
    headers: Dict[str, str],
    timeout_s: int,
    max_retries: int,
) -> Any:
    last: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, params=params, data=data, headers=headers, timeout=timeout_s)
            if resp.status_code in {429, 500, 502, 503, 504}:
                time.sleep(min(2 ** (attempt - 1), 30.0))
                continue
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception as e:
                raise ApiError(f"Non-JSON response: {resp.text[:500]}") from e
        except Exception as e:
            last = e
            if attempt < max_retries:
                time.sleep(min(2 ** (attempt - 1), 30.0))
                continue
    raise ApiError(f"Request failed after {max_retries} attempts: {last}") from last


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(p: Path, payload: Any) -> None:
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(p: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _parse_ontology_list(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = re.split(r"[,\s]+", raw)
    return [p.strip().upper() for p in parts if p.strip()]


def _parse_ontologies_from_any(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for v in value:
            if isinstance(v, str):
                out.extend(_parse_ontology_list(v))
        return out
    if isinstance(value, str):
        return _parse_ontology_list(value)
    return []


def _bounded_find_file(root: Path, name: str, max_depth: int = 4) -> Optional[Path]:
    try:
        root = root.resolve()
    except Exception:
        pass

    def walk(cur: Path, depth: int) -> Optional[Path]:
        if depth > max_depth:
            return None
        candidate = cur / name
        if candidate.is_file():
            return candidate
        try:
            for child in cur.iterdir():
                if child.is_dir():
                    found = walk(child, depth + 1)
                    if found:
                        return found
        except Exception:
            return None
        return None

    return walk(root, 0)


def load_algo_custom_data() -> Dict[str, Any]:
    env_paths = [
        os.environ.get("ALGO_CUSTOM_DATA"),
        os.environ.get("OCEAN_ALGO_CUSTOM_DATA"),
        os.environ.get("AGROSPAI_ALGO_CUSTOM_DATA"),
    ]
    for p in env_paths:
        if p:
            fp = Path(p)
            if fp.is_file():
                try:
                    return json.loads(fp.read_text(encoding="utf-8"))
                except Exception:
                    return {}

    common_roots = [Path("/data/inputs"), Path.cwd(), Path("/data"), Path("/tmp")]
    for root in common_roots:
        if root.exists() and root.is_dir():
            found = _bounded_find_file(root, "algoCustomData.json", max_depth=4)
            if found:
                try:
                    return json.loads(found.read_text(encoding="utf-8"))
                except Exception:
                    return {}

    return {}


def resolve_ontologies(args: argparse.Namespace) -> List[str]:
    if args.ontologies:
        return [o.strip().upper() for o in args.ontologies if o.strip()]

    custom = load_algo_custom_data()
    if isinstance(custom, dict):
        parsed = _parse_ontologies_from_any(custom.get("ontologies"))
        if parsed:
            return parsed

    for key in ("ONTOLOGIES", "ontologies", "AGROSPAI_ONTOLOGIES", "PARAM_ONTOLOGIES", "ALGO_PARAM_ONTOLOGIES"):
        parsed = _parse_ontology_list(os.environ.get(key, ""))
        if parsed:
            return parsed

    default_list = _parse_ontology_list(DEFAULT_ONTOLOGY) or ["AGROVOC"]
    if args.no_prompt_ontology or not (sys.stdin.isatty() and sys.stdout.isatty()):
        return default_list

    entered = input(
        "Enter ontology acronym(s) (comma/space-separated). "
        f"Press Enter for default [{', '.join(default_list)}]: "
    )
    chosen = _parse_ontology_list(entered)
    return chosen or default_list


def parse_bool(s: str) -> str:
    s2 = s.strip().lower()
    if s2 in {"true", "t", "1", "yes", "y"}:
        return "true"
    if s2 in {"false", "f", "0", "no", "n"}:
        return "false"
    raise argparse.ArgumentTypeError("Expected boolean (true/false)")


def build_annotator_params(args: argparse.Namespace) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if args.ontologies:
        params["ontologies"] = ",".join(args.ontologies)
    if args.semantic_types:
        params["semantic_types"] = ",".join(args.semantic_types)

    params["whole_word_only"] = args.whole_word_only
    params["exclude_numbers"] = args.exclude_numbers
    params["exclude_synonyms"] = args.exclude_synonyms
    params["longest_only"] = args.longest_only
    params["expand_mappings"] = args.expand_mappings
    params["expand_class_hierarchy"] = args.expand_class_hierarchy
    params["class_hierarchy_max_level"] = str(args.class_hierarchy_max_level)

    if args.minimum_match_length is not None:
        params["minimum_match_length"] = str(args.minimum_match_length)
    if args.stop_words:
        params["stop_words"] = ",".join(args.stop_words)
    if args.format:
        params["format"] = args.format
    if args.display_context is not None:
        params["display_context"] = args.display_context
    if args.display_links is not None:
        params["display_links"] = args.display_links

    return {k: v for k, v in params.items() if v is not None}


def _iter_annotations(resp: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(resp, list):
        for it in resp:
            if isinstance(it, dict):
                yield it


def _concept_id(ann: Dict[str, Any]) -> str:
    ac = ann.get("annotatedClass")
    if isinstance(ac, dict):
        v = ac.get("@id") or ac.get("id")
        if isinstance(v, str) and v.strip():
            return v.strip()
        links = ac.get("links")
        if isinstance(links, dict) and isinstance(links.get("self"), str):
            return links["self"]
    return ""


def _ontology_url(ann: Dict[str, Any]) -> str:
    v = ann.get("ontology")
    if isinstance(v, str):
        return v
    ac = ann.get("annotatedClass")
    if isinstance(ac, dict):
        links = ac.get("links")
        if isinstance(links, dict) and isinstance(links.get("ontology"), str):
            return links["ontology"]
    return ""


def _annotated_class_self(ann: Dict[str, Any]) -> str:
    ac = ann.get("annotatedClass")
    if isinstance(ac, dict):
        links = ac.get("links")
        if isinstance(links, dict) and isinstance(links.get("self"), str):
            return links["self"]
    return ""


def _offsets(ann: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    anns = ann.get("annotations")
    if isinstance(anns, list) and anns:
        first = anns[0]
        if isinstance(first, dict) and isinstance(first.get("from"), int) and isinstance(first.get("to"), int):
            return first["from"], first["to"]
    return None


def _match_text(ann: Dict[str, Any]) -> str:
    anns = ann.get("annotations")
    if isinstance(anns, list) and anns:
        first = anns[0]
        if isinstance(first, dict) and isinstance(first.get("text"), str):
            return first["text"]
    return ""


def merge_annotations(combined: List[Dict[str, Any]]) -> Dict[str, Any]:
    seen: set[Tuple[str, str, str, int, int]] = set()
    merged: List[Dict[str, Any]] = []
    raw = 0

    for item in combined:
        chunk = item.get("chunk") or {}
        resp = item.get("response")
        if not isinstance(chunk, dict):
            continue
        start = chunk.get("start")
        if not isinstance(start, int):
            continue

        for ann in _iter_annotations(resp):
            raw += 1
            off = _offsets(ann)
            if not off:
                continue
            frm, to = off
            g_from, g_to = frm + start, to + start

            ont = _ontology_url(ann)
            cid = _concept_id(ann)
            mt = _match_text(ann)

            key = (ont, cid, mt, g_from, g_to)
            if key in seen:
                continue
            seen.add(key)

            a2 = dict(ann)
            a2["_global_from"] = g_from
            a2["_global_to"] = g_to
            a2["_chunk_index"] = chunk.get("index")
            merged.append(a2)

    return {"annotations": merged, "counts": {"raw": raw, "merged": len(merged)}}


def build_concepts_summary(merged_payload: Dict[str, Any]) -> Dict[str, Any]:
    anns = merged_payload.get("annotations")
    if not isinstance(anns, list):
        anns = []

    bucket: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for ann in anns:
        if not isinstance(ann, dict):
            continue
        ont = _ontology_url(ann)
        cid = _concept_id(ann)
        if not cid:
            continue
        key = (ont, cid)

        if key not in bucket:
            bucket[key] = {
                "ontology": ont,
                "concept_id": cid,
                "pref_label": "",
                "count": 0,
                "example_matches": [],
                "_self": _annotated_class_self(ann),
            }

        occ = ann.get("annotations")
        occ_count = len(occ) if isinstance(occ, list) and occ else 1
        bucket[key]["count"] += occ_count

        mt = _match_text(ann)
        if mt:
            ex = bucket[key]["example_matches"]
            if mt not in ex:
                ex.append(mt)
                if len(ex) > 10:
                    del ex[10:]

    concepts = list(bucket.values())
    concepts.sort(key=lambda d: (-int(d.get("count", 0)), str(d.get("concept_id", ""))))

    return {"num_unique_concepts": len(concepts), "concepts": concepts}


def concepts_to_csv_rows(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    concepts = summary.get("concepts")
    if not isinstance(concepts, list):
        return []
    rows: List[Dict[str, Any]] = []
    for c in concepts:
        if not isinstance(c, dict):
            continue
        rows.append(
            {
                "ontology": c.get("ontology", ""),
                "concept_id": c.get("concept_id", ""),
                "pref_label": c.get("pref_label", ""),
                "count": c.get("count", 0),
                "example_matches": "|".join(c.get("example_matches", []) or []),
            }
        )
    return rows


def _discover_pontusx_input() -> Path:
    """
    Pontus-X downloads inputs under /data/inputs/<did-hash>/<fileIndex>.
    In your logs, dataset is at /data/inputs/<...>/0.

    We pick the first file under /data/inputs excluding algoCustomData.json.
    If multiple candidates exist, we error to avoid choosing wrong.
    """
    root = Path("/data/inputs")
    if not root.exists():
        raise InputError("No CLI input provided and /data/inputs not found (Pontus-X input staging missing).")

    candidates: List[Path] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.name == "algoCustomData.json":
            continue
        candidates.append(p)

    if not candidates:
        raise InputError("No input files found under /data/inputs (only algoCustomData.json present).")

    # Prefer exact .../<hash>/0 pattern
    preferred = [c for c in candidates if c.name == "0"]
    if len(preferred) == 1:
        return preferred[0]

    # If only one candidate overall, use it
    if len(candidates) == 1:
        return candidates[0]

    # Otherwise ambiguous
    msg = "Multiple input files found under /data/inputs; cannot choose automatically:\n" + "\n".join(
        f"- {c}" for c in sorted(candidates)
    )
    raise InputError(msg)


def _default_out_dir() -> str:
    # Pontus-X typically collects results from /data/outputs
    p = Path("/data/outputs")
    return str(p) if p.exists() else "agroportal_output"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="algorithm")
    parser.add_argument("input", nargs="?", default=None, help="Input file path or '-' for stdin. Optional in Pontus-X.")

    parser.add_argument("--out", default=_default_out_dir())
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--api-key", default=DEFAULT_API_KEY)
    parser.add_argument("--encoding", default="utf-8")

    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--overlap", type=int, default=DEFAULT_OVERLAP)

    parser.add_argument("--ontologies", nargs="*", default=[])
    parser.add_argument("--no-prompt-ontology", action="store_true", default=False)

    parser.add_argument("--semantic-types", nargs="*", default=[])
    parser.add_argument("--whole-word-only", type=parse_bool, default="true")
    parser.add_argument("--exclude-numbers", type=parse_bool, default="false")
    parser.add_argument("--exclude-synonyms", type=parse_bool, default="false")
    parser.add_argument("--longest-only", type=parse_bool, default="false")
    parser.add_argument("--expand-mappings", type=parse_bool, default="false")
    parser.add_argument("--expand-class-hierarchy", type=parse_bool, default="false")
    parser.add_argument("--class-hierarchy-max-level", type=int, default=0)
    parser.add_argument("--minimum-match-length", type=int, default=None)
    parser.add_argument("--stop-words", nargs="*", default=[])
    parser.add_argument("--format", choices=["json", "xml", "jsonp"], default=None)
    parser.add_argument("--display-context", type=parse_bool, default=None)
    parser.add_argument("--display-links", type=parse_bool, default=None)

    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S)
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)

    parser.add_argument("--resolve-labels", action="store_true", default=True)
    parser.add_argument("--no-resolve-labels", dest="resolve_labels", action="store_false")
    parser.add_argument("--labels-sleep", type=float, default=DEFAULT_LABELS_SLEEP_S)
    parser.add_argument("--label-lang", default=DEFAULT_LABEL_LANG)

    args = parser.parse_args(argv)

    if args.input is None:
        args.input = str(_discover_pontusx_input())
        args.no_prompt_ontology = True
        print(f"AUTO: discovered input file: {args.input}")

    try:

        ensure_dir(Path(args.out))

        raw = load_input_text(args.input, args.encoding)
        text = _normalize_text(raw)
        if not text:
            raise InputError("Empty input after normalization")

        args.ontologies = resolve_ontologies(args)
        chunks = chunk_text(text, args.chunk_size, args.overlap)

        out_dir = Path(args.out)
        annotator_url = args.base_url.rstrip("/") + "/annotator"
        annotator_headers = {"Authorization": f"apikey token={args.api_key}"}
        annotator_params = build_annotator_params(args)

        meta = {
            "script": str(Path(__file__).resolve()),
            "base_url": args.base_url,
            "annotator_url": annotator_url,
            "input": args.input,
            "text_length": len(text),
            "chunk_size": args.chunk_size,
            "overlap": args.overlap,
            "num_chunks": len(chunks),
            "ontologies": args.ontologies,
            "label_lang_requested": args.label_lang,
            "algo_custom_data_loaded": bool(load_algo_custom_data()),
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        write_json(out_dir / "run_metadata.json", meta)

        combined: List[Dict[str, Any]] = []
        with requests.Session() as session:
            for c in chunks:
                resp = request_with_retries(
                    session,
                    "POST",
                    annotator_url,
                    params=annotator_params,
                    data={"text": c.text},
                    headers=annotator_headers,
                    timeout_s=args.timeout,
                    max_retries=args.max_retries,
                )
                write_json(out_dir / f"chunk_{c.index:04d}.json", resp)
                combined.append({"chunk": asdict(c) | {"length": len(c.text)}, "response": resp})
                time.sleep(max(0.0, args.sleep))

        write_json(out_dir / "combined.json", combined)

        merged = merge_annotations(combined)
        write_json(out_dir / "merged_annotations.json", merged)

        summary = build_concepts_summary(merged)
        # (Label resolution intentionally left as-is)

        concepts = summary.get("concepts", [])
        if isinstance(concepts, list):
            for c in concepts:
                if isinstance(c, dict):
                    c.pop("_self", None)

        write_json(out_dir / "concepts_summary.json", summary)
        write_csv(
            out_dir / "concepts_summary.csv",
            concepts_to_csv_rows(summary),
            fieldnames=["ontology", "concept_id", "pref_label", "count", "example_matches"],
        )

        meta.update(
            {
                "num_raw_annotations": merged["counts"]["raw"],
                "num_merged_annotations": merged["counts"]["merged"],
                "num_unique_concepts": summary["num_unique_concepts"],
            }
        )
        write_json(out_dir / "run_metadata.json", meta)

        print(f"OK: ontologies={args.ontologies}; wrote outputs to: {out_dir.resolve()}")
        print(
            "Counts: "
            f"raw_annotations={meta['num_raw_annotations']} "
            f"merged_annotations={meta['num_merged_annotations']} "
            f"unique_concepts={meta['num_unique_concepts']}"
        )
        return 0

    except InputError as e:
        print(f"INPUT ERROR: {e}", file=sys.stderr)
        return 2
    except ApiError as e:
        print(f"API ERROR: {e}", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print("INTERRUPTED", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
