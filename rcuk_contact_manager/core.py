from __future__ import annotations

import csv
import io
import re
import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd

try:
    import openpyxl
except Exception:
    openpyxl = None


def decode_contact_bytes(data: bytes) -> str:
    for enc in ("utf-8", "utf-16", "utf-16-le", "latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("latin-1", errors="ignore")


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def strip_trailing_name_numbers(name: str) -> str:
    n = normalize_spaces(name)
    n = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", n)
    n = re.sub(r"([_\- ]+)\d+\s*$", "", n)
    n = re.sub(r"(\D)\d+\s*$", r"\1", n)
    return normalize_spaces(n)


def digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def unique_preserve(seq: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for x in seq:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def split_vcards(text: str) -> List[str]:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    pattern = re.compile(r"BEGIN:VCARD\n.*?\nEND:VCARD\n?", re.IGNORECASE | re.DOTALL)
    cards = pattern.findall(text)
    if cards:
        return [c if c.endswith("\n") else c + "\n" for c in cards]
    parts = re.split(r"(?=BEGIN:VCARD)", text, flags=re.IGNORECASE)
    out: List[str] = []
    for p in parts:
        p = p.strip()
        if p.upper().startswith("BEGIN:VCARD") and "END:VCARD" in p.upper():
            if not p.endswith("\n"):
                p += "\n"
            out.append(p)
    return out


def unfold_vcard_lines(card: str) -> List[str]:
    lines = card.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: List[str] = []
    for ln in lines:
        if not ln:
            continue
        if ln.startswith((" ", "\t")) and out:
            out[-1] += ln[1:]
        else:
            out.append(ln)
    return out


def _decode_vcard_value(key: str, val: str) -> str:
    if "ENCODING=QUOTED-PRINTABLE" in key.upper():
        try:
            import quopri
            return quopri.decodestring(val).decode("utf-8", errors="ignore")
        except Exception:
            return val
    return val


def parse_vcard_fields(card: str) -> Dict[str, object]:
    fn = ""
    tels: List[str] = []
    emails: List[str] = []
    org = ""
    for ln in unfold_vcard_lines(card):
        if ":" not in ln:
            continue
        key, val = ln.split(":", 1)
        val = _decode_vcard_value(key, val).strip()
        key_u = key.upper()
        if key_u.startswith("FN"):
            fn = val
        elif key_u.startswith("TEL"):
            tels.append(val)
        elif key_u.startswith("EMAIL"):
            emails.append(val)
        elif key_u.startswith("ORG") and not org:
            org = val
    return {"fn": fn, "tels": unique_preserve(tels), "emails": unique_preserve(emails), "org": org}


def _vcf_escape(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace(";", r"\;").replace(",", r"\,")


def build_vcard(fn: str, tels: List[str], emails: List[str]) -> str:
    lines = ["BEGIN:VCARD", "VERSION:3.0", f"FN:{_vcf_escape(fn or '')}"]
    for tel in unique_preserve(tels):
        if tel:
            lines.append(f"TEL:{tel}")
    for email in unique_preserve(emails):
        if email:
            lines.append(f"EMAIL:{email}")
    lines.append("END:VCARD")
    return "\n".join(lines) + "\n"


def smart_merge_cards(cards: List[str]) -> str:
    best_fn = ""
    all_tels: List[str] = []
    all_emails: List[str] = []
    for c in cards:
        f = parse_vcard_fields(c)
        fn = str(f.get("fn") or "").strip()
        if fn and len(fn) > len(best_fn):
            best_fn = fn
        all_tels.extend(list(f.get("tels") or []))
        all_emails.extend(list(f.get("emails") or []))
    return build_vcard(best_fn, unique_preserve(all_tels), unique_preserve(all_emails))


def extract_vcards_from_bytes(data: bytes) -> List[str]:
    texts: List[str] = []
    data_no_null = data.replace(b"\x00", b"")
    for b in (data_no_null, data):
        for enc in ("utf-8", "utf-16-le", "utf-16", "latin-1"):
            try:
                texts.append(b.decode(enc, errors="ignore"))
            except Exception:
                continue
    cards: List[str] = []
    seen: Set[str] = set()
    for t in texts:
        for c in split_vcards(t):
            k = c.strip()
            if k and k not in seen:
                seen.add(k)
                cards.append(c)
    return cards


def import_from_vcf_file(path: str) -> List[str]:
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    return split_vcards(text)


def import_from_csv(path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows = [{k or "": (v or "") for k, v in (row or {}).items()} for row in reader]
    headers = [h for h in (reader.fieldnames or [])]
    return rows, headers


def import_from_xlsx(path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    if openpyxl is None:
        raise RuntimeError("XLSX support requires openpyxl")
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h or "").strip() for h in next(rows_iter)]
    out_rows: List[Dict[str, str]] = []
    for r in rows_iter:
        d: Dict[str, str] = {}
        for i, h in enumerate(headers):
            d[h] = "" if i >= len(r) else ("" if r[i] is None else str(r[i]))
        out_rows.append(d)
    return out_rows, headers


def import_from_nbf_or_zip(path: str) -> Tuple[List[str], List[Tuple[str, int]]]:
    data = Path(path).read_bytes()
    per: List[Tuple[str, int]] = []
    cards: List[str] = []
    is_zip = False
    try:
        is_zip = zipfile.is_zipfile(path)
    except Exception:
        is_zip = False
    if is_zip:
        with zipfile.ZipFile(path, "r") as z:
            for name in sorted(z.namelist(), key=str.lower):
                if not name.lower().endswith(".vcf"):
                    continue
                try:
                    content = z.read(name)
                    text = decode_contact_bytes(content)
                    vc = split_vcards(text) or extract_vcards_from_bytes(content)
                    cards.extend(vc)
                    per.append((name, len(vc)))
                except Exception:
                    continue
    else:
        vc = extract_vcards_from_bytes(data)
        cards.extend(vc)
        per.append((Path(path).name, len(vc)))
    return cards, per


def import_from_ib(path: str) -> Tuple[List[str], List[Tuple[str, int]]]:
    data = Path(path).read_bytes()
    per: List[Tuple[str, int]] = []
    cards: List[str] = []
    vc = extract_vcards_from_bytes(data)
    if vc:
        cards.extend(vc)
        per.append((Path(path).name, len(vc)))
        return cards, per
    text = decode_contact_bytes(data)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    made = 0
    for ln in lines:
        if ";" in ln:
            a, b = ln.split(";", 1)
        elif "\t" in ln:
            a, b = ln.split("\t", 1)
        elif "," in ln:
            a, b = ln.split(",", 1)
        else:
            continue
        name = a.strip()
        phone = b.strip()
        if name or phone:
            cards.append(build_vcard(name, [phone] if phone else [], []))
            made += 1
    per.append((Path(path).name, made))
    return cards, per


def group_duplicates(cards: List[str], match_email: bool = True, match_phone: bool = True, match_name: bool = True) -> List[List[int]]:
    by_email: Dict[str, List[int]] = {}
    by_phone: Dict[str, List[int]] = {}
    by_fn: Dict[str, List[int]] = {}
    for i, c in enumerate(cards):
        f = parse_vcard_fields(c)
        emails = [e.lower() for e in (f.get("emails") or []) if e]
        tels = [digits_only(t) for t in (f.get("tels") or []) if t]
        fn = strip_trailing_name_numbers(str(f.get("fn") or "")).strip().lower()
        if match_email and emails:
            by_email.setdefault(emails[0], []).append(i)
        if match_phone and tels:
            by_phone.setdefault(tels[0], []).append(i)
        if match_name and fn:
            by_fn.setdefault(fn, []).append(i)
    groups: List[List[int]] = []
    used: Set[int] = set()
    def add_groups(d: Dict[str, List[int]]):
        nonlocal groups, used
        for idxs in d.values():
            if len(idxs) <= 1:
                continue
            g = [x for x in idxs if x not in used]
            if len(g) > 1:
                groups.append(g)
                used.update(g)
    if match_email:
        add_groups(by_email)
    if match_phone:
        add_groups(by_phone)
    if match_name:
        add_groups(by_fn)
    return groups


# -------- Streamlit-facing wrappers --------

def _save_upload_to_temp(uploaded_file) -> Tuple[str, str]:
    suffix = Path(uploaded_file.name).suffix or ""
    with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded_file.getbuffer())
        return tmp.name, suffix.lower()


def parse_vcf(uploaded_file) -> List[str]:
    path, _ = _save_upload_to_temp(uploaded_file)
    return import_from_vcf_file(path)


def load_spreadsheet(uploaded_file) -> pd.DataFrame:
    path, suffix = _save_upload_to_temp(uploaded_file)
    if suffix == ".csv":
        rows, _ = import_from_csv(path)
    else:
        rows, _ = import_from_xlsx(path)
    return pd.DataFrame(rows)


def df_to_contacts(df: pd.DataFrame, name_col: str, phone_col: str, email_col: str) -> List[str]:
    cards: List[str] = []
    for _, row in df.iterrows():
        name = str(row.get(name_col, "") or "").strip()
        phone = str(row.get(phone_col, "") or "").strip()
        email = str(row.get(email_col, "") or "").strip()
        if not (name or phone or email):
            continue
        cards.append(build_vcard(name, [phone] if phone else [], [email] if email else []))
    return cards


def parse_file(uploaded_file) -> List[str]:
    path, suffix = _save_upload_to_temp(uploaded_file)
    if suffix == ".vcf":
        return import_from_vcf_file(path)
    if suffix == ".csv":
        rows, headers = import_from_csv(path)
        if not rows:
            return []
        # heuristic map
        cols = {h.lower(): h for h in headers}
        name_col = next((cols[k] for k in cols if "name" in k), headers[0])
        phone_col = next((cols[k] for k in cols if any(x in k for x in ["phone", "mobile", "tel"])), headers[min(1, len(headers)-1)])
        email_col = next((cols[k] for k in cols if "mail" in k), headers[min(2, len(headers)-1)] if headers else "")
        return df_to_contacts(pd.DataFrame(rows), name_col, phone_col, email_col)
    if suffix in {".xlsx", ".xlsm"}:
        rows, headers = import_from_xlsx(path)
        if not rows:
            return []
        cols = {h.lower(): h for h in headers}
        name_col = next((cols[k] for k in cols if "name" in k), headers[0])
        phone_col = next((cols[k] for k in cols if any(x in k for x in ["phone", "mobile", "tel"])), headers[min(1, len(headers)-1)])
        email_col = next((cols[k] for k in cols if "mail" in k), headers[min(2, len(headers)-1)] if headers else "")
        return df_to_contacts(pd.DataFrame(rows), name_col, phone_col, email_col)
    if suffix in {".nbf", ".nbu", ".zip", ".nbackup"}:
        cards, _ = import_from_nbf_or_zip(path)
        return cards
    if suffix == ".ib":
        cards, _ = import_from_ib(path)
        return cards
    # final fallback: look for embedded vcards
    data = Path(path).read_bytes()
    return extract_vcards_from_bytes(data)


def find_duplicates(cards: List[str], match_email: bool = True, match_phone: bool = True, match_name: bool = True) -> List[List[str]]:
    groups_idx = group_duplicates(cards, match_email=match_email, match_phone=match_phone, match_name=match_name)
    return [[cards[i] for i in grp] for grp in groups_idx]


def merge_contacts(cards: List[str]) -> str:
    return smart_merge_cards(cards)


def export_vcf(cards: List[str]) -> bytes:
    return "".join(cards).encode("utf-8")
