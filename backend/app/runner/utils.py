import hashlib
import importlib
import json
import re
from pathlib import Path
from typing import Any, BinaryIO, Dict, Optional


def sha256(data: bytes) -> str:
	return hashlib.sha256(data).hexdigest()


def calculate_file_hash(file_obj: BinaryIO) -> str:
	pos = None
	try:
		pos = file_obj.tell()
	except Exception:
		pos = None
	content = file_obj.read()
	if not isinstance(content, (bytes, bytearray)):
		content = bytes(content)
	result = sha256(bytes(content))
	if pos is not None:
		try:
			file_obj.seek(pos)
		except Exception:
			pass
	return result


def validate_file_type(filename: str) -> str:
	name = (filename or "").lower()
	if name.endswith(".csv"):
		return "csv"
	if name.endswith(".parquet"):
		return "parquet"
	if name.endswith(".json"):
		return "json"
	if name.endswith(".xlsx") or name.endswith(".xls"):
		return "excel"
	if name.endswith(".txt"):
		return "txt"
	if name.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg")):
		return "image"
	return "unknown"


def HUMANIZED_TIME_UNITS() -> Dict[str, str]:
	return {"ms": "ms", "s": "s", "m": "m"}


def format_execution_time(duration_ms: float) -> str:
	units = HUMANIZED_TIME_UNITS()
	value = max(0.0, float(duration_ms))
	if value < 1000:
		return f"{value:.1f}{units['ms']}"
	seconds = value / 1000.0
	if seconds < 60:
		return f"{seconds:.2f}{units['s']}"
	minutes = int(seconds // 60)
	remaining = int(seconds % 60)
	return f"{minutes}{units['m']} {remaining}{units['s']}"


def sanitize_filename(filename: str) -> str:
	if filename == "":
		return ""
	if filename.startswith(".") and filename.count(".") == 1:
		return filename
	cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f@#$%]+', "_", filename)
	cleaned = cleaned.strip()
	if len(cleaned) <= 255:
		return cleaned
	path = Path(cleaned)
	stem = path.stem[: max(1, 255 - len(path.suffix))]
	return f"{stem}{path.suffix}"


def import_module(module_name: str) -> Optional[Any]:
	try:
		return importlib.import_module(module_name)
	except Exception:
		return None


def load_config(path: str) -> Dict[str, Any]:
	with open(path, "r", encoding="utf-8") as f:
		return json.load(f)
