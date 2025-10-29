from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml

from .paths import CONFIG_DIR
from .price_processor import process_one_price
from .exchange import get_eur_to_uah


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_supplier_id(supplier: str) -> Optional[int]:
    """
    Читає config/suppliers.yaml і повертає supplier_id для імені постачальника.
    Очікується структура:
      AP_GDANSK:
        supplier_id: 2
        ...
    """
    cfg = _load_yaml(CONFIG_DIR / "suppliers.yaml")
    node = cfg.get(supplier) or cfg.get(supplier.upper()) or cfg.get(supplier.lower())
    if not node:
        return None
    return int(node["supplier_id"]) if "supplier_id" in node and node["supplier_id"] is not None else None


def process_all_prices(
        supplier: str,
        remote_gz_path: str,
        *,
        delete_input_after: bool = False,  # зручно для Gmail-потоку
) -> List[Dict[str, Any]]:
    """
    Пройти всі профілі з config/profiles.yaml для заданого постачальника.
    """
    profiles_cfg = _load_yaml(CONFIG_DIR / "profiles.yaml")
    profiles = profiles_cfg.get("profiles", [])
    common = profiles_cfg.get("common", {})
    rounding = (common.get("rounding") or {"EUR": 2, "UAH": 0})

    supplier_id = _get_supplier_id(supplier)

    results: List[Dict[str, Any]] = []
    for profile in profiles:
        name = profile["name"]
        factor = float(profile["factor"])
        currency_out = str(profile["currency_out"]).upper()  # EUR | UAH
        format_ = profile["format"]  # xlsx | csv

        # r2_prefix може мати плейсхолдер {supplier}
        r2_prefix = (profile.get("r2_prefix") or "").format(supplier=supplier.lower())
        if r2_prefix and not r2_prefix.endswith("/"):
            r2_prefix += "/"

        columns = profile.get("columns") or []
        csv_cfg = profile.get("csv") or {}

        # курс лише для UAH-профілів
        rate = 1.0
        if currency_out == "UAH":
            rp = profile.get("rate_params") or {}
            # Підтримуємо обидва варіанти fallback
            fb = rp.get("fallback")
            fallback_value = fb.get("value") if isinstance(fb, dict) else (fb or 50)
            rate = get_eur_to_uah(
                add_uah=rp.get("add_uah", 1),
                min_rate=rp.get("min_rate", 49),
                fallback=fallback_value,
            )

        print(f"➡️  {name}: factor={factor}, out={currency_out}, fmt={format_}, r2={r2_prefix}")

        # Викликаємо пайплайн
        key, url = process_one_price(
            remote_gz_path=remote_gz_path,
            supplier=supplier,
            supplier_id=supplier_id,
            factor=factor,
            currency_out=currency_out,
            format_=format_,
            rounding=rounding,
            r2_prefix=r2_prefix,
            columns=columns,
            csv_cfg=csv_cfg,
            rate=rate,
            delete_input_after=delete_input_after,  # ← прокинули
        )

        results.append({
            "name": name,
            "factor": factor,
            "currency": currency_out,
            "key": key,
            "url": url,
        })

    return results
