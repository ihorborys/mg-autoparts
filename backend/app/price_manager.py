from pathlib import Path
from typing import Dict, List, Any
import yaml

from .price_processor import process_one_price
from .exchange import get_eur_to_uah


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_supplier_id(supplier: str) -> int | None:
    """
    Читає config/suppliers.yaml і повертає supplier_id для імені постачальника.
    Очікується структура:
      AP_GDANSK:
        supplier_id: 2
        ...
    """
    cfg = _load_yaml(Path("config/suppliers.yaml"))
    node = cfg.get(supplier) or cfg.get(supplier.upper())
    if not node:
        return None
    return int(node.get("supplier_id")) if node.get("supplier_id") is not None else None


def process_all_prices(supplier: str, remote_gz_path: str) -> List[Dict[str, Any]]:
    """
    Пройти всі профілі з config/profiles.yaml для заданого постачальника.
    """
    profiles_cfg = _load_yaml(Path("config/profiles.yaml"))
    profiles = profiles_cfg.get("profiles", [])
    common = profiles_cfg.get("common", {})
    rounding = (common.get("rounding") or {"EUR": 2, "UAH": 0})

    supplier_id = _get_supplier_id(supplier)

    results: List[Dict[str, Any]] = []
    for profile in profiles:
        name = profile["name"]
        factor = float(profile["factor"])
        currency_out = profile["currency_out"]
        format_ = profile["format"]  # xlsx | csv
        r2_prefix = profile["r2_prefix"].format(supplier=supplier.lower())
        columns = profile.get("columns") or []
        csv_cfg = profile.get("csv") or {}

        # курс лише для UAH-профілів (Exist)
        rate = 1.0
        if currency_out == "UAH":
            rp = profile.get("rate_params") or {}
            # підтримуємо обидва формати: { add_uah, min_rate, fallback } або fallback: {policy:..., value:...}
            fallback = rp["fallback"]["value"] if isinstance(rp.get("fallback"), dict) else (rp.get("fallback") or 50)
            rate = get_eur_to_uah(
                add_uah=rp.get("add_uah", 1),
                min_rate=rp.get("min_rate", 49),
                fallback=fallback,
            )

        print(f"➡️  {name}: factor={factor}, out={currency_out}, fmt={format_}, r2={r2_prefix}")

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
        )

        results.append({
            "name": name,
            "factor": factor,
            "currency": currency_out,
            "key": key,
            "url": url,
        })

    return results
