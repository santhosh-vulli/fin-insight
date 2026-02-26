from datetime import datetime
from decimal import Decimal
from collections import defaultdict
from statistics import mean, stdev
from typing import List, Dict, Any
import math


class AdvancedFPAEngine:

    def __init__(self):
        pass

    # ─────────────────────────────────────────────
    # 1️⃣ Multi-Dimensional Aggregation
    # ─────────────────────────────────────────────

    def aggregate(
        self,
        invoices: List[Dict[str, Any]],
        group_by: List[str],
    ) -> Dict[str, Decimal]:

        result = defaultdict(Decimal)

        for inv in invoices:
            key_parts = []

            for field in group_by:
                if field == "month":
                    date = datetime.fromisoformat(inv["invoice_date"])
                    key_parts.append(date.strftime("%Y-%m"))
                else:
                    key_parts.append(str(inv.get(field)))

            key = "|".join(key_parts)
            result[key] += Decimal(str(inv["amount"]))

        return dict(result)

    # ─────────────────────────────────────────────
    # 2️⃣ Rolling Average (3 or 6 month)
    # ─────────────────────────────────────────────

    def rolling_average(self, values: List[Decimal], window: int = 3) -> List[Decimal]:
        if len(values) < window:
            return []

        averages = []
        for i in range(len(values) - window + 1):
            subset = values[i:i + window]
            averages.append(sum(subset) / Decimal(window))

        return averages

    # ─────────────────────────────────────────────
    # 3️⃣ Statistical Anomaly Detection (Z-score)
    # ─────────────────────────────────────────────

    def z_score_anomaly(self, values: List[Decimal]) -> Dict[str, Any]:
        if len(values) < 2:
            return {"anomaly": False}

        values_float = [float(v) for v in values]
        avg = mean(values_float)

        if len(values_float) > 1:
            std_dev = stdev(values_float)
        else:
            std_dev = 0

        if std_dev == 0:
            return {"anomaly": False}

        latest = values_float[-1]
        z_score = (latest - avg) / std_dev

        return {
            "latest_value": latest,
            "mean": avg,
            "std_dev": std_dev,
            "z_score": round(z_score, 2),
            "anomaly": abs(z_score) > 2
        }

    # ─────────────────────────────────────────────
    # 4️⃣ Vendor Concentration Risk
    # ─────────────────────────────────────────────

    def vendor_concentration(self, invoices: List[Dict[str, Any]]) -> Dict[str, Any]:
        vendor_totals = defaultdict(Decimal)
        total_spend = Decimal("0")

        for inv in invoices:
            vendor = inv["vendor_id"]
            amount = Decimal(str(inv["amount"]))
            vendor_totals[vendor] += amount
            total_spend += amount

        if total_spend == 0:
            return {"concentration_percent": 0}

        largest_vendor = max(vendor_totals.values())
        concentration = (largest_vendor / total_spend) * 100

        return {
            "largest_vendor_share_percent": round(concentration, 2),
            "risk": "high" if concentration > 40 else "medium" if concentration > 25 else "low"
        }

    # ─────────────────────────────────────────────
    # 5️⃣ Budget Burn Velocity
    # ─────────────────────────────────────────────

    def burn_velocity(
        self,
        spent: Decimal,
        allocated: Decimal,
        days_passed: int,
        days_in_month: int
    ) -> Dict[str, Any]:

        if allocated == 0 or days_passed == 0:
            return {"risk": "unknown"}

        daily_burn = spent / Decimal(days_passed)
        projected = daily_burn * Decimal(days_in_month)

        overrun = projected > allocated

        return {
            "daily_burn": round(daily_burn, 2),
            "projected_month_end": round(projected, 2),
            "overrun_risk": overrun
        }

    # ─────────────────────────────────────────────
    # 6️⃣ Weighted Forecast (Trend Projection)
    # ─────────────────────────────────────────────

    def weighted_forecast(self, values: List[Decimal]) -> Dict[str, Any]:
        if len(values) < 2:
            return {"forecast": None}

        weights = list(range(1, len(values) + 1))
        weighted_sum = sum(v * w for v, w in zip(values, weights))
        total_weight = sum(weights)

        forecast = weighted_sum / Decimal(total_weight)

        return {
            "weighted_forecast": round(forecast, 2)
        }

    # ─────────────────────────────────────────────
    # 7️⃣ Composite Financial Risk Index
    # ─────────────────────────────────────────────

    def financial_risk_index(
        self,
        mom_anomaly: bool,
        vendor_concentration_risk: str,
        burn_overrun: bool,
        sla_escalations: int,
        rule_violations: int
    ) -> Dict[str, Any]:

        score = 0

        if mom_anomaly:
            score += 25

        if vendor_concentration_risk == "high":
            score += 20
        elif vendor_concentration_risk == "medium":
            score += 10

        if burn_overrun:
            score += 20

        score += min(sla_escalations * 3, 15)
        score += min(rule_violations * 2, 20)

        if score > 70:
            level = "high"
        elif score > 40:
            level = "medium"
        else:
            level = "low"

        return {
            "financial_risk_score": score,
            "financial_risk_level": level
        }