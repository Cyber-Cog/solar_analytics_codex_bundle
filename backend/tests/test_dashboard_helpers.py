"""Unit tests for dashboard_helpers (no database; stdlib unittest only)."""
import unittest
from datetime import date

from dashboard_helpers import (
    WMS_INSOLATION_SUM_DIVISOR,
    gti_insolation_kwh_m2_from_sums,
    resolve_dashboard_date_range,
)


class TestDashboardHelpers(unittest.TestCase):
    def test_resolve_single_side_fills_other(self):
        self.assertEqual(resolve_dashboard_date_range("2026-03-02", ""), ("2026-03-02", "2026-03-02"))
        self.assertEqual(resolve_dashboard_date_range("", "2026-03-02"), ("2026-03-02", "2026-03-02"))
        self.assertEqual(resolve_dashboard_date_range(None, "2026-03-02"), ("2026-03-02", "2026-03-02"))

    def test_resolve_both_set(self):
        self.assertEqual(
            resolve_dashboard_date_range("2026-03-01", "2026-03-03"),
            ("2026-03-01", "2026-03-03"),
        )

    def test_resolve_strips_and_truncates(self):
        self.assertEqual(
            resolve_dashboard_date_range("  2026-03-02T00:00:00  ", "  2026-03-02  "),
            ("2026-03-02", "2026-03-02"),
        )

    def test_resolve_empty_defaults_to_last_7_days(self):
        a, b = resolve_dashboard_date_range("", "")
        da = date.fromisoformat(a)
        db = date.fromisoformat(b)
        self.assertEqual((db - da).days, 7)

    def test_gti_insolation_uses_divisor(self):
        self.assertAlmostEqual(gti_insolation_kwh_m2_from_sums(60000.0, 0.0), 1.0)

    def test_gti_insolation_falls_back_to_irradiance(self):
        self.assertAlmostEqual(
            gti_insolation_kwh_m2_from_sums(0.0, WMS_INSOLATION_SUM_DIVISOR),
            1.0,
        )


if __name__ == "__main__":
    unittest.main()
