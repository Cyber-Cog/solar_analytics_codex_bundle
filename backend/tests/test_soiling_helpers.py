import unittest

from soiling_helpers import (
    linreg_slope_per_step,
    median_consecutive_delta,
    moving_median,
    soiling_loss_kwh_from_pr_steps,
)


class TestSoilingHelpers(unittest.TestCase):
    def test_moving_median(self):
        self.assertEqual(moving_median([10.0, 20.0, 30.0], 3), [15.0, 20.0, 25.0])

    def test_linreg_slope(self):
        y = [0.0, 1.0, 2.0, 3.0]
        s = linreg_slope_per_step(y)
        self.assertIsNotNone(s)
        self.assertAlmostEqual(s, 1.0, places=5)

    def test_median_delta(self):
        self.assertAlmostEqual(median_consecutive_delta([100.0, 98.0, 97.0]), -1.5)

    def test_soiling_loss_kwh(self):
        pr = [100.0, 99.0, 97.0]
        e_ref = [1000.0, 1000.0, 1000.0]
        loss = soiling_loss_kwh_from_pr_steps(pr, e_ref)
        self.assertAlmostEqual(loss, 30.0, places=5)


if __name__ == "__main__":
    unittest.main()
