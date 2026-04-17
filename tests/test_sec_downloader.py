import unittest

from data_pipeline.downloaders.sec_downloader import _download_limit_for_range


class SECDownloaderLimitTests(unittest.TestCase):
    def test_10q_limit_expands_for_long_backfills(self) -> None:
        years = list(range(2018, 2026))
        self.assertGreaterEqual(_download_limit_for_range("10-Q", years), 36)

    def test_10k_limit_never_drops_below_default_floor(self) -> None:
        years = [2024]
        self.assertEqual(_download_limit_for_range("10-K", years), 20)


if __name__ == "__main__":
    unittest.main()
