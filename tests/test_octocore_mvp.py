import tempfile
import unittest
from fractions import Fraction
from pathlib import Path

from octocore_mvp import (
    MintEvent,
    MonitorStore,
    candidate_probability,
    format_mint_alert,
    format_start_message,
    probability_within,
    should_send_mint_alert,
    state_for_next_mint,
    window_for_token,
)


class WindowTests(unittest.TestCase):
    def test_regular_window_bounds_and_position(self) -> None:
        window = window_for_token(3211)

        self.assertEqual((window.id, window.start, window.end, window.size), (5, 2777, 3470, 694))
        self.assertEqual(window.position(3211), 435)

    def test_last_window_keeps_collection_remainder(self) -> None:
        window = window_for_token(11111)

        self.assertEqual((window.id, window.start, window.end, window.size), (16, 10411, 11111, 701))
        self.assertEqual(window.position(11111), 701)

    def test_next_mint_state_moves_to_new_window_after_boundary(self) -> None:
        state = state_for_next_mint(latest_minted=3470, current_window_has_unique=True)

        self.assertEqual((state.window.id, state.next_token_id, state.position, state.remaining), (6, 3471, 0, 694))
        self.assertEqual(state.probability_next, candidate_probability(694))


class ProbabilityTests(unittest.TestCase):
    def test_contract_derived_probability_uses_modulo_preimage_count(self) -> None:
        modulus = 80
        expected = Fraction((2**256 + modulus - 1) // modulus, 2**256)

        self.assertEqual(candidate_probability(modulus), expected)
        self.assertEqual(candidate_probability(1), Fraction(1, 1))

    def test_probability_is_zero_after_unique_is_minted(self) -> None:
        state = state_for_next_mint(latest_minted=3211, current_window_has_unique=True)

        self.assertEqual(state.probability_next, Fraction(0, 1))
        self.assertEqual(probability_within(remaining=100, mint_count=20, unique_already_minted=True), Fraction(0, 1))

    def test_probability_within_is_the_complement_of_each_contract_trial(self) -> None:
        expected = Fraction(1, 1) - (Fraction(1, 1) - candidate_probability(3)) * (
            Fraction(1, 1) - candidate_probability(2)
        )

        self.assertEqual(probability_within(remaining=3, mint_count=2), expected)
        self.assertEqual(probability_within(remaining=3, mint_count=3), Fraction(1, 1))


class StoreTests(unittest.TestCase):
    def test_duplicate_mint_is_ignored_and_special_is_queryable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MonitorStore(Path(directory) / "monitor.sqlite3")
            store.migrate()
            mint = MintEvent(
                token_id=555,
                tx_hash="0xabc",
                block_number=56516291,
                block_hash="0xblock",
                timestamp=1790014702,
                minter="0x93803fc8eb644c0ee414c1f8c5521d46aeb766dd",
                price_wei=10**16,
                seed="0xseed",
                work="0xwork",
                target="0xtarget",
                nonce="0xnonce",
                unique_index=12,
            )

            self.assertTrue(store.insert_mint(mint))
            self.assertFalse(store.insert_mint(mint))
            self.assertEqual(store.mint_count(), 1)
            last = store.last_unique()
            self.assertIsNotNone(last)
            self.assertEqual((last.token_id, last.unique_index, last.window_id, last.position), (555, 12, 1, 555))

    def test_regular_mint_alert_keeps_the_window_unique_type(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MonitorStore(Path(directory) / "monitor.sqlite3")
            store.migrate()
            special = MintEvent(555, "0x555", 1, "0xblock", 1, "0xminter", 10**16, "0xseed", "0xwork", "0xtarget", "0xnonce", 12)
            regular = MintEvent(600, "0x600", 2, "0xblock2", 2, "0xminter2", 10**16, "0xseed2", "0xwork2", "0xtarget2", "0xnonce2", 255)
            store.insert_mint(special)
            store.insert_mint(regular)

            alert = format_mint_alert(regular, store)

            self.assertIn("Current 1/1: MINTED — Ninja", alert)
            self.assertIn("Mints since last 1/1: 45", alert)
            self.assertIn('href="https://debank.com/profile/0xminter2"', alert)
            self.assertIn('href="https://explorer.inkonchain.com/tx/0x600"', alert)
            self.assertIn("Chance that the next valid mint is a 1/1: 0.00%", alert)
            self.assertIn("follow me: https://x.com/IntelPocik", alert)

    def test_alerts_are_suppressed_after_the_window_unique_is_minted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MonitorStore(Path(directory) / "monitor.sqlite3")
            store.migrate()
            special = MintEvent(555, "0x555", 1, "0xblock", 1, "0xminter", 10**16, "0xseed", "0xwork", "0xtarget", "0xnonce", 12)
            store.insert_mint(special)

            self.assertFalse(should_send_mint_alert(store))
            start = format_start_message(store)
            self.assertIn("Current 1/1: MINTED — Ninja", start)
            self.assertIn("Current chance to mint a 1/1: 0.00%", start)

    def test_start_message_says_alerts_resume_in_next_window_after_unique(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MonitorStore(Path(directory) / "monitor.sqlite3")
            store.migrate()
            store.insert_mint(MintEvent(555, "0x555", 1, "0xblock", 1, "0xminter", 10**16, "0xseed", "0xwork", "0xtarget", "0xnonce", 12))

            self.assertIn("I will notify you when the next window begins", format_start_message(store))


if __name__ == "__main__":
    unittest.main()
