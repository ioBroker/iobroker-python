"""Tests for configuration decryption.

The expected values are not hand-written: they come from ioBroker's own encrypt routine, so a
mistake in the reimplementation shows up as a mismatch rather than as two consistent-but-wrong
sides. See ``tools/gen_crypto_fixtures.mjs`` for how the fixtures were produced.
"""

from __future__ import annotations

import pytest

from iobroker.crypto import AES_PREFIX, decrypt, decrypt_native

# Produced by @iobroker/js-controller-common-db tools.encrypt().
AES_SECRET = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718"
LEGACY_SECRET = "Zgfr56gFe87jJOM"


class TestLegacy:
    def test_matches_iobroker(self) -> None:
        # Fixture from tools.encrypt(LEGACY_SECRET, "hunter2").
        assert decrypt(LEGACY_SECRET, "2PDU") == "hunter2"

    def test_round_trip(self) -> None:
        # The XOR form is symmetric, so encrypting is the same operation.
        plain = "hunter2"
        cipher = decrypt(LEGACY_SECRET, plain)

        assert cipher != plain
        assert decrypt(LEGACY_SECRET, cipher) == plain

    def test_used_when_the_secret_is_not_hex(self) -> None:
        # A value that looks like AES must still go the legacy way when the secret cannot be one,
        # because that is the branch js-controller takes -- picking differently would return
        # rubbish instead of failing.
        value = f"{AES_PREFIX}00:11"
        assert decrypt(LEGACY_SECRET, value) != value

    def test_empty_secret_returns_the_value(self) -> None:
        # A fresh installation before the secret exists; better than dividing by the length of an
        # empty string.
        assert decrypt("", "anything") == "anything"


class TestAes:
    """Fixtures produced by @iobroker/js-controller-common-db tools.encrypt()."""

    CASES = [
        ("hunter2", f"{AES_PREFIX}2ce1d9ebc7492b1fd761e6f7b3d4f22e:62cfc641dea5d70fe979ed032e3e8ed7"),
        (
            "pässw0rd mit Leerzeichen",
            f"{AES_PREFIX}e6d39ab026d9fe0e1cd4f72dc242fd36:"
            "e8d49391f444dab0b76d61a61767ceb3d452802d6ee58f53a2978c91e285e7c7",
        ),
        ("", f"{AES_PREFIX}8c65e096e8fe1c03990135e987a0fc63:79d408e76cc504f98d3b4c58b4edde4e"),
    ]

    @pytest.mark.parametrize("plain,cipher", CASES)
    def test_matches_iobroker(self, plain: str, cipher: str) -> None:
        pytest.importorskip("cryptography")
        # Non-ASCII and the empty string are in here because both exercise the padding: the empty
        # string is a full block of padding, and multi-byte characters make the byte length differ
        # from the character length.
        assert decrypt(AES_SECRET, cipher) == plain

    def test_falls_back_when_the_prefix_is_missing(self) -> None:
        assert decrypt(AES_SECRET, "plain") == decrypt(AES_SECRET, "plain")


class TestDecryptNative:
    def test_only_touches_the_declared_keys(self) -> None:
        native = {"host": "192.168.1.5", "password": "secret", "port": 8080}
        out = decrypt_native(LEGACY_SECRET, native, ["password"])

        assert out["host"] == "192.168.1.5"
        assert out["port"] == 8080
        assert out["password"] != "secret"

    def test_leaves_the_input_alone(self) -> None:
        # The instance object is read elsewhere too; mutating it in place would leak decrypted
        # credentials into anything else holding that dict.
        native = {"password": "secret"}
        decrypt_native(LEGACY_SECRET, native, ["password"])

        assert native["password"] == "secret"

    def test_ignores_non_strings_and_missing_keys(self) -> None:
        # Both happen when an io-package.json lists a key wrongly, which is not worth crashing an
        # adapter over.
        native = {"flag": True, "empty": ""}
        out = decrypt_native(LEGACY_SECRET, native, ["flag", "empty", "absent"])

        assert out == native

    def test_no_declaration_changes_nothing(self) -> None:
        native = {"password": "secret"}
        assert decrypt_native(LEGACY_SECRET, native, None) == native
