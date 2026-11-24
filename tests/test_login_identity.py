"""L-3a: Telegram's id_token verified, the person built, the bot's right
counted (app/login/identity.py) -- offline, on keys of the test's own.

Held: a good token of an RSA key (RS256) and of an EC key (ES256), each
with key set entries of both forms -- with `alg` and `use`, and with the
key's own fields only -- gives the person; every reason of a refusal its
own test and its own word; the `id` claim by the contract's rule; the
fields by its table; the bot's right by RFC 6749 §5.1, the live sign-in's
cell included; the moment of the check the one given; nothing of a token
or a claim in a repr or a log line.

THE LIVE SIGN-IN of 24.11 (the owner's test bot, the dispatcher's
script): alg RS256, kid `oidc-1` among 4 keys, `aud` a STRING equal to
the bot's id, `id` a STRING and `sub` not `id`, the exchange's `scope`
'openid profile' -- its shape is TestTheLiveShape.

Mutations M1-M13 (the gate's list) -- each red here.
"""

import base64
import hashlib
import hmac
import json
import math
import random
from datetime import UTC, datetime
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt.algorithms import ECAlgorithm, RSAAlgorithm
from structlog.testing import capture_logs

from app.core.config import TELEGRAM_LOGIN_ISSUER
from app.login.identity import (
    CLOCK_LEEWAY,
    IdTokenRejected,
    TelegramIdentity,
    bot_access_granted,
    identity_from_claims,
    telegram_id,
    verify_identity,
)

R = IdTokenRejected
_CLIENT = "7702864529"
_NOW = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)

# The test's own keys, made once.
_RSA = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_RSA_OTHER = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_EC = ec.generate_private_key(ec.SECP256R1())


def _jwk(private: Any, kid: str, *, alg: str | None) -> dict[str, Any]:
    """A key set entry: with `alg` and `use` when `alg` is given, else the
    key's own fields only (kty, kid, n/e or crv/x/y)."""
    if isinstance(private, rsa.RSAPrivateKey):
        public = json.loads(RSAAlgorithm.to_jwk(private.public_key()))
        entry = {"kty": "RSA", "kid": kid, "n": public["n"], "e": public["e"]}
    else:
        public = json.loads(ECAlgorithm.to_jwk(private.public_key()))
        entry = {"kty": "EC", "kid": kid, "crv": public["crv"],
                 "x": public["x"], "y": public["y"]}
    if alg is not None:
        entry.update(alg=alg, use="sig")
    return entry


def _jwks(*, full: bool = True) -> dict[str, Any]:
    return {"keys": [
        _jwk(_RSA, "k-rsa", alg="RS256" if full else None),
        _jwk(_EC, "k-ec", alg="ES256" if full else None),
    ]}


def _claims(**changes: Any) -> dict[str, Any]:
    """A good token's claims, the live shape: `id` a string, `sub` not `id`."""
    stamp = int(_NOW.timestamp())
    claims: dict[str, Any] = {
        "iss": TELEGRAM_LOGIN_ISSUER,
        "aud": _CLIENT,
        "exp": stamp + 300,
        "iat": stamp - 10,
        "sub": "unit-sub-not-the-id",
        "id": "123456789",
        "given_name": "Ada",
        "family_name": "Lovelace",
        "name": "Ada Lovelace",
        "preferred_username": "ada",
        "picture": "https://cdn.example.org/ada.jpg",
    }
    for name, value in changes.items():
        if value is _DROP:
            del claims[name]
        else:
            claims[name] = value
    return claims


_DROP = object()


def _token(
    claims: dict[str, Any] | None = None,
    *,
    key: Any = _RSA,
    alg: str = "RS256",
    kid: str | None = "k-rsa",
) -> str:
    headers = {} if kid is None else {"kid": kid}
    return jwt.encode(claims if claims is not None else _claims(), key,
                      algorithm=alg, headers=headers)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _forged(header: dict[str, Any], signature: bytes = b"") -> str:
    """A token signed by hand -- what PyJWT will not make."""
    head = _b64(json.dumps(header).encode())
    body = _b64(json.dumps(_claims()).encode())
    return f"{head}.{body}.{_b64(signature)}"


def _verify(token: object, jwks: object = None, *, now: datetime = _NOW,
            granted: bool = False) -> TelegramIdentity | IdTokenRejected:
    return verify_identity(
        token, _jwks() if jwks is None else jwks, client_id=_CLIENT, now=now,
        bot_access_granted=granted,
    )


# -----------------------------------------------------------------------------
# Item 2 -- the id_token
# -----------------------------------------------------------------------------


class TestTheGoodToken:
    @pytest.mark.parametrize("full", [True, False], ids=["alg-use", "bare"])
    @pytest.mark.parametrize(("key", "alg", "kid"), [
        (_RSA, "RS256", "k-rsa"), (_EC, "ES256", "k-ec"),
    ], ids=["RS256", "ES256"])
    def test_gives_the_person(self, key: Any, alg: str, kid: str, full: bool) -> None:
        identity = _verify(_token(key=key, alg=alg, kid=kid), _jwks(full=full),
                           granted=True)
        assert isinstance(identity, TelegramIdentity), identity
        assert identity.telegram_id == 123456789
        assert identity.subject == "123456789"
        assert identity.provider == "telegram"
        assert identity.verified_at == _NOW
        assert identity.bot_access_granted is True

    def test_the_moment_is_the_one_given(self) -> None:
        """M11: a token of the year 2000, checked in 2000 -- good; checked
        at the moment given and not by a clock of the module's."""
        then = datetime(2000, 1, 1, tzinfo=UTC)
        stamp = int(then.timestamp())
        token = _token(_claims(exp=stamp + 300, iat=stamp - 10))
        assert isinstance(_verify(token, now=then), TelegramIdentity)
        assert _verify(token) is R.EXPIRED


class TestTheAlgorithm:
    @pytest.mark.parametrize("header", [
        {"alg": "none", "kid": "k-rsa"},
        {"alg": "RS512", "kid": "k-rsa"},
        {"alg": "ES384", "kid": "k-ec"},
        {"alg": "rs256", "kid": "k-rsa"},
        {"kid": "k-rsa"},
        {"alg": ["RS256"], "kid": "k-rsa"},
    ], ids=["none", "RS512", "ES384", "lowercase", "absent", "a-list"])
    def test_another_is_refused(self, header: dict[str, Any]) -> None:
        assert _verify(_forged(header)) is R.ALGORITHM

    def test_hs256_with_the_public_key_as_its_secret(self) -> None:
        """The classic confusion: the published key used as an HMAC
        secret. Refused by the list, before a key is looked at."""
        pem = _RSA.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        header = {"alg": "HS256", "kid": "k-rsa"}
        head = _b64(json.dumps(header).encode())
        body = _b64(json.dumps(_claims()).encode())
        signature = hmac.new(pem, f"{head}.{body}".encode(), hashlib.sha256).digest()
        assert _verify(f"{head}.{body}.{_b64(signature)}") is R.ALGORITHM


class TestTheKey:
    def test_no_kid(self) -> None:
        """Refused even when the set holds one key (velo's sample would
        take it): the contract picks the key by `kid`."""
        one = {"keys": [_jwk(_RSA, "k-rsa", alg="RS256")]}
        assert _verify(_token(kid=None), one) is R.NO_KID
        assert _verify(_token(kid=""), one) is R.NO_KID

    @pytest.mark.parametrize("jwks", [
        {"keys": [_jwk(_EC, "k-ec", alg="ES256")]},
        {"keys": []},
        {"keys": "k-rsa"},
        {},
        [],
        "keys",
        {"keys": [{"kid": "k-rsa", "kty": "RSA"}]},
    ], ids=["not-there", "empty", "keys-not-a-list", "no-keys", "a-list",
            "a-string", "unusable"])
    def test_unknown_key(self, jwks: Any) -> None:
        assert _verify(_token(), jwks) is R.UNKNOWN_KEY

    def test_two_keys_of_one_kid(self) -> None:
        twice = {"keys": [_jwk(_RSA, "k-rsa", alg="RS256"),
                          _jwk(_RSA_OTHER, "k-rsa", alg="RS256")]}
        assert _verify(_token(), twice) is R.AMBIGUOUS_KEY

    def test_signed_by_another_key(self) -> None:
        assert _verify(_token(key=_RSA_OTHER)) is R.SIGNATURE

    def test_a_key_of_the_wrong_type(self) -> None:
        """RS256 named, the EC key's kid: refused for the key's type,
        before any signature is checked.

        Was SIGNATURE (L-3a): PyJWT refused to verify with a key of the
        wrong type, and that was all the check there was. R-3 checks the
        key against the algorithm first (TestTheKeyFitsTheAlgorithm), so
        the word is the key's -- the signature is never looked at."""
        token = _token()
        head = _b64(json.dumps({"alg": "RS256", "kid": "k-ec"}).encode())
        swapped = ".".join([head, *token.split(".")[1:]])
        assert _verify(swapped) is R.KEY_TYPE


def _prime(bits: int) -> int:
    """A random prime of `bits` bits (Miller-Rabin, 32 rounds)."""
    while True:
        candidate = random.getrandbits(bits) | (1 << (bits - 1)) | 1
        if all(candidate % p for p in (3, 5, 7, 11, 13, 17, 19, 23)) and all(
            _passes(candidate, random.randrange(2, candidate - 1))
            for _ in range(32)
        ):
            return candidate


def _passes(n: int, a: int) -> bool:
    d, s = n - 1, 0
    while d % 2 == 0:
        d, s = d // 2, s + 1
    x = pow(a, d, n)
    if x in (1, n - 1):
        return True
    for _ in range(s - 1):
        x = x * x % n
        if x == n - 1:
            return True
    return False


def _rsa_of(bits: int) -> rsa.RSAPrivateKey:
    """An RSA key of `bits` bits -- cryptography makes none under 1024,
    and loads one built from its primes."""
    if bits >= 1024:
        return rsa.generate_private_key(public_exponent=65537, key_size=bits)
    e = 65537
    while True:
        p, q = _prime(bits // 2), _prime(bits // 2)
        if p != q and math.gcd(e, (p - 1) * (q - 1)) == 1:
            break
    d = pow(e, -1, (p - 1) * (q - 1))
    numbers = rsa.RSAPrivateNumbers(
        p, q, d, d % (p - 1), d % (q - 1), pow(q, -1, p),
        rsa.RSAPublicNumbers(e, p * q),
    )
    return numbers.private_key()


_RSA_1024 = _rsa_of(1024)
_RSA_512 = _rsa_of(512)
_EC_384 = ec.generate_private_key(ec.SECP384R1())
_EC_K1 = ec.generate_private_key(ec.SECP256K1())


def _entry(kind: str) -> dict[str, Any]:
    """A key set entry named `k-under-test`, of the kind named."""
    if kind == "rsa-2048":
        entry = _jwk(_RSA, "x", alg=None)
    elif kind == "rsa-1024":
        entry = _jwk(_RSA_1024, "x", alg=None)
    elif kind == "rsa-512":
        entry = _jwk(_RSA_512, "x", alg=None)
    elif kind == "rsa-private":
        entry = json.loads(RSAAlgorithm.to_jwk(_RSA))
    elif kind == "ec-p256":
        entry = _jwk(_EC, "x", alg=None)
    elif kind == "ec-p256-private":
        entry = json.loads(ECAlgorithm.to_jwk(_EC))
    elif kind == "ec-p384":
        entry = _jwk(_EC_384, "x", alg=None)
    elif kind == "ec-secp256k1":
        entry = json.loads(ECAlgorithm.to_jwk(_EC_K1.public_key()))
    elif kind == "oct-pem":
        # The confusion of a hostile key set: an HMAC key whose bytes are
        # the PEM of an RSA public key -- PyJWT reads such bytes as that
        # key for RS256.
        pem = _RSA.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        entry = {"kty": "oct", "k": _b64(pem)}
    else:
        assert kind == "oct"
        entry = {"kty": "oct", "k": _b64(b"unit-hmac-key-" + b"x" * 18)}
    entry["kid"] = "k-under-test"
    return entry


# The key that signs a token of each algorithm when the entry's own key
# cannot (a public entry, an HMAC key): the entry is refused before any
# signature is checked, so what signed it does not matter.
_SIGNER = {"rs256": _RSA, "es256": _EC}
_OWN_SIGNER: dict[str, Any] = {
    "rsa-2048": _RSA, "rsa-1024": _RSA_1024, "rsa-512": _RSA_512,
    "rsa-private": _RSA, "ec-p256": _EC, "ec-p256-private": _EC,
}


class TestTheKeyFitsTheAlgorithm:
    """R-3 item 3: the key named by `kid` must be of the algorithm --
    RS256 an RSA PUBLIC key of at least 2048 bits, ES256 an EC PUBLIC key
    on P-256 -- or the token is refused with the word `key_type`: a key
    set of Telegram's is no reason to verify with a weak, private, HMAC
    or foreign key. Each cell signed by the entry's own key where it can
    sign under the algorithm -- so on the 4.2.0 candidate RSA-512,
    RSA-1024, the private EC key and the PEM in an `oct` gave the person."""

    @pytest.mark.parametrize(("alg", "kind", "fits"), [
        ("RS256", "rsa-2048", True),
        ("RS256", "rsa-1024", False),
        ("RS256", "rsa-512", False),
        ("RS256", "rsa-private", False),
        ("RS256", "ec-p256", False),
        ("RS256", "ec-p256-private", False),
        ("RS256", "ec-p384", False),
        ("RS256", "ec-secp256k1", False),
        ("RS256", "oct-pem", False),
        ("RS256", "oct", False),
        ("ES256", "ec-p256", True),
        ("ES256", "ec-p256-private", False),
        ("ES256", "ec-p384", False),
        ("ES256", "ec-secp256k1", False),
        ("ES256", "rsa-2048", False),
        ("ES256", "rsa-1024", False),
        ("ES256", "rsa-512", False),
        ("ES256", "rsa-private", False),
        ("ES256", "oct-pem", False),
        ("ES256", "oct", False),
    ], ids=lambda value: value if isinstance(value, str) else None)
    def test_every_cell(self, alg: str, kind: str, fits: bool) -> None:
        own = _OWN_SIGNER.get(kind)
        own_fits_alg = own is not None and (
            isinstance(own, rsa.RSAPrivateKey) == (alg == "RS256")
        )
        signer = own if own_fits_alg else _SIGNER[alg.lower()]
        token = _token(key=signer, alg=alg, kid="k-under-test")
        result = _verify(token, {"keys": [_entry(kind)]})
        if fits:
            assert isinstance(result, TelegramIdentity), result
        else:
            assert result is R.KEY_TYPE

    def test_the_grid_has_both_outcomes(self) -> None:
        """The pair: a fitting key of each algorithm gives the person,
        through the same entries and the same signing."""
        for alg, kind in (("RS256", "rsa-2048"), ("ES256", "ec-p256")):
            token = _token(key=_OWN_SIGNER[kind], alg=alg, kid="k-under-test")
            assert isinstance(
                _verify(token, {"keys": [_entry(kind)]}), TelegramIdentity,
            )


class TestTheClaims:
    @pytest.mark.parametrize(("change", "word"), [
        ({"exp": _DROP}, R.NO_EXP),
        ({"exp": None}, R.NO_EXP),
        ({"exp": "soon"}, R.NO_EXP),
        ({"exp": True}, R.NO_EXP),
        ({"exp": float("inf")}, R.NO_EXP),
        ({"iat": _DROP}, R.NO_IAT),
        ({"iat": "then"}, R.NO_IAT),
        ({"iat": False}, R.NO_IAT),
        ({"iss": _DROP}, R.ISSUER),
        ({"iss": "https://accounts.example.org"}, R.ISSUER),
        ({"iss": TELEGRAM_LOGIN_ISSUER + "/"}, R.ISSUER),
        ({"aud": _DROP}, R.AUDIENCE),
        ({"aud": "1234"}, R.AUDIENCE),
        ({"aud": int(_CLIENT)}, R.AUDIENCE),
        ({"aud": [_CLIENT]}, R.AUDIENCE),
    ], ids=["no-exp", "exp-null", "exp-text", "exp-true", "exp-infinite",
            "no-iat", "iat-text", "iat-false", "no-iss", "iss-other",
            "iss-slash", "no-aud", "aud-other", "aud-a-number", "aud-a-list"])
    def test_refused(self, change: dict[str, Any], word: IdTokenRejected) -> None:
        assert _verify(_token(_claims(**change))) is word

    def test_exp_with_the_leeway(self) -> None:
        stamp = int(_NOW.timestamp())
        leeway = int(CLOCK_LEEWAY.total_seconds())
        assert leeway == 30
        past = _token(_claims(exp=stamp - leeway - 1))
        within = _token(_claims(exp=stamp - leeway + 1))
        assert _verify(past) is R.EXPIRED
        assert isinstance(_verify(within), TelegramIdentity)

    def test_iat_with_the_leeway(self) -> None:
        stamp = int(_NOW.timestamp())
        ahead = _token(_claims(iat=stamp + 31))
        within = _token(_claims(iat=stamp + 29))
        assert _verify(ahead) is R.IAT_IN_FUTURE
        assert isinstance(_verify(within), TelegramIdentity)

    def test_no_good_id(self) -> None:
        assert _verify(_token(_claims(id=_DROP))) is R.NO_ID
        assert _verify(_token(_claims(id=True))) is R.NO_ID


class TestNotAToken:
    @pytest.mark.parametrize("token", [
        "", "abc", "a.b.c", "....", 5, None, b"x.y.z",
    ], ids=["empty", "word", "three-words", "dots", "int", "none", "bytes"])
    def test_malformed(self, token: object) -> None:
        assert _verify(token) is R.MALFORMED

    def test_a_signed_payload_that_is_no_object(self) -> None:
        """The signature good, the payload a JSON array: no claims."""
        token = jwt.api_jws.encode(b"[1, 2]", _RSA, algorithm="RS256",
                                   headers={"kid": "k-rsa"})
        assert _verify(token) is R.MALFORMED


# -----------------------------------------------------------------------------
# Item 3 -- the person
# -----------------------------------------------------------------------------


class TestTheId:
    @pytest.mark.parametrize(("value", "number"), [
        (12345, 12345),
        ("12345", 12345),
        ("00012345", 12345),
        (2**63 - 1, 2**63 - 1),
        (str(2**63 - 1), 2**63 - 1),
        (1, 1),
    ], ids=["number", "string", "leading-zeros", "max", "max-string", "one"])
    def test_taken(self, value: object, number: int) -> None:
        assert telegram_id(value) == number

    @pytest.mark.parametrize("value", [
        0, "0", "000", -1, "-1", 2**63, str(2**63), True, False, "12a",
        " 1", "+1", "1_0", "\u0661\u0662", "\uff11\uff12", "1" * 5000,
        None, 1.5, 12.0, "", [1],
    ], ids=["zero", "zero-string", "zeros", "negative", "negative-string",
            "2^63", "2^63-string", "true", "false", "12a", "space", "plus",
            "underscore", "arabic-digits", "fullwidth-digits", "5000-digits",
            "null", "float", "float-whole", "empty", "a-list"])
    def test_refused(self, value: object) -> None:
        assert telegram_id(value) is None


def _identity(**changes: Any) -> TelegramIdentity:
    found = identity_from_claims(_claims(**changes), verified_at=_NOW,
                                 bot_access_granted=False)
    assert isinstance(found, TelegramIdentity), found
    return found


class TestTheFields:
    def test_by_the_table(self) -> None:
        person = _identity()
        assert (person.first_name, person.last_name, person.username,
                person.photo_url) == ("Ada", "Lovelace", "ada",
                                      "https://cdn.example.org/ada.jpg")

    def test_the_subject_is_the_id_not_sub(self) -> None:
        person = _identity(id=987, sub="unit-other-sub")
        assert person.subject == "987"

    @pytest.mark.parametrize(("change", "first"), [
        ({"given_name": _DROP}, "Ada Lovelace"),
        ({"given_name": ""}, "Ada Lovelace"),
        ({"given_name": 5}, "Ada Lovelace"),
        ({"given_name": _DROP, "name": _DROP}, None),
        ({"given_name": _DROP, "name": ""}, None),
    ], ids=["no-given", "empty-given", "given-not-text", "neither",
            "neither-empty"])
    def test_first_name_falls_back_to_name(
        self, change: dict[str, Any], first: str | None,
    ) -> None:
        assert _identity(**change).first_name == first

    @pytest.mark.parametrize("field", ["family_name", "preferred_username"])
    def test_absent_is_null(self, field: str) -> None:
        person = _identity(**{field: _DROP})
        assert (person.last_name if field == "family_name"
                else person.username) is None

    @pytest.mark.parametrize("picture", [
        "http://cdn.example.org/ada.jpg", "javascript:alert(1)",
        "cdn.example.org/ada.jpg", "https:///ada.jpg", "", 5, None,
        "https://[::1/ada.jpg",
    ], ids=["http", "javascript", "no-scheme", "no-host", "empty",
            "not-text", "null", "unparseable"])
    def test_photo_only_https(self, picture: object) -> None:
        assert _identity(picture=picture).photo_url is None


# -----------------------------------------------------------------------------
# Item 4 -- the bot's right
# -----------------------------------------------------------------------------


class TestTheBotsRight:
    @pytest.mark.parametrize(("requested", "exchange", "granted"), [
        (False, {}, False),
        (False, {"scope": "openid profile telegram:bot_access"}, False),
        (True, {"access_token": "x"}, True),
        (True, {"scope": "openid profile"}, False),
        (True, {"scope": "openid profile telegram:bot_access"}, True),
        (True, {"scope": "telegram:bot_access_x openid"}, False),
        (True, {"scope": None}, False),
        (True, {"scope": 5}, False),
        (True, {"scope": ["telegram:bot_access"]}, False),
        (True, "scope", False),
        (True, None, False),
    ], ids=["not-asked", "not-asked-granted", "asked-no-scope",
            "asked-live-cell", "asked-granted", "asked-lookalike",
            "scope-null", "scope-number", "scope-a-list", "not-an-object",
            "none"])
    def test_by_the_rule(self, requested: bool, exchange: object,
                         granted: bool) -> None:
        assert bot_access_granted(requested, exchange) is granted


# -----------------------------------------------------------------------------
# The live sign-in's shape, and what is never shown
# -----------------------------------------------------------------------------


class TestTheLiveShape:
    def test_four_keys_oidc_1_string_id_string_aud(self) -> None:
        others = [_jwk(rsa.generate_private_key(public_exponent=65537,
                                                key_size=2048), f"oidc-{n}",
                       alg="RS256") for n in (2, 3, 4)]
        jwks = {"keys": [*others, _jwk(_RSA, "oidc-1", alg="RS256")]}
        exchange = {"access_token": "x", "expires_in": 3600, "id_token": "t",
                    "scope": "openid profile", "token_type": "Bearer"}
        person = _verify(_token(kid="oidc-1"), jwks,
                         granted=bot_access_granted(True, exchange))
        assert isinstance(person, TelegramIdentity)
        assert person.telegram_id == 123456789
        assert person.subject == "123456789"
        assert person.bot_access_granted is False


class TestNothingShown:
    def test_the_identity_repr_names_no_field(self) -> None:
        person = _identity()
        shown = repr(person)
        for value in ("Ada", "Lovelace", "ada", "123456789", "cdn.example"):
            assert value not in shown

    def test_no_log_line(self) -> None:
        with capture_logs() as logs:
            _verify(_token())
            _verify(_token(key=_RSA_OTHER))
            _verify("abc")
        assert logs == []
