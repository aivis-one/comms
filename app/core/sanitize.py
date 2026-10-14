# =============================================================================
# COMMS Service -- the one sanitizer (secrets out of every text comms
# writes: records, journal, logs)
# =============================================================================
#
# Lives in core (H8): the logging setup (app/core/logging.py) runs every
# log line through it, and core imports nothing above it -- core is
# rank 0 of the package graph (app/__init__.py). Every caller imports
# it from HERE; there is no second copy and no re-export
# (tests/test_log_sanitizing.py holds that with an AST check).
# =============================================================================

import re
import traceback

# Secret-redaction patterns (review 1.1, widened in F0-comms item 2).
#
# THE LIST IS DRIVEN BY THE SECRETS THIS SERVICE ACTUALLY HOLDS, in the
# form each takes where it can surface in an error text -- not by what a
# secret "usually" looks like. The secrets (app/core/config.py) and the
# form each is checked in:
#   DATABASE_URL        postgresql+asyncpg://comms:<pw>@host/db   userinfo
#   REDIS_URL           redis://:<pw>@host:6379/0 (EMPTY user --
#                       the form deploy/comms-deploy.sh writes)    userinfo
#   COMMS_SERVICE_TOKEN Authorization: Bearer <t>; bare hex         header,
#                                                                  bearer, hex
#   TELEGRAM_BOT_TOKEN  .../bot<id>:<secret>/sendMessage inside an
#                       aiohttp error text (aiogram wraps it as
#                       "<ClientError class>: <error>")            telegram
#   EMAIL_MAILGUN_API_KEY bare <32hex>-<8hex>-<8hex> or key-<32hex>;
#                       Authorization: Basic <b64 of api:key>     mailgun,
#                                                                  header
# Generated passwords and the service token (openssl rand -hex 24/32 in
# comms-deploy.sh) are also caught BARE by the long-hex pattern.
#
# ORDER MATTERS, and every pattern is IDEMPOTENT on its own output --
# the email path sanitizes the provider's text where it enters, and the
# service layer sanitizes the exception again when it writes the
# record, so every text goes through this twice:
#   1. the Authorization header, scheme included -- before bearer, so
#      "Authorization: Basic x" loses its credentials and not just the
#      word "Basic";
#   2. bearer on its own (a token with no header name in front);
#   3. userinfo in a URL/DSN, EMPTY user allowed, and the password runs
#      to the LAST @ of the authority (a raw @ in a password must not
#      leave its tail behind);
#   4. token-shaped values with no keyword next to them;
#   5. key=value / key: value for credential keywords.
_AUTH_HEADER_RE = re.compile(
    r"(?i)\b(authorization)\b\s*[=:]\s*(?:(?:bearer|basic|digest|token)\s+)?\S+"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+\S+")
_URL_USERINFO_RE = re.compile(r"(://[^/\s:@]*:)[^/\s]*(@)")
_TELEGRAM_TOKEN_RE = re.compile(r"\b(?:bot)?\d{6,}:[A-Za-z0-9_-]{30,}")
_MAILGUN_KEY_RE = re.compile(
    r"(?i)\b(?:key-[0-9a-f]{32}|[0-9a-f]{32}-[0-9a-f]{8}-[0-9a-f]{8})\b"
)
# KNOWN CEILING -- bare secrets the operator chose by hand.
#   1. Mechanics: this pattern catches a secret with no keyword, header
#      or URL around it ONLY in the shape comms-deploy.sh mints (48+ hex
#      characters). A password, token or key the operator typed in
#      another shape, surfacing bare in an error text, passes through
#      every pattern here unchanged.
#   2. Status: acknowledged by design.
#   3. Task: none -- no secret outside the minted shapes exists in any
#      deploy we know of, and the shape of a hand-typed value cannot be
#      recognised by any pattern without also eating ordinary text.
#   4. Unfreeze trigger: a secret appears in a deploy's configuration
#      that was NOT written by comms-deploy.sh generate_env and is not
#      one of the provider credentials matched above.
#   5. Agreed fix: redact by VALUE, not by shape -- read the configured
#      secrets from settings once and replace their literal occurrences.
#   6. Rejected: widening the shape patterns to "any long token" (eats
#      message ids, digests and ordinary identifiers, and still misses a
#      short password); refusing non-hex secrets at startup (not ours to
#      demand of a provider credential, and a red start on a valid
#      configuration).
_LONG_HEX_RE = re.compile(r"\b[0-9a-fA-F]{48,}\b")
_KEYVAL_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret|api[_-]?key|apikey"
    r"|authorization)\b\s*[=:]\s*\S+"
)


def sanitize_text(text: str) -> str:
    """Remove every secret form listed above from a text. Not truncated.

    Idempotent: sanitize_text(sanitize_text(x)) == sanitize_text(x).
    Callers cut AFTER calling this, never before: a cut through the
    middle of a secret can leave a prefix no pattern recognises. The
    one gap in coverage is the KNOWN CEILING on _LONG_HEX_RE.
    """
    text = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}=[redacted]", text)
    text = _BEARER_RE.sub("bearer [redacted]", text)
    text = _URL_USERINFO_RE.sub(r"\1[redacted]\2", text)
    text = _TELEGRAM_TOKEN_RE.sub("[redacted]", text)
    text = _MAILGUN_KEY_RE.sub("[redacted]", text)
    text = _LONG_HEX_RE.sub("[redacted]", text)
    return _KEYVAL_RE.sub(lambda m: f"{m.group(1)}=[redacted]", text)


def sanitize_error(exc: Exception) -> str:
    """An exception's text with secrets removed, cut to the record size."""
    return sanitize_text(str(exc))[:2000]


def sanitized_traceback(exc: BaseException) -> str:
    """The traceback structlog's format_exc_info would render, redacted.

    For a log line that must keep the stack but cannot let the renderer
    print it raw: log it as `exception=sanitized_traceback(exc)` instead
    of logger.exception(). The key and the text are what format_exc_info
    produces, so the record keeps its shape.

    Redacted as ONE text, never cut: traceback.format_exception prints
    str() of every exception in the chain (__cause__, __context__), the
    notes and the members of an exception group, and a secret can sit in
    any of them -- the aiogram network error carries the bot's URL both
    in its own text and in its __cause__. Frame locals are not printed
    by this formatter, so they are not a path. What the patterns cannot
    recognise is the KNOWN CEILING on _LONG_HEX_RE; this is another
    caller of sanitize_text, not another sanitizer.
    """
    rendered = "".join(traceback.format_exception(exc))
    # format_exc_info drops the one trailing newline; so does this.
    return sanitize_text(rendered.removesuffix("\n"))
