# =============================================================================
# COMMS Service -- Module Specification (options that are not channels)
# =============================================================================
#
# An option of the instance (primer.yaml `options`) is a channel
# (app/core/channels.py) or a MODULE: a capability of comms that is not
# a way to send -- the sign-in through Telegram (`login`). A module is
# switched like a channel and by nothing else:
#
#     flag `false`     -> the module is off on this deploy; its keys are
#                         NOT read and NOT checked, whatever they hold
#     flag `true`      -> every channel the module needs must be switched
#                         on, and every key of the module set (not empty,
#                         not whitespace alone); otherwise startup is
#                         REFUSED, naming the option and every such key
#                         -- never a key's value: keys carry secrets
#     any other value, or no flag at all
#                      -> startup is REFUSED, naming the flag: exactly
#                         `true` or `false` (FLAG_VALUES, the channels'
#                         rule)
#
#   On a box the machine renders the flag `false` for an option the
#   passport does not switch on, and writes its literal
#   `primer-switched-off` into a key under `when:` of a switched-off
#   option (the machine v0.1.8, pridem/stacks/options.py: flag(), off()
#   and LITERAL). That value is not known here and never compared: the
#   keys of a switched-off module are simply not read.
#
# A module is NOT a channel: it is not in CHANNEL_SPECS, has no
# DeliveryChannel, no formatter and no state in the channel map of the
# startup log or of /health.
#
# WHY A SEPARATE MODULE OF ITS OWN: it imports no settings, as
# app/core/channels.py does not -- the test suite switches every module
# off and blanks its keys BEFORE the settings object exists
# (tests/conftest.py). Its refusals are collected with every other
# problem of the start (app/core/config.py Settings._validate): one
# message, fixed in one pass.
# =============================================================================

from collections.abc import Mapping
from dataclasses import dataclass

from app.core.channels import FLAG_VALUES, channel_spec


@dataclass(frozen=True)
class ModuleSpec:
    """An option of the instance that is not a channel."""

    # The option's name, as primer.yaml `options` and `when:` spell it.
    name: str
    # The variable that switches it: exactly `true` or `false`.
    flag: str
    # The keys a switched-on module needs, every one set. Not read at all
    # when it is off.
    keys: tuple[str, ...]
    # The channels a switched-on module needs switched on.
    channels: tuple[str, ...]


MODULE_SPECS: tuple[ModuleSpec, ...] = (
    # The sign-in through Telegram: the OIDC client's secret, from
    # BotFather; the sign-in is Telegram's, so the channel is needed.
    ModuleSpec(
        name="login",
        flag="LOGIN_ENABLED",
        keys=("TELEGRAM_LOGIN_CLIENT_SECRET",),
        channels=("telegram",),
    ),
)


def module_flag_keys() -> tuple[str, ...]:
    """Every module's flag, in declaration order."""
    return tuple(spec.flag for spec in MODULE_SPECS)


def module_env_keys() -> tuple[str, ...]:
    """Every key of every module, in declaration order."""
    return tuple(key for spec in MODULE_SPECS for key in spec.keys)


def _flag_problem(spec: ModuleSpec, value: str | None) -> str:
    """The refusal of a flag that is not exactly true or false."""
    expected = " or ".join(FLAG_VALUES)
    if value is None:
        return (
            f"{spec.flag} is not set: it switches option '{spec.name}' "
            f"on or off and must be exactly {expected}"
        )
    return (
        f"{spec.flag}={value!r} is not a switch: option '{spec.name}' "
        f"is on or off by exactly {expected}"
    )


def module_problems(values: Mapping[str, str | None]) -> list[str]:
    """Apply the rule to one deploy's flags and keys.

    Args:
        values: Variable name -> configured value, the module flags and
            keys and the flags of the channels the modules need. A key
            that is missing or "" is not set; a flag that is missing (or
            None) is not set either -- and that is a refusal.

    Returns:
        One human-readable problem per broken module -- all of them. A
        key is named, its value never is.

    A channel flag that is not exactly true or false is refused by the
    channels' rule, naming it; whether the channel is on is then not
    known, so a module's need of it is not judged.
    """
    problems: list[str] = []
    for spec in MODULE_SPECS:
        flag = values.get(spec.flag)
        if flag not in FLAG_VALUES:
            problems.append(_flag_problem(spec, flag))
            continue
        if flag == "false":
            # Switched off: nothing of the module is read.
            continue

        lines: list[str] = []
        for channel in spec.channels:
            channel_flag = channel_spec(channel).flag
            if values.get(channel_flag or "") == "false":
                lines.append(
                    f"  needs channel '{channel}', which is switched off "
                    f"({channel_flag}=false)"
                )
        missing = [key for key in spec.keys if (values.get(key) or "") == ""]
        if missing:
            lines.append(f"  missing: {', '.join(missing)}")
        lines.extend(
            f"  {key} consists only of whitespace"
            for key in spec.keys
            if key not in missing and (values.get(key) or "").strip() == ""
        )
        if not lines:
            continue
        channels = ", ".join(
            f"{channel_spec(channel).flag}=true" for channel in spec.channels
        )
        problems.append("\n".join([
            f"option '{spec.name}' is misconfigured:",
            *lines,
            f"  {spec.flag}=true: set {channels} and every key of "
            f"'{spec.name}' ({', '.join(spec.keys)}), or set "
            f"{spec.flag}=false if this deploy has no {spec.name}.",
        ]))
    return problems
