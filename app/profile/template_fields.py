# =============================================================================
# COMMS Service -- what a template field may be (TPL-1)
# =============================================================================
#
# One rule for the two places that read a template's fields: the profile
# loader, which refuses a template at startup (app/profile/loader.py
# _leaf_complaint), and the renderer, which must not do at delivery what
# the loader refuses (app/engine/template_engine.py). The fields are read
# by the parser str.format_map itself uses (_string, through
# string.Formatter): a field name splits into its first name and its
# steps, each an attribute or an item.
#
#   - NO ATTRIBUTE. A variable is a JSON value: an attribute of it is
#     never the letter's, and every Python object has some --
#     {x.__class__} printed "<class 'str'>", and a chain of them walks
#     into the interpreter. Item access ({user[name]}) is the one way in.
#   - NO FIELD INSIDE A FORMAT SPEC. {amount:{width}} takes its width from
#     the letter, and a width is memory: the product's value would size
#     the string comms builds.
#   - NO WIDTH OR PRECISION ABOVE MAX_TEMPLATE_SPEC_WIDTH. A width wider
#     than a whole letter means nothing to the person (a telegram text is
#     4096 characters, app/engine/formatters.py; an email body 5000,
#     MAX_BODY_LEN) and costs the worker the memory of it.
# =============================================================================

import re
from _string import formatter_field_name_split
from string import Formatter

# The widest field and the longest precision a template may ask for.
MAX_TEMPLATE_SPEC_WIDTH = 4096

# The standard format spec: [[fill]align][sign][z][#][0][width]
# [grouping][.precision][type]. A spec this does not match is left to
# format() itself, which refuses it.
_SPEC_RE = re.compile(
    r"(?:.?[<>=^])?[-+ ]?z?#?0?(?P<width>\d+)?[,_]?"
    r"(?:\.(?P<precision>\d+))?[a-zA-Z%]?",
    re.DOTALL,
)


def spec_too_wide(spec: str) -> bool:
    """Whether a format spec asks for a width or a precision above
    MAX_TEMPLATE_SPEC_WIDTH."""
    match = _SPEC_RE.fullmatch(spec)
    if match is None:
        return False
    return any(
        group is not None and int(group) > MAX_TEMPLATE_SPEC_WIDTH
        for group in (match.group("width"), match.group("precision"))
    )


def path_has_attribute(field_name: str) -> bool:
    """Whether a field path takes an attribute anywhere ({x.y},
    {x[a].__class__}) -- by the split format_map makes of it."""
    _, steps = formatter_field_name_split(field_name)
    return any(is_attr for is_attr, _ in steps)


def field_problem(template: str) -> str | None:
    """What makes a template's fields unfit, by the rules above, or
    None. Raises ValueError on a template the parser refuses (unbalanced
    braces) -- the caller's dry run names that one."""
    for _, field_name, spec, _ in Formatter().parse(template):
        if field_name is None:
            continue
        if path_has_attribute(field_name):
            return (
                f"uses attribute access on a template variable "
                f"({{{field_name}}}). Variables are JSON values, so "
                f"`{{user.name}}` fails at runtime and `{{x.__class__}}` "
                f"reaches into the interpreter -- use item access "
                f"(`{{user[name]}}`)."
            )
        if spec and any(
            name is not None for _, name, _, _ in Formatter().parse(spec)
        ):
            return (
                f"takes a field inside the format spec of {{{field_name}}}: a "
                f"width or a precision from the letter would size the text "
                f"comms builds"
            )
        if spec and spec_too_wide(spec):
            return (
                f"asks {{{field_name}}} for a width or precision above "
                f"{MAX_TEMPLATE_SPEC_WIDTH}, wider than any letter"
            )
    return None
