# =============================================================================
# COMMS Service -- the Telegram HTML a template may carry (TPL-2)
# =============================================================================
#
# The telegram text goes out with ParseMode.HTML, and Telegram refuses the
# whole message ("can't parse entities") when the markup does not parse --
# on EVERY letter of the type, since the markup is the template's: the
# values, the placeholders and the stored text are escaped
# (app/engine/formatters.py). So the profile is refused at load instead.
#
# THE RULES ARE TELEGRAM'S PARSER, ported: the Bot API turns parse_mode
# "html" into td_api::parseTextEntities (tdlib/telegram-bot-api,
# telegram-bot-api/Client.cpp), which TDLib answers with parse_html
# (td/telegram/SynchronousRequests.cpp -> td/telegram/MessageEntity.cpp
# parse_html, read at tdlib/td@4c6377a). Its SYNTAX refusals only:
#   - a start tag not closed by ">" before the end;
#   - a tag other than a b strong i em s strike del u ins tg-spoiler
#     tg-emoji tg-time span pre code blockquote;
#   - an empty attribute name; a bare attribute value (a name token:
#     letters, digits, "." and "-") not followed by a space or ">";
#   - <span> without class="tg-spoiler";
#   - an end tag with nothing open, not closed by ">", or naming another
#     tag than the one open ("</>" closes whatever is open);
#   - a tag still open at the end.
# A raw "<" always begins a tag -- "a < b" is refused. An unknown "&...;"
# is text, not an error. NOT ported -- refusals that depend on an
# attribute's VALUE: a tg-emoji id that is not a positive integer, a
# tg-time format, a link Telegram will not take. A template field stands
# as "1" when the text is checked (a valid id, a valid name token), so a
# field there is never refused for its value; a literal one is left to the
# send, where "can't parse entities" stays a refusal of the one letter.
# =============================================================================

TELEGRAM_HTML_TAGS = frozenset({
    "a", "b", "strong", "i", "em", "s", "strike", "del", "u", "ins",
    "tg-spoiler", "tg-emoji", "tg-time", "span", "pre", "code", "blockquote",
})

# TDLib's is_space (tdutils/td/utils/misc.h); the end of the text is its
# terminating NUL, which is_space counts too.
_SPACE = frozenset(" \t\r\n\0\v")


def _is_alnum(char: str) -> bool:
    """TDLib's is_alnum: ASCII letters and digits only."""
    return char.isascii() and char.isalnum()


def telegram_html_problem(text: str) -> str | None:
    """Why Telegram's HTML parser refuses `text`, in its own words but for
    the offset, or None."""
    # The text with its terminating NUL, as the parser reads it.
    t = text + "\0"
    n = len(text)
    open_tags: list[str] = []
    i = 0
    while i < n:
        if t[i] != "<":
            i += 1
            continue
        i += 1
        if t[i] != "/":
            problem, i = _start_tag(t, i, open_tags)
        else:
            problem, i = _end_tag(t, i, open_tags)
        if problem is not None:
            return problem
    if open_tags:
        return f'no end tag for the start tag "{open_tags[-1]}"'
    return None


def _start_tag(t: str, i: int, open_tags: list[str]) -> tuple[str | None, int]:
    begin = i
    while t[i] not in _SPACE and t[i] != ">":
        i += 1
    if t[i] == "\0":
        return "a start tag is not closed", i
    tag = t[begin:i].lower()
    if tag not in TELEGRAM_HTML_TAGS:
        return f'the start tag "{tag}" is not one Telegram supports', i
    argument = ""
    while t[i] != ">":
        while t[i] != "\0" and t[i] in _SPACE:
            i += 1
        if t[i] == ">":
            break
        name_begin = i
        while t[i] not in _SPACE and t[i] not in "=>/\"'":
            i += 1
        attribute = t[name_begin:i]
        if not attribute:
            return f'an empty attribute name in the tag "{tag}"', i
        while t[i] != "\0" and t[i] in _SPACE:
            i += 1
        if t[i] != "=":
            if t[i] == "\0":
                return f'the start tag "{tag}" is not closed', i
            continue
        i += 1
        while t[i] != "\0" and t[i] in _SPACE:
            i += 1
        if t[i] == "\0":
            return f'the start tag "{tag}" is not closed', i
        if t[i] not in "'\"":
            value_begin = i
            while _is_alnum(t[i]) or t[i] in ".-":
                i += 1
            value = t[value_begin:i].lower()
            if t[i] not in _SPACE and t[i] != ">":
                return (
                    f'an unquoted value in the tag "{tag}" ends in a '
                    f"character a name token does not take", i
                )
        else:
            quote = t[i]
            i += 1
            value_begin = i
            while t[i] != quote and t[i] != "\0":
                i += 1
            value = t[value_begin:i]
            if t[i] == quote:
                i += 1
        if t[i] == "\0":
            return "a start tag is not closed", i
        if tag == "span" and attribute == "class" and value.startswith("tg-"):
            argument = value[3:]
    if tag == "span" and argument != "spoiler":
        return 'the tag "span" must have class "tg-spoiler"', i
    open_tags.append(tag)
    return None, i + 1


def _end_tag(t: str, i: int, open_tags: list[str]) -> tuple[str | None, int]:
    if not open_tags:
        return "an end tag closes no open tag", i
    begin = i + 1
    while t[i] not in _SPACE and t[i] != ">":
        i += 1
    end = t[begin:i].lower()
    while t[i] in _SPACE and t[i] != "\0":
        i += 1
    if t[i] != ">":
        return "an end tag is not closed", i
    if end and end != open_tags[-1]:
        return f'the end tag "{end}" closes the start tag "{open_tags[-1]}"', i
    open_tags.pop()
    return None, i + 1
