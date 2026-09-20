"""
GSM-7 safety for SMS bodies.

WHY THIS EXISTS
---------------
An SMS is billed per *segment*, and the segment size depends on which alphabet
the whole message is encoded in:

    GSM-7 (the 7-bit default alphabet)  160 chars single / 153 per concatenated part
    UCS-2 (UTF-16, used for everything else)  70 chars single /  67 per concatenated part

A *single* character outside GSM-7 re-encodes the ENTIRE message as UCS-2 and
cuts its capacity by 2.28x. In September 2026, 70% of production SMS were UCS-2
and carried 84% of all billed segments — because four of one clinic's five staff
have a Chinese honorific in `staff.name` ("Cindy Cheng 鄭醫師"). The median UCS-2
message contained just THREE non-GSM characters and paid double for them.

So: fold interpolated fields to GSM-7 at SMS render time.

SCOPE — READ BEFORE REUSING
---------------------------
⚠️ DISPLAY ONLY, AND ONLY FOR SMS. This never touches the database, the
dashboard, or email bodies (email has no 7-bit alphabet and no segment cost, so
folding there would destroy information for no gain). Apply it to the *values*
interpolated into an SMS — customer name, staff name, service name, location
name — never to the template itself, and never to anything the AI agent reads.

NEVER RETURNS AN EMPTY FIELD
----------------------------
A name written entirely in a non-Latin script folds to "". Emitting "Hi ," or
"with  for " is worse than paying for UCS-2, so `to_gsm7` returns the ORIGINAL
string in that case and the message is sent as UCS-2. On production today that
guard fires for 2 rows (1 service name, 1 customer name).
"""

import re
import unicodedata

# --- GSM 03.38 default alphabet (1 septet each) -----------------------------
# Order follows the spec's table so it can be checked against it by eye.
GSM7_BASIC = frozenset(
    "@£$¥èéùìòÇ\nØø\rÅå"
    "Δ_ΦΓΛΩΠΨΣΘΞÆæßÉ"
    " !\"#¤%&'()*+,-./"
    "0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNO"
    "PQRSTUVWXYZÄÖÑÜ§"
    "¿abcdefghijklmno"
    "pqrstuvwxyzäöñüà"
)

# --- GSM 03.38 extension table (ESC + char = 2 septets each) ----------------
# These are legal but cost DOUBLE, which matters when a body sits near a
# segment boundary. `gsm7_length` accounts for that; `is_gsm7` accepts them.
GSM7_EXTENDED = frozenset("\f^{}\\[~]|€")

# --- Punctuation and symbols that are safe to normalise ---------------------
# Word processors, Google Sheets and phone keyboards inject these constantly.
# Each one silently doubles the price of an entire SMS, and every replacement
# below is visually equivalent to a reader.
_PUNCTUATION = {
    "‘": "'", "’": "'", "‚": "'", "‛": "'",   # curly single quotes
    "“": '"', "”": '"', "„": '"', "‟": '"',   # curly double quotes
    "–": "-", "—": "-", "―": "-", "−": "-",   # en/em dash, minus
    "…": "...",                                              # ellipsis
    "•": "-", "·": "-",                                 # bullets
    "×": "x",                                                # multiplication sign
    "⁄": "/",                                                # fraction slash
    "°": " deg",                                             # degree sign
    "™": "(TM)", "®": "(R)", "©": "(C)",
    "₹": "INR ", "€": "€",                         # rupee -> INR; euro is in the ext table
}

# Every Unicode space separator (U+2006 SIX-PER-EM SPACE turns up in real
# production booking data) plus the zero-width characters that ride along with
# copy-pasted text and are invisible but not free.
_SPACES = dict.fromkeys(
    [" ", " ", " ", " ", " ", " ", " ",
     " ", " ", " ", " ", " ", " ", " ",
     " ", "　"],
    " ",
)
_ZERO_WIDTH = dict.fromkeys(["​", "‌", "‍", "﻿", "­"], "")

# --- Professional titles worth keeping ---------------------------------------
# Dropping CJK outright would turn "Cindy Cheng 鄭醫師" into "Cindy Cheng" and
# lose the fact that she is a doctor. These terms are therefore REMOVED from
# where they appear and re-attached as an English title PREFIX, which is how
# the title reads naturally in English:
#
#     "Cindy Cheng 鄭醫師"  ->  "Dr Cindy Cheng"
#
# Keep this list short and evidence-led. A term only belongs here if dropping
# it would lose meaning a customer relies on; decorative characters should just
# be dropped by the generic path below.
TITLE_PREFIXES = {
    "醫師": "Dr",
    "醫生": "Dr",
    "医师": "Dr",
    "医生": "Dr",
    "中醫師": "Dr",
    "中医师": "Dr",
    "藥師": "Dr",
    "药师": "Dr",
}

_WHITESPACE_RUN = re.compile(r"[ \t]{2,}")
_SPACE_BEFORE_PUNCT = re.compile(r" +([,.;:!?)])")


def is_gsm7(text: str) -> bool:
    """True if every character can be sent in the GSM-7 alphabet."""
    if not text:
        return True
    return all(ch in GSM7_BASIC or ch in GSM7_EXTENDED for ch in text)


def gsm7_length(text: str) -> int:
    """
    Length of `text` in SEPTETS, not characters — extension-table characters
    (^{}\\[~]|€) occupy two. Returns -1 if the text is not GSM-7 encodable, so
    callers can distinguish "long" from "not representable".
    """
    total = 0
    for ch in text or "":
        if ch in GSM7_BASIC:
            total += 1
        elif ch in GSM7_EXTENDED:
            total += 2
        else:
            return -1
    return total


def sms_segments(text: str) -> int:
    """
    How many segments Twilio will bill for `text`.

    A message that fits in one segment uses the full alphabet capacity (160
    GSM-7 / 70 UCS-2). Once it does not fit, EVERY part loses room to the
    concatenation header, so the limits drop to 153 / 67 — which is why a
    161-character message costs two segments, not one and a bit.
    """
    if not text:
        return 0
    septets = gsm7_length(text)
    if septets >= 0:
        return 1 if septets <= 160 else -(-septets // 153)
    # UCS-2 is billed in UTF-16 code units, so astral characters (emoji) cost 2.
    units = sum(2 if ord(ch) > 0xFFFF else 1 for ch in text)
    return 1 if units <= 70 else -(-units // 67)


def _tidy(text: str) -> str:
    """Close up the gaps left behind by removed characters."""
    text = _WHITESPACE_RUN.sub(" ", text)
    text = _SPACE_BEFORE_PUNCT.sub(r"\1", text)
    return text.strip()


def to_gsm7(text, *, titles: bool = True):
    """
    Fold `text` into the GSM-7 alphabet for use in an SMS body.

    The order matters, cheapest and most faithful first:
      1. already GSM-7  -> returned untouched (the common case, no allocation)
      2. title terms    -> lifted out and re-attached as an English prefix
      3. spaces/punct   -> normalised to their ASCII equivalents
      4. accents        -> NFKD-folded, so "José" becomes "Jose" rather than "Jos"
      5. anything left  -> dropped

    Returns the ORIGINAL value unchanged if folding would leave nothing behind
    (see the module docstring) or if `text` is None/not a string, so callers can
    pass a nullable DB column straight in.

    Set `titles=False` for fields where a "Dr" prefix would be nonsense, such as
    a customer name or a location name.
    """
    if not isinstance(text, str) or not text:
        return text
    if is_gsm7(text):
        return text

    working = text
    prefix = ""

    if titles:
        # Longest first, so "中醫師" is matched before "醫師".
        for term in sorted(TITLE_PREFIXES, key=len, reverse=True):
            if term in working:
                working = working.replace(term, " ")
                prefix = TITLE_PREFIXES[term]
                break

    for source, replacement in {**_SPACES, **_ZERO_WIDTH, **_PUNCTUATION}.items():
        if source in working:
            working = working.replace(source, replacement)

    # NFKD splits "é" into "e" + combining acute; dropping the combining marks
    # leaves readable Latin instead of a hole.
    working = "".join(
        ch for ch in unicodedata.normalize("NFKD", working)
        if not unicodedata.combining(ch)
    )

    working = "".join(ch for ch in working if ch in GSM7_BASIC or ch in GSM7_EXTENDED)
    working = _tidy(working)

    if not working:
        # Nothing survived — an empty name is worse than a UCS-2 message.
        return text

    return _tidy(f"{prefix} {working}") if prefix else working
