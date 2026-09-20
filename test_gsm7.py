"""
Tests for tasks/utils/gsm7.py.

The money is in the segment arithmetic, so that is tested against the exact
boundaries Twilio bills on (160/153 GSM-7, 70/67 UCS-2) rather than by
example. The "never return an empty field" guard is tested explicitly because
its failure mode ships "Hi ," to a paying customer.

Run:  python -m pytest test_gsm7.py -q
"""

import pathlib
import re

import pytest

from tasks.utils.gsm7 import (
    GSM7_BASIC,
    GSM7_EXTENDED,
    gsm7_length,
    is_gsm7,
    sms_segments,
    to_gsm7,
)


# --- the alphabet tables themselves -----------------------------------------

def test_basic_alphabet_has_the_127_characters_the_spec_defines():
    """128 table positions, but 0x1B is the ESC that introduces the extension
    table rather than a character you can send, so 127 are usable."""
    assert len(GSM7_BASIC) == 127


def test_extension_table_characters_are_gsm7_but_cost_two_septets():
    for ch in GSM7_EXTENDED:
        assert is_gsm7(ch)
        assert gsm7_length(ch) == 2


def test_characters_outside_the_alphabet_are_rejected():
    for ch in ("醫", "’", "🤖", " "):
        assert not is_gsm7(ch)
    assert gsm7_length("醫師") == -1


def test_the_accented_letters_that_are_already_free_are_not_touched():
    """GSM-7 carries a chunk of Latin-1 natively. Folding these would be a
    pointless loss of fidelity — they cost exactly one septet as they are."""
    for ch in "éèùìòÇØøÅåÄÖÑÜäöñüàß¿¡§£$¥¤":
        assert is_gsm7(ch), ch
        assert gsm7_length(ch) == 1


# --- segment arithmetic (this is what gets billed) ---------------------------

@pytest.mark.parametrize("length,expected", [
    (1, 1), (160, 1),     # a single segment gets the full 160
    (161, 2), (306, 2),   # once concatenated every part drops to 153
    (307, 3),
])
def test_gsm7_segment_boundaries(length, expected):
    assert sms_segments("a" * length) == expected


@pytest.mark.parametrize("length,expected", [
    (1, 1), (70, 1),      # a single UCS-2 segment gets 70
    (71, 2), (134, 2),    # concatenated UCS-2 parts hold 67
    (135, 3),
])
def test_ucs2_segment_boundaries(length, expected):
    assert sms_segments("醫" * length) == expected


def test_one_stray_character_doubles_the_price_of_a_whole_message():
    """The entire reason this module exists."""
    body = "a" * 245
    assert sms_segments(body) == 2
    assert sms_segments(body[:242] + "鄭醫師") == 4


def test_emoji_cost_two_ucs2_units_each():
    """Astral-plane characters are surrogate pairs in UTF-16, so a body of 36
    emoji does not fit the 70-unit single segment."""
    assert sms_segments("🤖" * 35) == 1
    assert sms_segments("🤖" * 36) == 2


def test_empty_text_costs_nothing():
    assert sms_segments("") == 0


# --- folding ----------------------------------------------------------------

def test_plain_ascii_is_returned_untouched():
    name = "Cindy Cheng"
    assert to_gsm7(name) is name


def test_the_production_case_a_chinese_honorific_on_a_latin_name():
    assert to_gsm7("Cindy Cheng 鄭醫師") == "Dr Cindy Cheng"
    assert to_gsm7("Carrie Xu 徐醫師") == "Dr Carrie Xu"


def test_longer_title_terms_win_over_shorter_ones():
    assert to_gsm7("Jenny Wang 王中醫師") == "Dr Jenny Wang"


def test_titles_can_be_suppressed_for_fields_where_dr_makes_no_sense():
    assert to_gsm7("Cindy Cheng 鄭醫師", titles=False) == "Cindy Cheng"


def test_accents_fold_to_latin_rather_than_being_deleted():
    """ü is not in GSM-7 so it folds to u; ö and ø ARE, so they stay."""
    assert to_gsm7("Zoë Müller-Sørensen 陳") == "Zoe Muller-Sørensen"
    assert to_gsm7("José") == "José"  # already GSM-7 — é is in the alphabet


def test_smart_punctuation_is_normalised():
    assert to_gsm7("Mum’s Kitchen") == "Mum's Kitchen"
    assert to_gsm7("Deep–Tissue “Relax”") == 'Deep-Tissue "Relax"'


def test_invisible_characters_from_pasted_text_are_removed():
    """U+2006 turns up in real production booking data and is not free."""
    assert to_gsm7("Acupuncture Follow up") == "Acupuncture Follow up"
    assert to_gsm7("Massage​﻿") == "Massage"


def test_emoji_are_dropped():
    assert to_gsm7("Speako AI \U0001f916 New Call ✅") == "Speako AI New Call"


def test_folding_closes_up_the_gap_it_leaves_behind():
    assert to_gsm7("30 mins 針灸 Acupuncture") == "30 mins Acupuncture"
    assert to_gsm7("Herbal 療程.") == "Herbal."       # no space left before the stop
    assert to_gsm7("  陳 Massage  ") == "Massage"     # no leading/trailing space


# --- the guard --------------------------------------------------------------

def test_a_field_with_no_latin_at_all_is_returned_unchanged():
    """Real production row. Sending UCS-2 beats sending an empty service name."""
    original = "此處請不要預約"
    assert to_gsm7(original) == original


def test_a_title_only_name_is_not_reduced_to_a_bare_dr():
    assert to_gsm7("醫師") == "醫師"


def test_none_and_non_strings_pass_straight_through():
    """Callers hand us nullable DB columns."""
    assert to_gsm7(None) is None
    assert to_gsm7("") == ""


# --- end to end -------------------------------------------------------------

def _import_sms_or_skip():
    """tasks.sms pulls in tasks.celery_app, and this repo's local venv has a
    celery package that cannot be imported under Python 3.13. Skip there rather
    than fail; CI and the Render workers import it fine."""
    try:
        import tasks.sms as sms
    except ImportError as exc:                      # pragma: no cover
        pytest.skip(f"tasks.sms not importable here: {exc}")
    return sms


def test_every_sms_builder_folds_its_fields():
    """
    Source-level guard. A seventh SMS builder that forgets `_sms_safe_fields`
    would silently reintroduce UCS-2 pricing, and no unit test would notice —
    so count the builders and the fold sites and require them to agree.
    """
    source = (pathlib.Path(__file__).parent / "tasks" / "sms.py").read_text()
    builders = re.findall(r"^def (send_sms_\w+|send_reminder)\(", source, re.M)
    folds = source.count("_sms_safe_fields(") - 1      # minus the definition
    assert len(builders) == 6, builders
    assert folds == len(builders), (
        f"{len(builders)} SMS builders but {folds} fold sites — a builder is "
        f"interpolating raw DB values into an SMS body."
    )


def test_sms_safe_fields_folds_the_four_interpolated_columns():
    sms = _import_sms_or_skip()

    out = sms._sms_safe_fields(
        "Juecha",
        "HRT Acupuncture and Massage Centre -Sunnybank Plaza",
        "Cindy Cheng 鄭醫師",
        "General Consultation Initial",
    )
    assert out[2] == "Dr Cindy Cheng"          # staff keeps the title
    assert out[1] == "HRT Acupuncture and Massage Centre -Sunnybank Plaza"

    # A title prefix belongs on staff and nowhere else.
    customer, location, staff, service = sms._sms_safe_fields(
        "陳大文 醫師", "Mum’s Kitchen", None, "此處請不要預約"
    )
    assert not customer.startswith("Dr")
    assert location == "Mum's Kitchen"
    assert staff is None                        # None passes straight through
    assert service == "此處請不要預約"            # guard: no Latin left, keep it


def test_a_real_confirmation_body_halves_its_segment_count():
    template = (
        "Hi Juecha, your booking (Ref: 3787) is confirmed at HRT Acupuncture and "
        "Massage Centre -Sunnybank Plaza on 21 Sep 2026 (Mon) 11:30AM with {staff} "
        "for General Consultation Initial. Manage your booking: "
        "https://tinyurl.com/4zu66fsa [Speako AI]"
    )
    before = template.format(staff="Cindy Cheng 鄭醫師")
    after = template.format(staff=to_gsm7("Cindy Cheng 鄭醫師"))
    assert sms_segments(before) == 4
    assert sms_segments(after) == 2
