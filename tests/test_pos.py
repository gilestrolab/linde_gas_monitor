"""Tests for the multi-PO support: loading, usage counting, and selection."""
from datetime import datetime

import pytest


# ---------- load_pos ----------

def test_load_pos_from_file(make_link):
    link = make_link(pos=[
        {'number': 'PO-A', 'email': 'a@x', 'ratio': 1,
         'created': '2025-01-01', 'expires': '2027-01-01'},
        {'number': 'PO-B', 'email': 'b@x', 'ratio': 2,
         'created': '2025-06-01', 'expires': '2027-06-01'},
    ])
    assert [po['number'] for po in link.pos] == ['PO-A', 'PO-B']
    assert link.pos[1]['ratio'] == 2


def test_load_pos_fallback_to_credentials(make_link):
    """If pos.json is absent, fall back to the legacy single PO."""
    link = make_link()
    assert len(link.pos) == 1
    assert link.pos[0]['number'] == 'LEGACY-PO'
    assert link.pos[0]['email'] == 'legacy@example.com'
    assert link.pos[0]['ratio'] == 1
    assert link.pos[0]['expires'] is None


# ---------- get_po_usage ----------

def test_usage_counts_three_col_lines_only(make_link):
    """Legacy 2-col lines must not contribute to usage counts."""
    link = make_link(
        pos=[{'number': 'PO-A', 'ratio': 1}],
        log_lines=[
            '2025-01-01 10:00,left,PO-A',
            '2025-01-02 10:00,right,PO-A',
            '2025-01-03 10:00,left',            # legacy 2-col -> ignored
            '2025-01-04 10:00,left,PO-OTHER',   # not in configured set -> ignored
        ],
    )
    assert link.get_po_usage() == {'PO-A': 2}


def test_usage_with_no_log(make_link):
    link = make_link(pos=[{'number': 'PO-A', 'ratio': 1}])
    assert link.get_po_usage() == {'PO-A': 0}


# ---------- select_po ----------

def test_select_po_single(make_link):
    link = make_link(pos=[
        {'number': 'PO-A', 'ratio': 1, 'expires': '2099-12-31'},
    ])
    assert link.select_po()['number'] == 'PO-A'


def test_select_po_respects_ratio(make_link):
    """Over many rounds the 2:1 ratio must be respected exactly."""
    link = make_link(pos=[
        {'number': 'PO-A', 'ratio': 2, 'expires': '2099-12-31'},
        {'number': 'PO-B', 'ratio': 1, 'expires': '2099-12-31'},
    ])
    picks = []
    for i in range(30):
        po = link.select_po()
        picks.append(po['number'])
        with open(link.last_alert_file, 'a') as f:
            f.write(f'2025-01-01 {i:02d}:00,left,{po["number"]}\n')
    a, b = picks.count('PO-A'), picks.count('PO-B')
    assert b > 0
    assert a == 2 * b, f"expected 2:1 ratio, got {a}:{b}"


def test_select_po_equal_ratio_cycles(make_link):
    """Equal ratios produce a strict alternation."""
    link = make_link(pos=[
        {'number': 'PO-A', 'ratio': 1, 'expires': '2099-12-31'},
        {'number': 'PO-B', 'ratio': 1, 'expires': '2099-12-31'},
    ])
    picks = []
    for i in range(6):
        po = link.select_po()
        picks.append(po['number'])
        with open(link.last_alert_file, 'a') as f:
            f.write(f'2025-01-01 {i:02d}:00,left,{po["number"]}\n')
    assert picks == ['PO-A', 'PO-B', 'PO-A', 'PO-B', 'PO-A', 'PO-B']


def test_select_po_skips_expired(make_link):
    link = make_link(pos=[
        {'number': 'PO-EXP', 'ratio': 1, 'expires': '2020-01-01'},
        {'number': 'PO-OK',  'ratio': 1, 'expires': '2099-12-31'},
    ])
    for _ in range(5):
        assert link.select_po()['number'] == 'PO-OK'


def test_select_po_skips_zero_ratio(make_link):
    """Ratio <= 0 makes a PO inactive (a way to pause it without deletion)."""
    link = make_link(pos=[
        {'number': 'PO-OFF', 'ratio': 0, 'expires': '2099-12-31'},
        {'number': 'PO-ON',  'ratio': 1, 'expires': '2099-12-31'},
    ])
    for _ in range(5):
        assert link.select_po()['number'] == 'PO-ON'


def test_select_po_no_pos_returns_none(make_link):
    link = make_link(pos=[])
    assert link.select_po() is None


def test_select_po_all_expired_returns_none(make_link):
    link = make_link(pos=[
        {'number': 'PO-X', 'ratio': 1, 'expires': '2020-01-01'},
    ])
    assert link.select_po() is None


def test_select_po_handles_invalid_expiry(make_link):
    """A malformed expires field should not crash; the PO stays usable."""
    link = make_link(pos=[
        {'number': 'PO-A', 'ratio': 1, 'expires': 'not-a-date'},
    ])
    assert link.select_po()['number'] == 'PO-A'


# ---------- initial_amount: passed through to the PO record and rendered ----------

def test_initial_amount_preserved_on_load(make_link):
    link = make_link(pos=[
        {'number': 'PO-A', 'email': 'pi@x', 'ratio': 1,
         'initial_amount': 500, 'expires': '2099-12-31'},
    ])
    assert link.pos[0]['initial_amount'] == 500


def test_initial_amount_rendered_in_pos_tab(make_link):
    """The Initial Amount column should appear and integer values render as £NNN."""
    import linde_manager
    link = make_link(pos=[
        {'number': 'PO-A', 'email': 'pi@x', 'ratio': 1,
         'initial_amount': 500, 'expires': '2099-12-31'},
        {'number': 'PO-NOAMT', 'email': 'pi2@x', 'ratio': 1,
         'expires': '2099-12-31'},
    ])
    class HStub: pass
    html = linde_manager.RequestHandler.render_pos_tab(HStub())
    assert '<th>Initial Amount</th>' in html
    assert '£500' in html
    # POs without an initial amount should render an em-dash, not "None"
    assert '>None<' not in html
