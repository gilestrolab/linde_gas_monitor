"""Test fixtures for linde_manager.

Adds the app/ directory to sys.path so tests can `import linde_manager`
without requiring an installable package layout, then provides a fixture
that builds a minimal LindeLink instance bypassing the network-heavy
__init__ (token fetch, SMTP probe).
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'app')))

import linde_manager  # noqa: E402


def _make_link(data_dir, pos=None, log_lines=None, credentials=None):
    """
    Build a LindeLink-like object backed by a temp data dir, without running
    __init__ (which would attempt network and SMTP I/O).
    """
    creds = credentials or {
        'PO': 'LEGACY-PO',
        'smtp_recipient': 'legacy@example.com',
    }
    (data_dir / 'credentials.json').write_text(json.dumps(creds))
    linde_manager._DATADIR = str(data_dir)

    link = object.__new__(linde_manager.LindeLink)
    link.credentials = creds
    link.last_alert_file = str(data_dir / 'last_alert.log')
    link.log_file = str(data_dir / 'data_log.csv')

    if pos is not None:
        (data_dir / 'pos.json').write_text(json.dumps({'pos': pos}))
    link.load_pos()

    if log_lines:
        with open(link.last_alert_file, 'w') as f:
            for line in log_lines:
                f.write(line.rstrip('\n') + '\n')

    linde_manager.link = link
    return link


@pytest.fixture
def make_link(tmp_path):
    """Factory fixture: build a LindeLink against a fresh temp data dir."""
    def _factory(pos=None, log_lines=None, credentials=None):
        return _make_link(tmp_path, pos=pos, log_lines=log_lines, credentials=credentials)
    return _factory
