from ledger.derive import (COMPLETED, IN_PROGRESS_DIALED, IN_PROGRESS_TELEPHONY_FAILED,
                           YET_TO_REACH_OUT, Call, effective_disposition, lead_bucket)

TERMINAL = {"renewed", "not_interested", "dnd", "wrong_number"}
is_terminal = lambda code: code in TERMINAL


def test_no_events_no_disposition():
    assert effective_disposition([], []) is None


def test_latest_set_wins_without_override():
    sets = [(100, "e1", "payment_link_sent"), (300, "e3", "renewed"), (200, "e2", "callback_requested")]
    assert effective_disposition(sets, []) == "renewed"


def test_override_beats_a_later_set():
    sets = [(100, "e1", "renewed"), (900, "e9", "callback_requested")]
    overrides = [(500, "e5", "not_interested")]
    assert effective_disposition(sets, overrides) == "not_interested"


def test_latest_override_wins_among_overrides():
    overrides = [(500, "e5", "not_interested"), (700, "e7", "renewed"), (600, "e6", "dnd")]
    assert effective_disposition([(1, "e1", "callback_requested")], overrides) == "renewed"


def test_override_alone_is_enough():
    assert effective_disposition([], [(5, "e", "dnd")]) == "dnd"


def test_tie_is_broken_deterministically_by_event_id():
    a = effective_disposition([(10, "evt_b", "renewed"), (10, "evt_a", "dnd")], [])
    b = effective_disposition([(10, "evt_a", "dnd"), (10, "evt_b", "renewed")], [])
    assert a == b == "renewed"


def test_input_events_are_not_mutated():
    sets = [(1, "a", "renewed")]
    overrides = [(2, "b", "dnd")]
    effective_disposition(sets, overrides)
    assert sets == [(1, "a", "renewed")] and overrides == [(2, "b", "dnd")]


def call(i, failed=False, disp=None, connected=False):
    return Call(f"c{i}", i, connected, failed, disp)


def test_bucket_no_calls():
    assert lead_bucket([], is_terminal) == YET_TO_REACH_OUT


def test_bucket_terminal_beats_everything():
    calls = [call(1, failed=True), call(2, disp="renewed", connected=True)]
    assert lead_bucket(calls, is_terminal) == COMPLETED


def test_bucket_terminal_beats_telephony_failed_on_same_call():
    assert lead_bucket([call(1, failed=True, disp="dnd")], is_terminal) == COMPLETED


def test_bucket_non_terminal_disposition_is_not_completed():
    calls = [call(1, disp="callback_requested", connected=True)]
    assert lead_bucket(calls, is_terminal) == IN_PROGRESS_DIALED


def test_bucket_all_calls_failed():
    assert lead_bucket([call(1, failed=True), call(2, failed=True)], is_terminal) == IN_PROGRESS_TELEPHONY_FAILED


def test_bucket_one_failed_one_dialed_is_dialed():
    assert lead_bucket([call(1, failed=True), call(2)], is_terminal) == IN_PROGRESS_DIALED


def test_bucket_unanswered_calls_are_dialed():
    assert lead_bucket([call(1)], is_terminal) == IN_PROGRESS_DIALED
