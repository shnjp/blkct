from blkct.utils import make_new_session_id


def test_make_new_session_id():
    a = make_new_session_id()
    assert a and isinstance(a, str)

    b = make_new_session_id()
    assert b and isinstance(b, str)

    assert a != b
