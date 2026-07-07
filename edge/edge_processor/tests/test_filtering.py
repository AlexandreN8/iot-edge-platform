from filtering import is_valid_json, is_duplicate

def test_valid_json_accepted():
    assert is_valid_json(b'{"a": 1}') is True

def test_invalid_json_rejected():
    assert is_valid_json(b'not json') is False

def test_first_message_not_duplicate():
    assert is_duplicate(b'{"unique": "first"}') is False

def test_exact_repeat_is_duplicate():
    payload = b'{"unique": "repeat-test"}'
    is_duplicate(payload)  # première fois, l'enregistre
    assert is_duplicate(payload) is True