from src.utils import normalize_address


def test_normalize_address():
    assert normalize_address("123 Main Street.") == "123 main st"
