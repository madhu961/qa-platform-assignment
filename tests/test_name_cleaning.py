from src.utils import clean_corporate_name


def test_clean_corporate_name_removes_suffixes():
    assert clean_corporate_name("Acme Corporation") == "acme"
    assert clean_corporate_name("Acme Corp.") == "acme"
    assert clean_corporate_name("Acme, Inc.") == "acme"
    assert clean_corporate_name("Acme LLC") == "acme"
