import pytest
from src.quality_checks import DataQualityException


def test_placeholder():
    with pytest.raises(DataQualityException):
        raise DataQualityException("bad contract")
