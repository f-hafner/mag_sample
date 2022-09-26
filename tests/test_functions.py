
from src.dataprep.helpers.functions import list_from_tuples

import pytest


def test_list_from_tuples():
    a = (3, "some string")
    b = ((4, "another string"), (6, "another string2"))
    e = (a,)
    assert list_from_tuples(a) == ["some string"]
    assert list_from_tuples(b) == ["another string", "another string2"]
    assert list_from_tuples(e) == ["some string"]
    c = (4, 5)
    with pytest.raises(TypeError, match="(int, str)"):
        list_from_tuples(c)
    d = (a, c)
    with pytest.raises(TypeError, match="need to be strings"):
        list_from_tuples(d)


