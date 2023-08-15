from src.dataprep.helpers.functions import tupelize_links

import pytest



def test_tupelize():
    # define a list of links to which we add the iteration ID
    links = [[[219849812498, 1249821], 0.9], [[2198498124982, 12498212], 0.95]]
    iteration_id = 1

    result = tupelize_links(links, iteration_id)
    first = next(result)
    second = next(result)
    assert first[0] == links[0][0][0]
    assert second[3] == iteration_id 

    with pytest.raises(StopIteration):
        third = next(result)
