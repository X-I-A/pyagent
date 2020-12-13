from pyagent.agent import Agent

def test_age_list():
    age_list = []
    assert not Agent._age_list_point_in(age_list, 1)
    age_list = Agent._age_list_add_item(age_list, [1, 2])
    assert age_list == [[1, 2]]
    age_list = Agent._age_list_add_item(age_list, [9, 10])
    assert age_list == [[1, 2], [9, 10]]
    age_list = Agent._age_list_add_item(age_list, [4, 5])
    assert age_list == [[1, 2], [4, 5], [9, 10]]
    assert Agent._age_list_set_start(age_list, 1) == [[1, 2], [4, 5], [9, 10]]
    age_list = Agent._age_list_set_start(age_list, 5)
    assert age_list == [[5, 5], [9, 10]]
    assert Agent._age_list_point_in(age_list, 5)
    assert Agent._age_list_point_in(age_list, 10)
    assert Agent._age_list_point_in(age_list, 9)
    assert not Agent._age_list_point_in(age_list, 8)
    age_list = Agent._age_list_add_item(age_list, [1, 10])
    assert age_list == [[5, 10]]