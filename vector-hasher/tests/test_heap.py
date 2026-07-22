from app.heap import flatten_sorted, push_with_eviction

K = 3


def test_pushing_fewer_than_k_entries_always_inserts() -> None:
    heap: list[tuple[float, str]] = []

    changed = push_with_eviction(heap, (0.5, "A"), K)

    assert changed is True
    assert len(heap) == 1


def test_pushing_a_closer_entry_once_full_evicts_only_the_farthest() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)
    push_with_eviction(heap, (0.9, "C"), K)

    changed = push_with_eviction(heap, (0.3, "D"), K)

    assert changed is True
    assert flatten_sorted(heap) == [(0.1, "A"), (0.3, "D"), (0.5, "B")]


def test_pushing_a_farther_entry_once_full_is_a_no_op() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)
    push_with_eviction(heap, (0.9, "C"), K)

    changed = push_with_eviction(heap, (1.5, "Z"), K)

    assert changed is False
    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B"), (0.9, "C")]


def test_flatten_sorted_returns_closest_first_order() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.9, "C"), K)
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)

    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B"), (0.9, "C")]


def test_pushing_an_entity_already_in_the_heap_is_a_no_op_not_a_duplicate() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)

    changed = push_with_eviction(heap, (0.5, "B"), K)

    assert changed is False
    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B")]


def test_pushing_an_entity_already_in_a_full_heap_does_not_evict_anything() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)
    push_with_eviction(heap, (0.9, "C"), K)

    changed = push_with_eviction(heap, (0.9, "C"), K)

    assert changed is False
    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B"), (0.9, "C")]
