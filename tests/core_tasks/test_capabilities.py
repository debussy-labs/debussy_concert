import pytest

from debussy.core.tasks import (
    Capability,
    UnsupportedCapabilityError,
    check_capabilities,
)


def test_capability_values_are_stable():
    # Backends and configuration refer to capabilities by these strings.
    assert {capability.value for capability in Capability} == {
        "scheduling",
        "retries",
        "templating",
        "native_operator",
        "xcom",
    }
    assert Capability("retries") is Capability.RETRIES


def test_check_passes_when_everything_is_provided():
    check_capabilities(
        {Capability.RETRIES},
        {Capability.RETRIES, Capability.SCHEDULING},
        subject="motif X",
    )
    check_capabilities([], [], subject="motif X")


def test_check_reports_every_missing_capability():
    with pytest.raises(UnsupportedCapabilityError) as error:
        check_capabilities(
            [Capability.XCOM, Capability.SCHEDULING, Capability.RETRIES],
            [Capability.RETRIES],
            subject="BigQueryQueryJobMotif",
        )
    assert error.value.subject == "BigQueryQueryJobMotif"
    assert error.value.missing == {Capability.XCOM, Capability.SCHEDULING}
    assert str(error.value) == (
        "BigQueryQueryJobMotif needs capabilities the backend lacks:"
        " scheduling, xcom"
    )
