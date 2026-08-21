# =============================================================================
# COMMS Service -- The test-database guard (T-78)
# =============================================================================
#
# The guard exists because the suite deletes every row before every
# test. Pointed at the live database, it empties it. So what is checked
# here is not a validator's edge cases -- it is whether a wrong URL can
# get past.
#
# BOTH HALVES, DELIBERATELY. A file that only proved "bad names are
# refused" would pass just as well if the guard refused EVERYTHING, and
# a suite that refuses to run is a suite nobody runs -- which is how the
# guard would quietly be deleted a month later. So every refusal here
# has a twin that must be let through.
#
# The rule is exercised through require_test_database() with URLs built
# in the test, not through the ambient configuration: the interesting
# inputs (an unreadable URL, a URL with no database at all) cannot be
# produced by the machine this happens to run on.
# =============================================================================

import pytest

from tests.conftest import TEST_DB_SUFFIX, require_test_database

_LIVE = "postgresql+asyncpg://comms:comms@localhost:5432/comms"
_TEST = "postgresql+asyncpg://comms:comms@localhost:5432/comms_test"


class TestNamesThatAreLetThrough:
    def test_the_test_database_passes(self) -> None:
        assert require_test_database(_TEST) == "comms_test"

    def test_query_parameters_do_not_confuse_the_name(self) -> None:
        """A URL carrying options is still a URL: the name is a field,
        not the tail of a string."""
        assert (
            require_test_database(f"{_TEST}?ssl=require&connect_timeout=5")
            == "comms_test"
        )

    def test_a_password_containing_the_database_name_is_not_a_problem(
        self,
    ) -> None:
        """The reason the guard parses instead of matching substrings --
        and the same reason the deploy command strips a suffix rather
        than replacing text."""
        url = "postgresql+asyncpg://comms:comms_prod@db.internal:5432/comms_test"
        assert require_test_database(url) == "comms_test"

    def test_any_host_and_driver_are_accepted(self) -> None:
        """The contract is the NAME. Where the database lives is not this
        guard's business -- a remote test database is still a test
        database."""
        assert (
            require_test_database("postgresql://u:p@10.0.0.9:6543/scratch_test")
            == "scratch_test"
        )


class TestNamesThatAreRefused:
    def test_the_live_database_is_refused(self) -> None:
        with pytest.raises(pytest.UsageError):
            require_test_database(_LIVE)

    def test_the_refusal_names_the_database_it_refused(self) -> None:
        """An operator who sees this message has just been told the
        suite would have wiped something; they need to know WHAT, and
        whether it was the one they thought."""
        with pytest.raises(pytest.UsageError) as caught:
            require_test_database(_LIVE)

        message = str(caught.value)
        assert "'comms'" in message
        assert TEST_DB_SUFFIX in message

    def test_a_name_that_merely_contains_the_suffix_is_refused(self) -> None:
        """`comms_test_backup` is not a test database, and a substring
        check would have said it was."""
        with pytest.raises(pytest.UsageError):
            require_test_database(f"{_TEST}_backup")

    def test_a_url_with_no_database_at_all_is_refused(self) -> None:
        """Emptiness axis: no name is not a passing name."""
        with pytest.raises(pytest.UsageError):
            require_test_database(
                "postgresql+asyncpg://comms:comms@localhost:5432/"
            )

    def test_an_unreadable_url_is_refused_not_skipped(self) -> None:
        """The one path that could have bypassed the check: an exception
        on the way to reading the name. It must end in a refusal, not in
        a traceback that some caller catches."""
        with pytest.raises(pytest.UsageError):
            require_test_database("this is not a url")

    def test_an_empty_url_is_refused(self) -> None:
        with pytest.raises(pytest.UsageError):
            require_test_database("")
