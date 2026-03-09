"""Tests for ParallelRunningStateMachine — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.parallel_running_state_machine import (
    ParallelRunningStateMachine,
)
from athenaforge.domain.value_objects.wave import ParallelRunMode


class TestValidForwardTransitions:
    def test_old_only_to_shadow(self):
        sm = ParallelRunningStateMachine()
        result = sm.transition(ParallelRunMode.OLD_ONLY, ParallelRunMode.SHADOW)

        assert result == ParallelRunMode.SHADOW

    def test_shadow_to_reverse_shadow(self):
        sm = ParallelRunningStateMachine()
        result = sm.transition(ParallelRunMode.SHADOW, ParallelRunMode.REVERSE_SHADOW)

        assert result == ParallelRunMode.REVERSE_SHADOW

    def test_reverse_shadow_to_new_only(self):
        sm = ParallelRunningStateMachine()
        result = sm.transition(
            ParallelRunMode.REVERSE_SHADOW, ParallelRunMode.NEW_ONLY
        )

        assert result == ParallelRunMode.NEW_ONLY


class TestRollbackTransitions:
    def test_shadow_to_old_only(self):
        sm = ParallelRunningStateMachine()
        result = sm.transition(ParallelRunMode.SHADOW, ParallelRunMode.OLD_ONLY)

        assert result == ParallelRunMode.OLD_ONLY

    def test_reverse_shadow_to_old_only(self):
        sm = ParallelRunningStateMachine()
        result = sm.transition(
            ParallelRunMode.REVERSE_SHADOW, ParallelRunMode.OLD_ONLY
        )

        assert result == ParallelRunMode.OLD_ONLY

    def test_new_only_to_old_only(self):
        sm = ParallelRunningStateMachine()
        result = sm.transition(ParallelRunMode.NEW_ONLY, ParallelRunMode.OLD_ONLY)

        assert result == ParallelRunMode.OLD_ONLY


class TestInvalidTransitions:
    def test_old_only_to_reverse_shadow_raises(self):
        sm = ParallelRunningStateMachine()

        with pytest.raises(ValueError, match="Invalid transition"):
            sm.transition(
                ParallelRunMode.OLD_ONLY, ParallelRunMode.REVERSE_SHADOW
            )

    def test_old_only_to_new_only_raises(self):
        sm = ParallelRunningStateMachine()

        with pytest.raises(ValueError, match="Invalid transition"):
            sm.transition(ParallelRunMode.OLD_ONLY, ParallelRunMode.NEW_ONLY)

    def test_shadow_to_new_only_raises(self):
        sm = ParallelRunningStateMachine()

        with pytest.raises(ValueError, match="Invalid transition"):
            sm.transition(ParallelRunMode.SHADOW, ParallelRunMode.NEW_ONLY)

    def test_old_only_to_old_only_raises(self):
        sm = ParallelRunningStateMachine()

        with pytest.raises(ValueError, match="Invalid transition"):
            sm.transition(ParallelRunMode.OLD_ONLY, ParallelRunMode.OLD_ONLY)

    def test_new_only_to_shadow_raises(self):
        sm = ParallelRunningStateMachine()

        with pytest.raises(ValueError, match="Invalid transition"):
            sm.transition(ParallelRunMode.NEW_ONLY, ParallelRunMode.SHADOW)

    def test_new_only_to_reverse_shadow_raises(self):
        sm = ParallelRunningStateMachine()

        with pytest.raises(ValueError, match="Invalid transition"):
            sm.transition(
                ParallelRunMode.NEW_ONLY, ParallelRunMode.REVERSE_SHADOW
            )


class TestCanTransition:
    def test_can_transition_forward(self):
        sm = ParallelRunningStateMachine()

        assert sm.can_transition(ParallelRunMode.OLD_ONLY, ParallelRunMode.SHADOW) is True
        assert sm.can_transition(ParallelRunMode.SHADOW, ParallelRunMode.REVERSE_SHADOW) is True
        assert sm.can_transition(ParallelRunMode.REVERSE_SHADOW, ParallelRunMode.NEW_ONLY) is True

    def test_can_transition_rollback(self):
        sm = ParallelRunningStateMachine()

        assert sm.can_transition(ParallelRunMode.SHADOW, ParallelRunMode.OLD_ONLY) is True
        assert sm.can_transition(ParallelRunMode.REVERSE_SHADOW, ParallelRunMode.OLD_ONLY) is True
        assert sm.can_transition(ParallelRunMode.NEW_ONLY, ParallelRunMode.OLD_ONLY) is True

    def test_cannot_transition_to_same_state(self):
        sm = ParallelRunningStateMachine()

        for mode in ParallelRunMode:
            assert sm.can_transition(mode, mode) is False

    def test_cannot_skip_forward(self):
        sm = ParallelRunningStateMachine()

        assert sm.can_transition(ParallelRunMode.OLD_ONLY, ParallelRunMode.REVERSE_SHADOW) is False
        assert sm.can_transition(ParallelRunMode.OLD_ONLY, ParallelRunMode.NEW_ONLY) is False
        assert sm.can_transition(ParallelRunMode.SHADOW, ParallelRunMode.NEW_ONLY) is False


class TestGetValidTransitions:
    def test_old_only_valid_transitions(self):
        sm = ParallelRunningStateMachine()
        valid = sm.get_valid_transitions(ParallelRunMode.OLD_ONLY)

        assert valid == [ParallelRunMode.SHADOW]

    def test_shadow_valid_transitions(self):
        sm = ParallelRunningStateMachine()
        valid = sm.get_valid_transitions(ParallelRunMode.SHADOW)

        assert ParallelRunMode.REVERSE_SHADOW in valid
        assert ParallelRunMode.OLD_ONLY in valid
        assert len(valid) == 2

    def test_reverse_shadow_valid_transitions(self):
        sm = ParallelRunningStateMachine()
        valid = sm.get_valid_transitions(ParallelRunMode.REVERSE_SHADOW)

        assert ParallelRunMode.NEW_ONLY in valid
        assert ParallelRunMode.OLD_ONLY in valid
        assert len(valid) == 2

    def test_new_only_valid_transitions(self):
        sm = ParallelRunningStateMachine()
        valid = sm.get_valid_transitions(ParallelRunMode.NEW_ONLY)

        # NEW_ONLY has no forward transition, only rollback
        assert valid == [ParallelRunMode.OLD_ONLY]
