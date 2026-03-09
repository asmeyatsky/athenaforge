from __future__ import annotations

from athenaforge.domain.value_objects.wave import ParallelRunMode

# Forward transitions (happy path)
_FORWARD_TRANSITIONS: dict[ParallelRunMode, ParallelRunMode] = {
    ParallelRunMode.OLD_ONLY: ParallelRunMode.SHADOW,
    ParallelRunMode.SHADOW: ParallelRunMode.REVERSE_SHADOW,
    ParallelRunMode.REVERSE_SHADOW: ParallelRunMode.NEW_ONLY,
}

# Every state can rollback to OLD_ONLY
_ROLLBACK_TARGET = ParallelRunMode.OLD_ONLY


class ParallelRunningStateMachine:
    """Pure domain service enforcing valid parallel-run state transitions."""

    def can_transition(
        self, current: ParallelRunMode, target: ParallelRunMode
    ) -> bool:
        """Return ``True`` if transitioning from *current* to *target* is valid."""
        if current == target:
            return False
        # Rollback to OLD_ONLY is always valid from any state (except OLD_ONLY itself)
        if target == _ROLLBACK_TARGET:
            return current != _ROLLBACK_TARGET
        # Forward transition
        return _FORWARD_TRANSITIONS.get(current) == target

    def transition(
        self, current: ParallelRunMode, target: ParallelRunMode
    ) -> ParallelRunMode:
        """Validate and return *target* if the transition is legal.

        Raises ``ValueError`` for invalid transitions.
        """
        if not self.can_transition(current, target):
            raise ValueError(
                f"Invalid transition: {current.value} → {target.value}"
            )
        return target

    def get_valid_transitions(
        self, current: ParallelRunMode
    ) -> list[ParallelRunMode]:
        """Return all states reachable from *current*."""
        valid: list[ParallelRunMode] = []
        # Forward transition
        forward = _FORWARD_TRANSITIONS.get(current)
        if forward is not None:
            valid.append(forward)
        # Rollback (if not already at OLD_ONLY)
        if current != _ROLLBACK_TARGET:
            valid.append(_ROLLBACK_TARGET)
        return valid
