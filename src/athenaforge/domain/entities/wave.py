from __future__ import annotations

from dataclasses import dataclass, field, replace

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.transfer_events import DVTValidationCompleted
from athenaforge.domain.value_objects.wave import ParallelRunMode, WaveStatus


# ── valid transitions ───────────────────────────────────────────
_VALID_TRANSITIONS: dict[WaveStatus, set[WaveStatus]] = {
    WaveStatus.PLANNED: {WaveStatus.SHADOW_RUNNING},
    WaveStatus.SHADOW_RUNNING: {WaveStatus.REVERSE_SHADOW_RUNNING, WaveStatus.ROLLED_BACK},
    WaveStatus.REVERSE_SHADOW_RUNNING: {WaveStatus.CUTOVER_READY, WaveStatus.ROLLED_BACK},
    WaveStatus.CUTOVER_READY: {WaveStatus.CUTTING_OVER, WaveStatus.ROLLED_BACK},
    WaveStatus.CUTTING_OVER: {WaveStatus.COMPLETED, WaveStatus.ROLLED_BACK},
    WaveStatus.COMPLETED: set(),
    WaveStatus.ROLLED_BACK: set(),
}


@dataclass(frozen=True)
class Wave:
    """Aggregate representing a migration wave that progresses through parallel-run stages."""

    wave_id: str
    name: str
    lob: str
    tables: tuple[str, ...]
    status: WaveStatus = WaveStatus.PLANNED
    mode: ParallelRunMode = ParallelRunMode.OLD_ONLY
    dvt_results: dict[str, bool] = field(default_factory=dict)
    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    # ── private helpers ─────────────────────────────────────────

    def _assert_transition(self, target: WaveStatus) -> None:
        """Raise *ValueError* if *target* is not a valid successor of the current status."""
        allowed = _VALID_TRANSITIONS.get(self.status, set())
        if target not in allowed:
            raise ValueError(
                f"Cannot transition from {self.status.value} to {target.value}"
            )

    # ── commands ────────────────────────────────────────────────

    def start_shadow_run(self) -> Wave:
        """Begin the shadow-run phase."""
        self._assert_transition(WaveStatus.SHADOW_RUNNING)
        return replace(self, status=WaveStatus.SHADOW_RUNNING, mode=ParallelRunMode.SHADOW)

    def advance_to_reverse_shadow(self) -> Wave:
        """Move from shadow to reverse-shadow phase."""
        self._assert_transition(WaveStatus.REVERSE_SHADOW_RUNNING)
        return replace(
            self,
            status=WaveStatus.REVERSE_SHADOW_RUNNING,
            mode=ParallelRunMode.REVERSE_SHADOW,
        )

    def cutover(self) -> Wave:
        """Initiate the final cutover."""
        self._assert_transition(WaveStatus.CUTTING_OVER)
        new_wave = replace(
            self,
            status=WaveStatus.CUTTING_OVER,
            mode=ParallelRunMode.NEW_ONLY,
        )
        return new_wave

    def rollback(self, reason: str) -> Wave:
        """Roll the wave back to the old system."""
        self._assert_transition(WaveStatus.ROLLED_BACK)
        rolled = replace(
            self,
            status=WaveStatus.ROLLED_BACK,
            mode=ParallelRunMode.OLD_ONLY,
        )
        return rolled

    def check_gate(self, criteria: dict[str, bool]) -> tuple[Wave, bool]:
        """Evaluate gate criteria. Returns the updated wave and whether the gate passed."""
        new_dvt = dict(self.dvt_results)
        new_dvt.update(criteria)
        passed = all(new_dvt.values())

        new_wave = replace(self, dvt_results=new_dvt)

        if passed:
            tables_validated = len(new_dvt)
            tables_passed = sum(1 for v in new_dvt.values() if v)
            tables_failed = tables_validated - tables_passed
            new_wave._events.append(
                DVTValidationCompleted(
                    aggregate_id=self.wave_id,
                    tier="",
                    tables_validated=tables_validated,
                    tables_passed=tables_passed,
                    tables_failed=tables_failed,
                )
            )

            # Auto-advance to CUTOVER_READY when gate passes from REVERSE_SHADOW_RUNNING
            if self.status == WaveStatus.REVERSE_SHADOW_RUNNING:
                new_wave = replace(new_wave, status=WaveStatus.CUTOVER_READY)

        return new_wave, passed

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> list[DomainEvent]:
        """Return accumulated events and clear the internal list."""
        events = list(self._events)
        self._events.clear()
        return events
