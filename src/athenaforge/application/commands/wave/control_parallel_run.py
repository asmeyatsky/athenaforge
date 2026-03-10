from __future__ import annotations

from dataclasses import replace

from athenaforge.application.dtos.wave_dtos import ParallelRunResult
from athenaforge.domain.events.wave_events import ParallelRunModeChanged
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.ports.repository_ports import WaveRepositoryPort
from athenaforge.domain.services.parallel_running_state_machine import (
    ParallelRunningStateMachine,
)
from athenaforge.domain.value_objects.wave import ParallelRunMode


class ControlParallelRunUseCase:
    """Transition a wave's parallel-run mode using the state machine."""

    def __init__(
        self,
        state_machine: ParallelRunningStateMachine,
        wave_repo: WaveRepositoryPort,
        event_bus: EventBusPort,
    ) -> None:
        self._state_machine = state_machine
        self._wave_repo = wave_repo
        self._event_bus = event_bus

    async def execute(self, wave_id: str, target_mode: str) -> ParallelRunResult:
        wave = await self._wave_repo.get_by_id(wave_id)
        if wave is None:
            raise ValueError(f"Wave '{wave_id}' not found")

        previous_mode = wave.mode
        target = ParallelRunMode(target_mode)

        new_mode = self._state_machine.transition(previous_mode, target)

        updated_wave = replace(wave, mode=new_mode)
        await self._wave_repo.save(updated_wave)

        await self._event_bus.publish(
            ParallelRunModeChanged(
                aggregate_id=wave_id,
                wave_id=wave_id,
                old_mode=previous_mode.value,
                new_mode=new_mode.value,
            )
        )

        return ParallelRunResult(
            wave_id=wave_id,
            previous_mode=previous_mode.value,
            current_mode=new_mode.value,
            success=True,
        )
