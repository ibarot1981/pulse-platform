from __future__ import annotations


def parse_process_seq(seq_string: str) -> list[str]:
    if not seq_string:
        return []
    stages = [token.strip() for token in str(seq_string).split(" - ")]
    return [stage for stage in stages if stage]


def next_stage_from_index(stages: list[str], current_stage_index: int) -> tuple[int, str | None]:
    next_index = int(current_stage_index) + 1
    if next_index < 0 or next_index >= len(stages):
        return next_index, None
    return next_index, stages[next_index]

