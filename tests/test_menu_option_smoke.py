import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

from pulse.integrations import production
from pulse.menu.submenu import MAIN_STATE


class _DummyMessage:
    def __init__(self, text: str):
        self.text = text
        self.replies: list[tuple[str, object]] = []

    async def reply_text(self, text: str, reply_markup=None):
        self.replies.append((text, reply_markup))


class _DummyUpdate:
    def __init__(self, text: str):
        self.effective_message = _DummyMessage(text)
        self.effective_chat = type("Chat", (), {"id": 1})()


class _DummyContext:
    def __init__(self, user_data: dict):
        self.user_data = user_data
        self.bot = object()


class _FakeRepo:
    def get_role_name_by_user_id(self, user_id: str) -> str:
        return "SupervisorA"

    def get_costing_user_ref_by_user_id(self, user_id: str):
        return 1

    def list_pending_approvals(self):
        return []


class MenuOptionSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_production_menu_options_do_not_raise(self):
        base_user = {"user_id": "U1"}
        base_row = {"id": 1, "fields": {"batch_id": 10, "next_stage_name": "WELDING"}}
        common_patches = (
            patch("pulse.integrations.production._reply", new=AsyncMock()),
            patch("pulse.integrations.production._show_model_page", new=AsyncMock()),
            patch("pulse.integrations.production._show_parts_page", new=AsyncMock()),
            patch("pulse.integrations.production._show_pending_approvals_page", new=AsyncMock()),
            patch("pulse.integrations.production._show_pending_approval_confirmation", new=AsyncMock()),
            patch("pulse.integrations.production._show_my_ms_jobs_page", new=AsyncMock()),
            patch("pulse.integrations.production._show_my_ms_jobs_filter_menu", new=AsyncMock()),
            patch("pulse.integrations.production._show_my_ms_schedule_page", new=AsyncMock()),
            patch("pulse.integrations.production._show_my_ms_jobs_next_stage_filter_page", new=AsyncMock()),
            patch("pulse.integrations.production._show_my_ms_jobs_created_by_filter_page", new=AsyncMock()),
            patch("pulse.integrations.production._move_model_selection", new=AsyncMock()),
            patch("pulse.integrations.production._move_part_selection", new=AsyncMock()),
            patch("pulse.integrations.production._show_batch_type_prompt", new=AsyncMock()),
            patch("pulse.integrations.production.start_new_production_batch", new=AsyncMock()),
            patch("pulse.integrations.production._create_batch_from_flow", new=AsyncMock()),
            patch("pulse.integrations.production._prompt_schedule_date", new=AsyncMock()),
            patch("pulse.integrations.production._mark_ms_stage_done_pending_confirmation", new=AsyncMock()),
            patch("pulse.integrations.production._execute_ms_job_action", new=AsyncMock(return_value=True)),
            patch("pulse.integrations.production._show_ms_job_action_menu", new=AsyncMock()),
            patch("pulse.integrations.production._refresh_my_ms_jobs_selection", return_value=[base_row]),
            patch("pulse.integrations.production.approve_batches_by_ids", new=AsyncMock(return_value=["B1"])),
            patch("pulse.integrations.production.reject_batches_by_ids", new=AsyncMock(return_value=["B1"])),
            patch("pulse.integrations.production._is_batch_schedulable_for_role", return_value=True),
            patch("pulse.integrations.production.set_master_scheduled_date", new=AsyncMock()),
            patch("pulse.integrations.production.ProductionRepo", return_value=_FakeRepo()),
        )

        cases = [
            (production.SELECTING_BATCH_MODE_STATE, production._MODE_BY_MODEL, {}),
            (production.SELECTING_BATCH_MODE_STATE, production._MODE_BY_PART, {}),
            (production.SELECTING_BATCH_MODE_STATE, production.BACK_LABEL, {}),
            (
                production.SELECTING_PRODUCT_MODEL_STATE,
                production._PAGE_PREV,
                {"production_paging": {"records": ["M1"], "page": 0, "page_size": 5}},
            ),
            (
                production.SELECTING_PRODUCT_MODEL_STATE,
                production._PAGE_NEXT,
                {"production_paging": {"records": ["M1", "M2"], "page": 0, "page_size": 1}},
            ),
            (
                production.SELECTING_PRODUCT_MODEL_STATE,
                production.BACK_LABEL,
                {"production_paging": {"records": ["M1"], "page": 0, "page_size": 5}},
            ),
            (
                production.SELECTING_PRODUCT_PARTS_STATE,
                production._YES,
                {"production_batch_flow": {"awaiting_more_parts_answer": True}},
            ),
            (
                production.SELECTING_PRODUCT_PARTS_STATE,
                production._NO,
                {"production_batch_flow": {"awaiting_more_parts_answer": True, "selected_part_ids": [1]}},
            ),
            (
                production.SELECTING_PRODUCT_PARTS_STATE,
                production.BACK_LABEL,
                {"production_batch_flow": {"awaiting_more_parts_answer": True}},
            ),
            (
                production.ENTERING_BATCH_QTY_STATE,
                production.BACK_LABEL,
                {"production_batch_flow": {"batch_mode": production._MODE_BY_PART}},
            ),
            (
                production.ENTERING_BATCH_QTY_STATE,
                production.BACK_LABEL,
                {"production_batch_flow": {"batch_mode": production._MODE_BY_MODEL}},
            ),
            (production.CONFIRMING_BATCH_STATE, production.BACK_LABEL, {}),
            (production.CONFIRMING_BATCH_STATE, production._NO, {}),
            (production.CONFIRMING_BATCH_STATE, production._YES, {}),
            (production.SELECTING_BATCH_TYPE_STATE, production._TYPE_COMPLETE, {}),
            (production.SELECTING_BATCH_TYPE_STATE, production._TYPE_MS, {}),
            (production.SELECTING_BATCH_TYPE_STATE, production._TYPE_CNC, {}),
            (production.SELECTING_BATCH_TYPE_STATE, production._TYPE_STORE, {}),
            (production.SELECTING_BATCH_TYPE_STATE, production.BACK_LABEL, {}),
            (
                production.PENDING_APPROVALS_SELECTION_STATE,
                production._PAGE_PREV,
                {"pending_approvals_selection": {"records": [{"id": 1}], "page": 0, "page_size": 5}},
            ),
            (
                production.PENDING_APPROVALS_SELECTION_STATE,
                production._PAGE_NEXT,
                {"pending_approvals_selection": {"records": [{"id": 1}, {"id": 2}], "page": 0, "page_size": 1}},
            ),
            (
                production.PENDING_APPROVALS_SELECTION_STATE,
                production.BACK_LABEL,
                {"pending_approvals_selection": {"records": [{"id": 1}], "page": 0, "page_size": 5}},
            ),
            (
                production.PENDING_APPROVALS_CONFIRM_STATE,
                production._YES,
                {
                    "pending_approvals_confirm": {"selected_ids": [1]},
                    "pending_approvals_selection": {"records": [], "page": 0},
                },
            ),
            (
                production.PENDING_APPROVALS_CONFIRM_STATE,
                production._REJECT,
                {
                    "pending_approvals_confirm": {"selected_ids": [1]},
                    "pending_approvals_selection": {"records": [], "page": 0},
                },
            ),
            (production.PENDING_APPROVALS_CONFIRM_STATE, production._NO, {"pending_approvals_confirm": {}}),
            (production.PENDING_APPROVALS_CONFIRM_STATE, production.BACK_LABEL, {"pending_approvals_confirm": {}}),
            (
                production.MY_MS_JOBS_FILTER_STATE,
                production._MS_VIEW_ALL,
                {"my_ms_jobs_all_records": [base_row], "my_ms_jobs_creator_by_batch": {10: "Sup A"}},
            ),
            (
                production.MY_MS_JOBS_FILTER_STATE,
                production._MS_VIEW_BY_NEXT_STAGE,
                {"my_ms_jobs_all_records": [base_row], "my_ms_jobs_creator_by_batch": {10: "Sup A"}},
            ),
            (
                production.MY_MS_JOBS_FILTER_STATE,
                production._MS_VIEW_BY_CREATED_BY,
                {"my_ms_jobs_all_records": [base_row], "my_ms_jobs_creator_by_batch": {10: "Sup A"}},
            ),
            (production.MY_MS_JOBS_FILTER_STATE, production.BACK_LABEL, {"my_ms_jobs_all_records": [base_row]}),
            (
                production.MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE,
                production._PAGE_PREV,
                {
                    "my_ms_jobs_next_stage_selection": {"options": ["WELDING"], "page": 0, "page_size": 5},
                    "my_ms_jobs_all_records": [base_row],
                    "my_ms_jobs_creator_by_batch": {10: "Sup A"},
                },
            ),
            (
                production.MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE,
                production.BACK_LABEL,
                {"my_ms_jobs_next_stage_selection": {"options": ["WELDING"], "page": 0, "page_size": 5}},
            ),
            (
                production.MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE,
                "1",
                {
                    "my_ms_jobs_next_stage_selection": {"options": ["WELDING"], "page": 0, "page_size": 5},
                    "my_ms_jobs_all_records": [base_row],
                    "my_ms_jobs_creator_by_batch": {10: "Sup A"},
                },
            ),
            (
                production.MY_MS_JOBS_CREATED_BY_SELECTION_STATE,
                production._PAGE_PREV,
                {
                    "my_ms_jobs_created_by_selection": {"options": ["Sup A"], "page": 0, "page_size": 5},
                    "my_ms_jobs_all_records": [base_row],
                    "my_ms_jobs_creator_by_batch": {10: "Sup A"},
                },
            ),
            (
                production.MY_MS_JOBS_CREATED_BY_SELECTION_STATE,
                production.BACK_LABEL,
                {"my_ms_jobs_created_by_selection": {"options": ["Sup A"], "page": 0, "page_size": 5}},
            ),
            (
                production.MY_MS_JOBS_CREATED_BY_SELECTION_STATE,
                "1",
                {
                    "my_ms_jobs_created_by_selection": {"options": ["Sup A"], "page": 0, "page_size": 5},
                    "my_ms_jobs_all_records": [base_row],
                    "my_ms_jobs_creator_by_batch": {10: "Sup A"},
                },
            ),
            (
                production.MY_MS_SCHEDULE_SELECTION_STATE,
                production._PAGE_PREV,
                {"my_ms_schedule_selection": {"records": [{"batch_id": 10}], "page": 0, "page_size": 5}},
            ),
            (
                production.MY_MS_SCHEDULE_SELECTION_STATE,
                production._PAGE_NEXT,
                {"my_ms_schedule_selection": {"records": [{"batch_id": 10}, {"batch_id": 11}], "page": 0, "page_size": 1}},
            ),
            (
                production.MY_MS_SCHEDULE_SELECTION_STATE,
                production.BACK_LABEL,
                {"my_ms_schedule_selection": {"records": [{"batch_id": 10}], "page": 0, "page_size": 5}},
            ),
            (
                production.MY_MS_SCHEDULE_CONFIRM_STATE,
                production._YES,
                {"my_ms_schedule_confirm": {"selected_batch_ids": [10]}},
            ),
            (production.MY_MS_SCHEDULE_CONFIRM_STATE, production._NO, {"my_ms_schedule_confirm": {}}),
            (production.MY_MS_SCHEDULE_CONFIRM_STATE, production.BACK_LABEL, {"my_ms_schedule_confirm": {}}),
            (
                production.MY_MS_JOBS_SELECTION_STATE,
                production._PAGE_PREV,
                {"my_ms_jobs_selection": {"records": [base_row], "page": 0, "page_size": 5}},
            ),
            (
                production.MY_MS_JOBS_SELECTION_STATE,
                production._PAGE_NEXT,
                {"my_ms_jobs_selection": {"records": [base_row, base_row], "page": 0, "page_size": 1}},
            ),
            (
                production.MY_MS_JOBS_SELECTION_STATE,
                production.BACK_LABEL,
                {"my_ms_jobs_selection": {"records": [base_row], "page": 0, "page_size": 5}},
            ),
            (
                production.MY_MS_JOBS_ACTION_STATE,
                production._MS_ACTION_DONE,
                {"my_ms_jobs_action": {"selected_record": {"id": 1, "fields": {"batch_id": 10}}}, "user": base_user},
            ),
            (
                production.MY_MS_JOBS_ACTION_STATE,
                production._MS_ACTION_REMARKS,
                {"my_ms_jobs_action": {"selected_record": {"id": 1, "fields": {"batch_id": 10}}}, "user": base_user},
            ),
            (
                production.MY_MS_JOBS_ACTION_STATE,
                production._MS_ACTION_VIEW_LIST,
                {"my_ms_jobs_action": {"selected_record": {"id": 1, "fields": {"batch_id": 10}}}, "user": base_user},
            ),
            (
                production.MY_MS_JOBS_ACTION_STATE,
                production._MS_ACTION_HOLD,
                {"my_ms_jobs_action": {"selected_record": {"id": 1, "fields": {"batch_id": 10}}}, "user": base_user},
            ),
            (
                production.MY_MS_JOBS_ACTION_STATE,
                production.BACK_LABEL,
                {"my_ms_jobs_action": {"selected_record": {"id": 1, "fields": {"batch_id": 10}}}},
            ),
            (
                production.MY_MS_JOBS_CONFIRM_STATE,
                production._YES,
                {"my_ms_jobs_confirm": {"selected_ids": [1]}, "user": base_user},
            ),
            (production.MY_MS_JOBS_CONFIRM_STATE, production._NO, {"my_ms_jobs_confirm": {}}),
            (production.MY_MS_JOBS_CONFIRM_STATE, production.BACK_LABEL, {"my_ms_jobs_confirm": {}}),
            (production.MY_MS_JOBS_REMARKS_STATE, production.BACK_LABEL, {"my_ms_jobs_remarks": {"row_id": 1}}),
            (
                production.AWAITING_SCHEDULE_DATE_STATE,
                production._TODAY,
                {
                    "schedule_date_context": {"batch_ids": [10], "return_state": MAIN_STATE},
                    "user": base_user,
                },
            ),
            (
                production.AWAITING_SCHEDULE_DATE_STATE,
                production._TOMORROW,
                {
                    "schedule_date_context": {"batch_ids": [10], "return_state": MAIN_STATE},
                    "user": base_user,
                },
            ),
            (
                production.AWAITING_SCHEDULE_DATE_STATE,
                production.BACK_LABEL,
                {"schedule_date_context": {"batch_ids": [10], "return_state": MAIN_STATE}, "user": base_user},
            ),
        ]

        with ExitStack() as stack:
            for patcher in common_patches:
                stack.enter_context(patcher)
            for state, text, extra_user_data in cases:
                user_data = {"menu_state": state, **extra_user_data}
                context = _DummyContext(user_data)
                update = _DummyUpdate(text)
                handled = await production.handle_production_state_text(update, context, text)
                self.assertIn(handled, (True, False))


if __name__ == "__main__":
    unittest.main()
