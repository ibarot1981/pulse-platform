import unittest
from unittest.mock import AsyncMock, patch

from pulse.integrations import production
from pulse.main import (
    AWAITING_APPROVAL_STATE,
    AWAITING_SCHEDULE_DATE_STATE,
    CONFIRMING_BATCH_STATE,
    ENTERING_BATCH_QTY_STATE,
    MY_MS_JOBS_ACTION_STATE,
    MY_MS_JOBS_CREATED_BY_SELECTION_STATE,
    MY_MS_JOBS_CONFIRM_STATE,
    MY_MS_JOBS_FILTER_STATE,
    MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE,
    MY_MS_JOBS_REMARKS_STATE,
    MY_MS_JOBS_SELECTION_STATE,
    MY_MS_SCHEDULE_CONFIRM_STATE,
    MY_MS_SCHEDULE_SELECTION_STATE,
    PENDING_APPROVALS_CONFIRM_STATE,
    PENDING_APPROVALS_SELECTION_STATE,
    SELECTING_BATCH_MODE_STATE,
    SELECTING_BATCH_TYPE_STATE,
    SELECTING_PRODUCT_MODEL_STATE,
    SELECTING_PRODUCT_PARTS_STATE,
    fallback_text,
)
from pulse.menu.submenu import MAIN_MENU_LABEL, MAIN_STATE


class _DummyMessage:
    def __init__(self, text: str):
        self.text = text
        self.replies: list[tuple[str, object]] = []

    async def reply_text(self, text: str, reply_markup=None):
        self.replies.append((text, reply_markup))


class _DummyUpdate:
    def __init__(self, text: str):
        self.effective_message = _DummyMessage(text)


class _DummyContext:
    def __init__(self, user_data: dict):
        self.user_data = user_data


class MenuStateRoutingTests(unittest.IsolatedAsyncioTestCase):
    def test_batch_created_renderer_skips_non_approver_roles(self):
        renderer = production._batch_created_recipient_renderer(9)

        approver_render = renderer({"role_id": "R01", "role_name": "Production_Manager"})
        non_approver_render = renderer({"role_id": "R03", "role_name": "Production_Supervisor"})

        self.assertIn("reply_markup", approver_render)
        self.assertIsNotNone(approver_render.get("reply_markup"))
        self.assertEqual(non_approver_render.get("skip"), True)

    def test_list_ms_jobs_for_user_role_includes_handoff_pending_for_visibility(self):
        class _FakeCostingClient:
            def get_records(self, table: str):
                if table == "ProductBatchMS":
                    return [
                        {
                            "id": 1,
                            "fields": {
                                "batch_id": 10,
                                "current_stage_name": "CUTTING",
                                "current_stage_role_name": "SupervisorA",
                                "current_status": "CUTTING Pending",
                                "product_part": "Part-A",
                            },
                        },
                        {
                            "id": 2,
                            "fields": {
                                "batch_id": 11,
                                "current_stage_name": "CUTTING",
                                "current_stage_role_name": "SupervisorA",
                                "current_status": "Cutting Completed",
                                "product_part": "Part-B",
                            },
                        },
                        {
                            "id": 3,
                            "fields": {
                                "batch_id": 12,
                                "current_stage_name": "WELDING",
                                "current_stage_role_name": "SupervisorA",
                                "current_status": "Done - Pending Confirmation",
                                "product_part": "Part-C",
                            },
                        },
                        {
                            "id": 4,
                            "fields": {
                                "batch_id": 13,
                                "current_stage_name": "CUTTING",
                                "current_stage_role_name": "SupervisorB",
                                "current_status": "CUTTING Pending",
                                "product_part": "Part-D",
                            },
                        },
                        {
                            "id": 5,
                            "fields": {
                                "batch_id": 14,
                                "current_stage_name": "CUTTING",
                                "current_stage_role_name": "SupervisorA",
                                "current_status": "CUTTING Pending",
                                "product_part": "Part-E",
                            },
                        },
                    ]
                return []

        class _FakeRepo:
            costing_client = _FakeCostingClient()

            def get_all_master_batches(self):
                return [
                    {"id": 10, "fields": {"batch_no": "B-10", "approval_status": "Approved"}},
                    {"id": 11, "fields": {"batch_no": "B-11", "approval_status": "Approved"}},
                    {"id": 12, "fields": {"batch_no": "B-12", "approval_status": "Approved"}},
                    {"id": 13, "fields": {"batch_no": "B-13", "approval_status": "Approved"}},
                    {"id": 14, "fields": {"batch_no": "B-14", "approval_status": "Pending Approval"}},
                ]

            def format_product_parts(self, value):
                return str(value or "")

            def get_stage_role_for_process_stage(self, process_seq, stage_name: str) -> str:
                return ""

        rows = production._list_ms_jobs_for_user_role(_FakeRepo(), "SupervisorA")
        self.assertEqual([row.get("id") for row in rows], [1, 3])

    async def test_my_ms_jobs_filter_routes_to_next_stage_selector(self):
        context = _DummyContext(
            {
                "menu_state": production.MY_MS_JOBS_FILTER_STATE,
                "my_ms_jobs_all_records": [
                    {
                        "id": 101,
                        "fields": {
                            "current_stage_name": "CUTTING",
                            "next_stage_name": "WELDING",
                        },
                    }
                ],
            }
        )
        update = _DummyUpdate(production._MS_VIEW_BY_NEXT_STAGE)

        with (
            patch(
                "pulse.integrations.production._show_my_ms_jobs_next_stage_filter_page", new=AsyncMock()
            ) as show_filter_page,
            patch("pulse.integrations.production._reply", new=AsyncMock()),
        ):
            handled = await production.handle_production_state_text(update, context, update.effective_message.text)

        self.assertTrue(handled)
        self.assertEqual(context.user_data.get("menu_state"), production.MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE)
        next_stage_selection = context.user_data.get("my_ms_jobs_next_stage_selection", {})
        self.assertEqual(next_stage_selection.get("options"), ["WELDING"])
        show_filter_page.assert_awaited_once()

    async def test_fallback_routes_all_production_states_to_production_handler(self):
        production_states = [
            SELECTING_BATCH_MODE_STATE,
            SELECTING_PRODUCT_MODEL_STATE,
            SELECTING_PRODUCT_PARTS_STATE,
            ENTERING_BATCH_QTY_STATE,
            SELECTING_BATCH_TYPE_STATE,
            CONFIRMING_BATCH_STATE,
            AWAITING_APPROVAL_STATE,
            PENDING_APPROVALS_SELECTION_STATE,
            PENDING_APPROVALS_CONFIRM_STATE,
            MY_MS_SCHEDULE_SELECTION_STATE,
            MY_MS_SCHEDULE_CONFIRM_STATE,
            MY_MS_JOBS_FILTER_STATE,
            MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE,
            MY_MS_JOBS_CREATED_BY_SELECTION_STATE,
            MY_MS_JOBS_SELECTION_STATE,
            MY_MS_JOBS_ACTION_STATE,
            MY_MS_JOBS_CONFIRM_STATE,
            MY_MS_JOBS_REMARKS_STATE,
            AWAITING_SCHEDULE_DATE_STATE,
        ]

        with (
            patch("pulse.main.load_user_access", new=AsyncMock(return_value=True)),
            patch("pulse.main._menu_actions", return_value={}),
            patch("pulse.main._show_menu_for_state", new=AsyncMock()),
            patch("pulse.main._execute_menu_action", new=AsyncMock()),
            patch("pulse.main._reply_text", new=AsyncMock()) as reply_text,
            patch("pulse.main.handle_production_state_text", new=AsyncMock(return_value=True)) as prod_handler,
        ):
            for state in production_states:
                context = _DummyContext({"menu_state": state, "is_registered": True, "access_loaded": True})
                update = _DummyUpdate("not-a-menu-button")
                await fallback_text(update, context)

        self.assertEqual(prod_handler.await_count, len(production_states))
        self.assertEqual(reply_text.await_count, 0)

    async def test_main_menu_label_still_returns_to_main_menu(self):
        with (
            patch("pulse.main.load_user_access", new=AsyncMock(return_value=True)),
            patch("pulse.main._show_menu_for_state", new=AsyncMock()) as show_menu,
        ):
            context = _DummyContext({"menu_state": MY_MS_JOBS_FILTER_STATE})
            update = _DummyUpdate(MAIN_MENU_LABEL)
            await fallback_text(update, context)

        self.assertEqual(context.user_data.get("menu_state"), MAIN_STATE)
        show_menu.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
