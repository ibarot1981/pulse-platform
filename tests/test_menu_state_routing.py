import re
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


class _DummyBot:
    pass


class _DummyCallbackMessage:
    def __init__(self):
        self.replies: list[str] = []

    async def reply_text(self, text: str, **kwargs):
        self.replies.append(str(text))


class _DummyCallbackQuery:
    def __init__(self, data: str):
        self.data = data
        self.message = _DummyCallbackMessage()
        self.answered = False

    async def answer(self, *args, **kwargs):
        self.answered = True


class _DummyCallbackUpdate:
    def __init__(self, data: str):
        self.callback_query = _DummyCallbackQuery(data)


class MenuStateRoutingTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_iso_datetime_supports_decimal_epoch_text(self):
        parsed = production._parse_iso_datetime("1772814858.972331")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.year, 2026)

    def test_format_notification_datetime_default_pattern(self):
        with patch("pulse.integrations.production.NOTIFICATION_TIMEZONE", "Asia/Calcutta"):
            self.assertEqual(production._format_notification_datetime(0), "01-01-1970 05:30:00 IST")

    def test_format_notification_datetime_local_naive_string(self):
        with patch("pulse.integrations.production.NOTIFICATION_TIMEZONE", "Asia/Calcutta"):
            self.assertEqual(
                production._format_notification_datetime("2026-03-07 12:16:10"),
                "07-03-2026 12:16:10 IST",
            )

    def test_format_dt_short_uses_notification_timezone(self):
        with patch("pulse.integrations.production.NOTIFICATION_TIMEZONE", "Asia/Calcutta"):
            self.assertEqual(
                production._format_dt_short("2026-03-07 12:16:10"),
                "07-03-2026",
            )

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

    async def test_confirm_batch_yes_does_not_auto_show_main_menu_again(self):
        async def _handle_production(update, context, text):
            context.user_data["menu_state"] = MAIN_STATE
            return True

        with (
            patch("pulse.main.load_user_access", new=AsyncMock(return_value=True)),
            patch("pulse.main._menu_actions", return_value={}),
            patch("pulse.main._show_menu_for_state", new=AsyncMock()) as show_menu,
            patch("pulse.main._reply_text", new=AsyncMock()) as reply_text,
            patch("pulse.main.handle_production_state_text", new=_handle_production),
        ):
            context = _DummyContext({"menu_state": CONFIRMING_BATCH_STATE, "is_registered": True, "access_loaded": True})
            update = _DummyUpdate("Yes")
            await fallback_text(update, context)

        self.assertEqual(show_menu.await_count, 0)
        self.assertEqual(reply_text.await_count, 0)

    async def test_approval_callback_success_does_not_send_duplicate_reply(self):
        class _FakeRepo:
            def get_role_name_by_user_id(self, user_id: str) -> str:
                return "Production_Manager"

            def get_costing_user_ref_by_user_id(self, user_id: str):
                return 100

            def get_master_by_id(self, batch_id: int):
                return {"id": batch_id, "fields": {"batch_no": "B-1", "approval_status": "Pending Approval"}}

        update = _DummyCallbackUpdate("prodappr:approve:1")
        context = _DummyContext({"user": {"user_id": "U_PM"}})

        with (
            patch("pulse.integrations.production.ProductionRepo", return_value=_FakeRepo()),
            patch("pulse.integrations.production._is_production_manager", return_value=True),
            patch("pulse.integrations.production.approve_batches_by_ids", new=AsyncMock(return_value=["B-1"])),
        ):
            handled = await production.handle_production_callback(update, context)

        self.assertTrue(handled)
        self.assertTrue(update.callback_query.answered)
        self.assertEqual(update.callback_query.message.replies, [])

    async def test_approve_batch_notification_formats_start_date(self):
        class _FakeRepo:
            def get_costing_user_ref_by_user_id(self, user_id: str):
                return 100

        context = _DummyContext({"user": {"user_id": "U_PM"}})
        context.bot = _DummyBot()
        update = _DummyUpdate("irrelevant")

        notify_mock = AsyncMock()
        with (
            patch("pulse.integrations.production.ProductionRepo", return_value=_FakeRepo()),
            patch("pulse.integrations.production._is_production_manager", return_value=True),
            patch(
                "pulse.integrations.production.approve_batch_service",
                return_value={
                    "master": {
                        "fields": {
                            "batch_no": "B-1",
                            "start_date": "1772814858.972331",
                        }
                    },
                    "ms_rows": [],
                    "row_cutlist_map": {},
                    "cutlist_sections": [],
                },
            ),
            patch("pulse.integrations.production._attach_ms_cutlist_pdf", return_value=None),
            patch("pulse.integrations.production._attach_ms_row_cutlist_pdfs", return_value=None),
            patch("pulse.integrations.production._notify_ms_first_stage", new=AsyncMock()),
            patch("pulse.integrations.production._notify_event", new=notify_mock),
        ):
            await production.approve_batches_by_ids(update, context, [1])

        message = notify_mock.await_args.args[2]
        self.assertIn("Batch approved: B-1 | Start Date:", message)
        self.assertNotIn("1772814858.972331", message)
        self.assertRegex(message, r"Start Date: \d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2} [A-Z]{2,5}")


    async def test_ms_batch_approval_summary_then_detail_flow(self):
        batch_id = 101
        batch_no = "MAR26-S1KHFL-BASE-MCS-001"
        summary_rows = [
            {
                "id": 1,
                "fields": {
                    "process_seq": "SEQ-1",
                    "current_stage_name": "Press Cutting to Press Job",
                    "next_stage_name": "Plasma Cutting to Production",
                    "total_qty": 16,
                },
            },
            {
                "id": 2,
                "fields": {
                    "process_seq": "SEQ-1",
                    "current_stage_name": "Plasma Cutting",
                    "next_stage_name": "",
                    "total_qty": 16,
                    "current_stage_index": 1,
                    "product_part": 77,
                },
            },
            {
                "id": 3,
                "fields": {
                    "process_seq": "SEQ-2",
                    "current_stage_name": "Finishing",
                    "next_stage_name": "Dispatch",
                    "total_qty": 16,
                },
            },
        ]

        class _FakeCostingClient:
            def get_records(self, table: str):
                if table != "Users":
                    return []
                return [
                    {"id": 501, "fields": {"User_ID": "owner-user", "Name": "Sanjay Rohit"}},
                    {"id": 601, "fields": {"User_ID": "900000004", "Name": "Approver PM"}},
                    {"id": 602, "fields": {"User_ID": "900000003", "Name": "Production Supervisor"}},
                ]

        class _FakeRepo:
            costing_client = _FakeCostingClient()

            def get_role_name_by_user_id(self, user_id: str) -> str:
                if user_id == "900000003":
                    return "Production_Supervisor"
                return "Production_Manager"

            def get_costing_user_ref_by_user_id(self, user_id: str):
                return 900

            def get_master_by_id(self, master_id: int):
                if master_id != batch_id:
                    return None
                return {
                    "id": master_id,
                    "fields": {
                        "batch_no": batch_no,
                        "created_by": "owner-user",
                        "overall_status": "Schedule Pending",
                    },
                }

            def get_process_stage_names(self, process_seq):
                if process_seq == "SEQ-1":
                    return ["Press Cutting", "Plasma Cutting", "Finishing"]
                if process_seq == "SEQ-2":
                    return ["Finishing", "Dispatch"]
                return []

            def get_stage_role_for_process_stage(self, process_seq, stage_name: str) -> str:
                role_map = {
                    "Press Cutting to Press Job": "Production_Supervisor",
                    "Plasma Cutting": "Production_Supervisor",
                    "Finishing": "Production_Manager",
                }
                return role_map.get(stage_name, "")

            def get_all_master_batches(self):
                return [
                    {
                        "id": batch_id,
                        "fields": {
                            "batch_no": batch_no,
                            "created_by": "owner-user",
                        },
                    }
                ]

            def list_ms_rows_for_batch(self, master_id: int):
                if master_id != batch_id:
                    return []
                return [dict(row) for row in summary_rows]

            def format_product_parts(self, value):
                return f"Part-{value}"

        notify_stage_event = AsyncMock()
        context = _DummyContext(
            {
                "user": {
                    "user_id": "900000004",
                    "name": "Approver PM",
                },
            }
        )

        with patch("pulse.integrations.production.ProductionRepo", return_value=_FakeRepo()):
            with patch("pulse.integrations.production._notify_stage_event", new=notify_stage_event):
                await production._notify_ms_first_stage(_FakeRepo(), context, batch_id, summary_rows, batch_no)

            update = _DummyCallbackUpdate("msbatch:vd:101")
            context.user_data["user"]["user_id"] = "900000003"
            handled = await production.handle_production_callback(update, context)

            summary_calls = [
                call
                for call in notify_stage_event.await_args_list
                if "New Batch Approved" in (call.args[3] if len(call.args) >= 4 else "")
            ]
            summary_messages = [call.args[3] for call in summary_calls]
            summary_markups = [str(call.kwargs.get("reply_markup", "")) for call in summary_calls]
            self.assertEqual(len(summary_messages), 2)
            for message in summary_messages:
                self.assertIn(batch_no, message)
                self.assertIn("Batch By: Sanjay Rohit", message)
                self.assertIn("Approved By: Approver PM", message)
            self.assertTrue(all("msbatch:vd:101" in markup for markup in summary_markups))
            self.assertTrue(any("1. Press Cutting to Press Job to Plasma Cutting to Production" in message and "2. Plasma Cutting to Finishing" in message for message in summary_messages))
            self.assertTrue(any("1. Finishing to Dispatch" in message for message in summary_messages))

            self.assertTrue(handled)
            self.assertTrue(update.callback_query.answered)
            self.assertEqual(len(update.callback_query.message.replies), 2)
            for reply in update.callback_query.message.replies:
                self.assertIn("🟢", reply)
                self.assertIn("Batch No:", reply)
                self.assertIn("Current Stage:", reply)

if __name__ == "__main__":
    unittest.main()
