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
    SELECTING_BATCH_NOTIFIERS_STATE,
    SELECTING_BATCH_OWNER_STATE,
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
    def test_role_matches_supports_multi_role_tokens(self):
        self.assertTrue(production._role_matches("Cutting_Supervisor|Production_Supervisor", "Production_Supervisor"))
        self.assertTrue(production._role_matches("System_Admin,Production_Manager", "System_Admin"))
        self.assertFalse(production._role_matches("Cutting_Supervisor", "Production_Manager"))

    def test_can_confirm_handoff_denies_non_assigned_viewer_even_if_delegate_or_notifier(self):
        row_fields = {
            "current_status": production._MS_PENDING_CONFIRMATION,
            "next_stage_name": "Press Job",
            "process_seq": "PLS-PJ-PROD",
            "batch_id": 101,
        }

        with (
            patch("pulse.integrations.production._get_stage_assignment_user_ids", return_value={"next-stage-user"}),
            patch("pulse.integrations.production._get_row_delegated_user_ids", return_value={"cutting-user"}),
            patch("pulse.integrations.production._get_batch_notifier_user_ids", return_value={"cutting-user"}),
        ):
            allowed = production._can_confirm_handoff(
                repo=object(),
                row_fields=row_fields,
                user_role_name="Cutting_Supervisor",
                viewer_user_id="cutting-user",
                row_id=1,
            )

        self.assertFalse(allowed)

    def test_can_confirm_handoff_allows_assigned_next_stage_viewer(self):
        row_fields = {
            "current_status": production._MS_PENDING_CONFIRMATION,
            "next_stage_name": "Press Job",
            "process_seq": "PLS-PJ-PROD",
            "batch_id": 101,
        }

        with patch("pulse.integrations.production._get_stage_assignment_user_ids", return_value={"next-stage-user"}):
            allowed = production._can_confirm_handoff(
                repo=object(),
                row_fields=row_fields,
                user_role_name="Cutting_Supervisor",
                viewer_user_id="next-stage-user",
                row_id=1,
            )

        self.assertTrue(allowed)

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

    def test_batch_snapshot_filters_out_unowned_non_action_flow_for_supervisor(self):
        batch_id = 101
        batch_no = "MAR26-S1KHFL-BASE-MCS-001"

        class _FakeCostingClient:
            def get_records(self, table: str):
                if table == "Users":
                    return [{"id": 1, "fields": {"User_ID": "creator-1", "Name": "Creator One"}}]
                return []

        class _FakeRepo:
            costing_client = _FakeCostingClient()

            def list_ms_rows_for_batch(self, target_batch_id: int):
                if target_batch_id != batch_id:
                    return []
                return [
                    {
                        "id": 1,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PLS-PJ-PROD",
                            "current_stage_name": "Plasma Cutting",
                            "current_stage_role_name": "Cutting_Supervisor",
                            "current_status": "Plasma Cutting Pending",
                        },
                    },
                    {
                        "id": 2,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PLS-PROD",
                            "current_stage_name": "Plasma Cutting",
                            "current_stage_role_name": "Cutting_Supervisor",
                            "current_status": "Plasma Cutting Pending",
                        },
                    },
                    {
                        "id": 3,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PRS-PJ-PROD",
                            "current_stage_name": "Press Job",
                            "current_stage_role_name": "Press_Job_Supervisor",
                            "current_status": "Press Job Pending",
                        },
                    },
                    {
                        "id": 4,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PRS-PROD",
                            "current_stage_name": "Press Cutting",
                            "current_stage_role_name": "Cutting_Supervisor",
                            "current_status": "Press Cutting Pending",
                        },
                    },
                ]

            def get_master_by_id(self, target_batch_id: int):
                if target_batch_id != batch_id:
                    return None
                return {"id": batch_id, "fields": {"created_by": 1}}

            def get_all_master_batches(self):
                return [{"id": batch_id, "fields": {"created_by": 1, "batch_no": batch_no}}]

            def get_process_stage_names(self, process_seq):
                stage_map = {
                    "PLS-PJ-PROD": ["Plasma Cutting", "Press Job", "Production"],
                    "PLS-PROD": ["Plasma Cutting", "Production"],
                    "PRS-PJ-PROD": ["Press Cutting", "Press Job", "Production"],
                    "PRS-PROD": ["Press Cutting", "Production"],
                }
                return stage_map.get(process_seq, [])

            def get_process_display_label(self, process_seq):
                return str(process_seq or "")

            def get_stage_role_for_process_stage(self, process_seq, stage_name: str) -> str:
                return ""

        text = production._build_ms_batch_snapshot_overview_text(
            _FakeRepo(),
            batch_id,
            batch_no,
            user_role_name="Cutting_Supervisor",
            viewer_user_id="other-user",
        )
        self.assertIn("Flows: 3", text)
        self.assertIn("PLS-PJ-PROD", text)
        self.assertIn("PLS-PROD", text)
        self.assertIn("PRS-PROD", text)
        self.assertNotIn("PRS-PJ-PROD", text)

    def test_batch_snapshot_shows_all_flows_for_owner_and_admin(self):
        batch_id = 101
        batch_no = "MAR26-S1KHFL-BASE-MCS-001"

        class _FakeCostingClient:
            def get_records(self, table: str):
                if table == "Users":
                    return [{"id": 1, "fields": {"User_ID": "creator-1", "Name": "Creator One"}}]
                return []

        class _FakeRepo:
            costing_client = _FakeCostingClient()

            def list_ms_rows_for_batch(self, target_batch_id: int):
                if target_batch_id != batch_id:
                    return []
                return [
                    {
                        "id": 1,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PLS-PJ-PROD",
                            "current_stage_name": "Plasma Cutting",
                            "current_stage_role_name": "Cutting_Supervisor",
                            "current_status": "Plasma Cutting Pending",
                        },
                    },
                    {
                        "id": 2,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PLS-PROD",
                            "current_stage_name": "Plasma Cutting",
                            "current_stage_role_name": "Cutting_Supervisor",
                            "current_status": "Plasma Cutting Pending",
                        },
                    },
                    {
                        "id": 3,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PRS-PJ-PROD",
                            "current_stage_name": "Press Job",
                            "current_stage_role_name": "Press_Job_Supervisor",
                            "current_status": "Press Job Pending",
                        },
                    },
                    {
                        "id": 4,
                        "fields": {
                            "batch_id": batch_id,
                            "process_seq": "PRS-PROD",
                            "current_stage_name": "Press Cutting",
                            "current_stage_role_name": "Cutting_Supervisor",
                            "current_status": "Press Cutting Pending",
                        },
                    },
                ]

            def get_master_by_id(self, target_batch_id: int):
                if target_batch_id != batch_id:
                    return None
                return {"id": batch_id, "fields": {"created_by": 1}}

            def get_all_master_batches(self):
                return [{"id": batch_id, "fields": {"created_by": 1, "batch_no": batch_no}}]

            def get_process_stage_names(self, process_seq):
                stage_map = {
                    "PLS-PJ-PROD": ["Plasma Cutting", "Press Job", "Production"],
                    "PLS-PROD": ["Plasma Cutting", "Production"],
                    "PRS-PJ-PROD": ["Press Cutting", "Press Job", "Production"],
                    "PRS-PROD": ["Press Cutting", "Production"],
                }
                return stage_map.get(process_seq, [])

            def get_process_display_label(self, process_seq):
                return str(process_seq or "")

            def get_stage_role_for_process_stage(self, process_seq, stage_name: str) -> str:
                return ""

        repo = _FakeRepo()
        owner_text = production._build_ms_batch_snapshot_overview_text(
            repo,
            batch_id,
            batch_no,
            user_role_name="Cutting_Supervisor",
            viewer_user_id="creator-1",
        )
        admin_text = production._build_ms_batch_snapshot_overview_text(
            repo,
            batch_id,
            batch_no,
            user_role_name="System_Admin",
            viewer_user_id="other-user",
        )
        manager_text = production._build_ms_batch_snapshot_overview_text(
            repo,
            batch_id,
            batch_no,
            user_role_name="Production_Manager",
            viewer_user_id="other-user",
        )
        self.assertIn("Flows: 4", owner_text)
        self.assertIn("PRS-PJ-PROD", owner_text)
        self.assertIn("Flows: 4", admin_text)
        self.assertIn("PRS-PJ-PROD", admin_text)
        self.assertIn("Flows: 4", manager_text)
        self.assertIn("PRS-PJ-PROD", manager_text)

    def test_apply_ms_jobs_filter_includes_all_owner_rows_for_non_privileged_views(self):
        all_rows = [
            {"id": 1, "fields": {"batch_id": 10, "next_stage_name": "Production", "current_stage_name": "Plasma Cutting"}},
            {"id": 2, "fields": {"batch_id": 10, "next_stage_name": "Production", "current_stage_name": "Press Job"}},
            {"id": 3, "fields": {"batch_id": 20, "next_stage_name": "Dispatch", "current_stage_name": "Press Cutting"}},
        ]
        action_rows = [all_rows[2]]

        filtered_owner, _ = production._apply_my_ms_jobs_filter(
            all_rows,
            action_rows,
            production._MS_VIEW_BY_BATCH_NO,
            "B-10",
            {10: "Owner One", 20: "Owner Two"},
            {10: "creator-1", 20: "creator-2"},
            {10: "B-10", 20: "B-20"},
            "Cutting_Supervisor",
            "creator-1",
        )
        filtered_non_owner, _ = production._apply_my_ms_jobs_filter(
            all_rows,
            action_rows,
            production._MS_VIEW_BY_BATCH_NO,
            "B-10",
            {10: "Owner One", 20: "Owner Two"},
            {10: "creator-1", 20: "creator-2"},
            {10: "B-10", 20: "B-20"},
            "Cutting_Supervisor",
            "other-user",
        )
        self.assertEqual([row.get("id") for row in filtered_owner], [1, 2])
        self.assertEqual(filtered_non_owner, [])

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

    async def test_batch_view_number_selection_opens_batch_overview_not_row_action_menu(self):
        context = _DummyContext(
            {
                "menu_state": production.MY_MS_JOBS_SELECTION_STATE,
                "my_ms_jobs_filter": production._MS_VIEW_BY_BATCH_NO,
                "my_ms_jobs_batch_no_by_id": {10: "B-10"},
                "my_ms_jobs_selection": {
                    "records": [{"id": 1, "fields": {"batch_id": 10, "current_stage_name": "Press Job"}}],
                    "page": 0,
                    "page_size": 5,
                },
            }
        )
        update = _DummyUpdate("1")

        class _FakeRepo:
            def get_master_by_id(self, batch_id: int):
                return {"id": batch_id, "fields": {"batch_no": "B-10"}}

        with (
            patch("pulse.integrations.production.ProductionRepo", return_value=_FakeRepo()),
            patch("pulse.integrations.production._show_ms_batch_tracker_overview", new=AsyncMock()) as show_overview,
            patch("pulse.integrations.production._show_ms_job_action_menu", new=AsyncMock()) as show_action_menu,
            patch("pulse.integrations.production._reply", new=AsyncMock()),
        ):
            handled = await production.handle_production_state_text(update, context, update.effective_message.text)

        self.assertTrue(handled)
        self.assertEqual(context.user_data.get("menu_state"), production.MY_MS_BATCH_ACTION_STATE)
        self.assertEqual(context.user_data.get("my_ms_batch_action", {}).get("batch_id"), 10)
        show_overview.assert_awaited_once()
        show_action_menu.assert_not_awaited()

    async def test_stale_row_action_state_recovers_to_batch_selection_and_opens_overview(self):
        context = _DummyContext(
            {
                "menu_state": production.MY_MS_JOBS_ACTION_STATE,
                "my_ms_jobs_action": {"selected_record": {"id": 99, "fields": {"batch_id": 99}}},
                "my_ms_jobs_batch_selection": {
                    "options": ["B-10"],
                    "page": 0,
                    "page_size": 5,
                    "parent_state": production.MY_MS_JOBS_FILTER_STATE,
                },
            }
        )
        update = _DummyUpdate("1")

        class _FakeRepo:
            def get_master_by_batch_no(self, batch_no: str):
                return {"id": 10, "fields": {"batch_no": batch_no}}

        with (
            patch("pulse.integrations.production.ProductionRepo", return_value=_FakeRepo()),
            patch("pulse.integrations.production._show_ms_batch_tracker_overview", new=AsyncMock()) as show_overview,
            patch("pulse.integrations.production._reply", new=AsyncMock()) as reply_mock,
        ):
            handled = await production.handle_production_state_text(update, context, update.effective_message.text)

        self.assertTrue(handled)
        self.assertEqual(context.user_data.get("menu_state"), production.MY_MS_BATCH_ACTION_STATE)
        self.assertEqual(context.user_data.get("my_ms_batch_action", {}).get("batch_id"), 10)
        show_overview.assert_awaited_once()
        reply_texts = [call.args[1] for call in reply_mock.await_args_list if len(call.args) > 1]
        self.assertNotIn("Choose one action from menu.", reply_texts)

    async def test_fallback_routes_all_production_states_to_production_handler(self):
        production_states = [
            SELECTING_BATCH_MODE_STATE,
            SELECTING_PRODUCT_MODEL_STATE,
            SELECTING_PRODUCT_PARTS_STATE,
            ENTERING_BATCH_QTY_STATE,
            SELECTING_BATCH_TYPE_STATE,
            SELECTING_BATCH_OWNER_STATE,
            SELECTING_BATCH_NOTIFIERS_STATE,
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

    async def test_mark_done_final_stage_handoff_notifies_only_owner_for_action_and_approvers_for_info(self):
        class _FakeCostingClient:
            def get_records(self, table: str):
                if table != "Users":
                    return []
                return [{"id": 501, "fields": {"User_ID": "owner-user", "Name": "Owner User"}}]

        class _FakeRepo:
            costing_client = _FakeCostingClient()

            def get_ms_row_by_id(self, row_id: int):
                return {
                    "id": row_id,
                    "fields": {
                        "batch_id": 101,
                        "process_seq": "SEQ-1",
                        "current_stage_index": 1,
                        "current_stage_name": "Press Job",
                        "next_stage_name": "Production",
                        "current_status": "Press Job Pending",
                        "total_qty": 16,
                        "product_part": "P1",
                    },
                }

            def get_process_stage_names(self, process_seq):
                return ["Cutting", "Press Job", "Production"]

            def get_stage_role_for_process_stage(self, process_seq, stage_name: str) -> str:
                if stage_name == "Production":
                    return "Production_Supervisor"
                return "Machine-Shop Supervisor"

            def filter_table_fields(self, table: str, fields: dict):
                return fields

            def update_ms(self, row_id: int, fields: dict):
                return None

            def add_status_history(self, *args, **kwargs):
                return None

            def get_master_by_id(self, batch_id: int):
                return {"id": batch_id, "fields": {"batch_no": "B-101", "created_by": "owner-user"}}

            def format_product_parts(self, value):
                return str(value or "")

        context = _DummyContext({"user": {"user_id": "900000006"}})
        context.bot = _DummyBot()
        notify_stage_event = AsyncMock()
        notify_event = AsyncMock()

        with (
            patch("pulse.integrations.production._notify_stage_event", new=notify_stage_event),
            patch("pulse.integrations.production._notify_event", new=notify_event),
            patch("pulse.integrations.production.recalculate_master_overall_status", return_value="In Progress"),
        ):
            await production._mark_ms_stage_done_pending_confirmation(_FakeRepo(), context, 1, 7)

        self.assertGreaterEqual(notify_stage_event.await_count, 1)
        pending_call = notify_stage_event.await_args_list[0]
        renderer = pending_call.kwargs.get("recipient_renderer")
        self.assertIsNotNone(renderer)
        self.assertEqual(renderer({"user_id": "owner-user"}).get("skip"), None)
        self.assertEqual(renderer({"user_id": "other-user"}).get("skip"), True)

        self.assertGreaterEqual(notify_event.await_count, 1)
        info_call = notify_event.await_args_list[0]
        self.assertEqual(
            set(info_call.kwargs.get("context", {}).get("recipient_roles", [])),
            {"Production_Manager", "System_Admin"},
        )

    async def test_advance_final_stage_notifies_only_owner_for_action_and_approvers_for_info(self):
        class _FakeCostingClient:
            def get_records(self, table: str):
                if table != "Users":
                    return []
                return [{"id": 501, "fields": {"User_ID": "owner-user", "Name": "Owner User"}}]

        class _FakeRepo:
            costing_client = _FakeCostingClient()

            def get_ms_row_by_id(self, row_id: int):
                return {
                    "id": row_id,
                    "fields": {
                        "batch_id": 101,
                        "process_seq": "SEQ-1",
                        "current_stage_index": 0,
                        "current_stage_name": "Press Job",
                        "next_stage_name": "Production",
                        "current_status": production._MS_PENDING_CONFIRMATION,
                        "total_qty": 16,
                        "product_part": "P1",
                    },
                }

            def get_process_stage_names(self, process_seq):
                return ["Press Job", "Production"]

            def get_stage_role_for_process_stage(self, process_seq, stage_name: str) -> str:
                if stage_name == "Production":
                    return "Production_Supervisor"
                return "Machine-Shop Supervisor"

            def filter_table_fields(self, table: str, fields: dict):
                return fields

            def update_ms(self, row_id: int, fields: dict):
                return None

            def add_status_history(self, *args, **kwargs):
                return None

            def get_master_by_id(self, batch_id: int):
                return {"id": batch_id, "fields": {"batch_no": "B-101", "created_by": "owner-user"}}

            def format_product_parts(self, value):
                return str(value or "")

        context = _DummyContext({"user": {"user_id": "900000006"}})
        context.bot = _DummyBot()
        notify_stage_event = AsyncMock()
        notify_event = AsyncMock()

        with (
            patch("pulse.integrations.production._notify_stage_event", new=notify_stage_event),
            patch("pulse.integrations.production._notify_event", new=notify_event),
            patch("pulse.integrations.production.recalculate_master_overall_status", return_value="In Progress"),
        ):
            await production.advance_ms_stage(_FakeRepo(), context, 1, 7)

        pending_calls = [call for call in notify_stage_event.await_args_list if call.args[1] == "ms_stage_pending"]
        self.assertTrue(pending_calls)
        renderer = pending_calls[0].kwargs.get("recipient_renderer")
        self.assertIsNotNone(renderer)
        self.assertEqual(renderer({"user_id": "owner-user"}).get("skip"), None)
        self.assertEqual(renderer({"user_id": "another-sup"}).get("skip"), True)

        info_calls = [
            call
            for call in notify_event.await_args_list
            if set(call.kwargs.get("context", {}).get("recipient_roles", [])) == {"Production_Manager", "System_Admin"}
        ]
        self.assertTrue(info_calls)


if __name__ == "__main__":
    unittest.main()
