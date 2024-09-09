import json
from datetime import datetime, timezone as dt_timezone

from django.conf import settings
from django.contrib import admin
from django.utils import timezone
from django.utils.html import format_html

from .models import Task
from .utils import display_diff_time


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    exclude = ("message_data",)
    readonly_fields = ("message_details", "traceback", "retries", "queue_name", "actor_name", "display_params")
    list_display = (
        "id",
        "actor_name",
        "display_params",
        "display_status",
        "display_wait_time",
        "retries",
        "display_duration",
        "queue_name",
        "eta",
        "created_at",
        "updated_at",

    )
    list_filter = ("status", "created_at", "queue_name", "actor_name")
    search_fields = ("actor_name",)

    fieldsets = (
        (
            None,
            {
                'fields': (
                    'id', ('status', 'retries'), ('actor_name', 'queue_name'), 'display_params',
                )
            }
        ),

        (
            'Timeline',
            {
                'fields': (
                    ('created_at', 'start_at'), ('updated_at', 'end_at'), ('duration', 'wait_time')
                )
            }
        ),

        (
            'Details',
            {
                'fields': (
                    'message_details', 'traceback',
                )
            }
        ),

    )

    def eta(self, instance):
        timestamp = (
            instance.message.options.get("eta", instance.message.message_timestamp) / 1000
        )

        # Django expects a timezone-aware datetime if USE_TZ is True, and a naive datetime in localtime otherwise.
        tz = dt_timezone.utc if settings.USE_TZ else None
        return datetime.fromtimestamp(timestamp, tz=tz)

    def message_details(self, instance):
        message_details = json.dumps(instance.message._asdict(), indent=4)
        return format_html("<pre>{}</pre>", message_details)

    def traceback(self, instance):
        traceback = instance.message.options.get("traceback", None)
        if traceback:
            return format_html("<pre>{}</pre>", traceback)
        return None

    @admin.display(description='Status', ordering='status')
    def display_status(self, instance):
        status = instance.status.upper()
        if status == "FAILED":
            return format_html('<b style="color:{};">{}</b>', '#f20707', status)
        if status == "DONE":
            return format_html('<b style="color:{};">{}</b>', '#3d9402', status)
        return format_html('<b style="color:{};">{}</b>', '#ffad00', status)

    @admin.display(description='Wait time', ordering='wait_time')
    def display_wait_time(self, instance):
        return display_diff_time(instance.wait_time)

    @admin.display(description='Duration', ordering='duration')
    def display_duration(self, instance):
        return display_diff_time(instance.duration)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, task=None):
        return False

    def has_delete_permission(self, request, task=None):
        return False
