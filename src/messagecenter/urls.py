from __future__ import annotations

from django.urls import path

from messagecenter import views

urlpatterns = [
    path("", views.index, name="index"),
    path(
        ":task-result/<str:result_id>/<str:status>/",
        views.task_result,
        name="hx-task-result",
    ),
]
