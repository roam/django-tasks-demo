from __future__ import annotations

from collections.abc import Mapping

from django import forms
from django.http import HttpResponse
from django.shortcuts import redirect
from django.tasks import TaskResult, default_task_backend
from django.template.loader import render_to_string
from django.template.response import TemplateResponse

from messagecenter import tasks
from messagecenter.tasks import send_notification


class SendNotification(forms.Form):
    message = forms.CharField(widget=forms.Textarea(attrs={"rows": 4}))
    title = forms.CharField(max_length=50, required=False)

    def send(self) -> TaskResult:
        data = self.cleaned_data
        return tasks.send_notification.enqueue(
            message=data["message"], title=data.get("title")
        )


def index(request):
    html = HtmlCollector(request, root_template="index.html")
    backend = default_task_backend
    if request.method == "POST":
        form = SendNotification(request.POST)
        if form.is_valid():
            result = form.send()
            if request.htmx:
                html.add("#form", {"form": SendNotification()})
                html.add("#prepend-result", {"result": result})
                return html.to_response()
            return redirect("index")
        elif request.htmx:
            html.add("#form", {"form": form})
            return html.to_response()
    else:
        form = SendNotification()
    results = backend.get_results(tasks.send_notification)
    html.add(context={"form": form, "results": results})
    return html.to_response()


def task_result(request, result_id, status):
    result = send_notification.get_result(result_id)
    if result.status == status:
        # No need to swap the result.
        return HttpResponse(status=204)
    return TemplateResponse(request, "index.html#result", {"result": result})


class HtmlCollector:
    def __init__(self, request, root_template: str | None = None):
        self.request = request
        self.parts = []
        self.root_template = root_template

    def add(self, template: str | None = None, context: Mapping | None = None):
        if template is None:
            template = self.root_template
        elif template.startswith("#"):
            template = f"{self.root_template}{template}"
        result = render_to_string(template, context=context, request=self.request)
        self.parts.append(result)

    def to_response(self) -> HttpResponse:
        return HttpResponse("\n".join(self.parts))
