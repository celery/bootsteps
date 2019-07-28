import os
from unittest.mock import Mock

import pytest

from bootsteps.contrib.systemd import systemd_notify


@pytest.fixture
def systemd_notify_step():
    return systemd_notify.SystemDNotify(state={"READY": 1})


@pytest.fixture
def ensure_notify_socket_environment_variable_is_not_present(mocker):
    return mocker.patch.dict(os.environ, {})


@pytest.fixture
def ensure_systemd_python_library_is_not_installed(monkeypatch):
    monkeypatch.setattr(systemd_notify, "notify", None)


@pytest.fixture
def systemd_notify_mock():
    return Mock()


@pytest.fixture
def ensure_systemd_python_library_is_installed(monkeypatch, systemd_notify_mock):
    monkeypatch.setattr(systemd_notify, "notify", systemd_notify_mock)


def test_initialization():
    expected = {"READY": 1}
    step = systemd_notify.SystemDNotify(state=expected)

    assert step.state == expected


@pytest.fixture
def ensure_notify_socket_environment_variable_is_present(mocker):
    return mocker.patch.dict(os.environ, {"NOTIFY_SOCKET": "/path/to/unix/socket"})


def test_include_if_should_be_false_when_notify_socket_is_not_present(
    systemd_notify_step,
    ensure_notify_socket_environment_variable_is_not_present,
    ensure_systemd_python_library_is_installed,
):
    assert systemd_notify_step.include_if() is False


def test_include_if_should_be_false_when_systemd_library_is_not_installed(
    systemd_notify_step,
    ensure_notify_socket_environment_variable_is_present,
    ensure_systemd_python_library_is_not_installed,
):
    assert systemd_notify_step.include_if() is False


def test_include_if_should_be_true_when_notify_socket_is_present(
    systemd_notify_step,
    ensure_notify_socket_environment_variable_is_present,
    ensure_systemd_python_library_is_installed,
):
    assert systemd_notify_step.include_if() is True


def test_include_if_should_be_true_when_systemd_library_is_installed(
    systemd_notify_step,
    ensure_notify_socket_environment_variable_is_present,
    ensure_systemd_python_library_is_installed,
):
    assert systemd_notify_step.include_if() is True


def test_step_call(
    systemd_notify_step, ensure_systemd_python_library_is_installed, systemd_notify_mock
):
    systemd_notify_step()
    systemd_notify_mock.assert_called_once_with(status="READY=1")
