from unittest.mock import Mock


def assert_log_message_field_equals(log_message, field_name, value):
    __tracebackhide__ = True

    assert (
        field_name in log_message and value(log_message[field_name])
        if callable(value) and not isinstance(value, Mock)
        else log_message[field_name] == value
    ), f"{log_message[field_name]} != {value}"


def assert_field_equals_in_any_message(log_messages, field_name, value):
    __tracebackhide__ = True

    assert any(
        field_name in log_message and value(log_message[field_name])
        if callable(value) and not isinstance(value, Mock)
        else log_message[field_name] == value
        for log_message in log_messages
    )


def assert_logged_action_succeeded(logged_action):
    __tracebackhide__ = True

    assert_log_message_field_equals(
        logged_action.end_message, "action_status", "succeeded"
    )


def assert_logged_action_failed(logged_action):
    __tracebackhide__ = True

    assert_log_message_field_equals(
        logged_action.end_message, "action_status", "failed"
    )
