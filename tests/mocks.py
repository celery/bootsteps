import inspect

from asynctest import CoroutineMock, Mock, NonCallableMock

from bootsteps import Step


class TrioCoroutineMock(CoroutineMock):
    async def _mock_call(_mock_self, *args, **kwargs):
        result = super()._mock_call(*args, **kwargs)

        if isinstance(_mock_self.side_effect, BaseException):
            raise _mock_self.side_effect

        _call = _mock_self.call_args

        try:
            if inspect.isawaitable(result):
                result = await result
        except BaseException as e:
            result = e
        finally:
            _mock_self.await_count += 1
            _mock_self.await_args = _call
            _mock_self.await_args_list.append(_call)

        if isinstance(result, BaseException):
            raise result

        return result


def create_mock_step(
    name,
    requires=set(),
    required_by=set(),
    last=False,
    include_if=True,
    spec=Step,
    mock_class=Mock,
):
    mock_step = mock_class(name=name, spec=spec)
    mock_step.requires = requires
    mock_step.required_by = required_by
    mock_step.last = last
    if isinstance(include_if, bool):
        mock_step.include_if.return_value = include_if
    else:
        mock_step.include_if.side_effect = include_if

    return mock_step


def create_start_stop_mock_step(
    name,
    requires=set(),
    required_by=set(),
    last=False,
    include_if=True,
    mock_class=Mock,
):
    mock_step = NonCallableMock(name=name, spec=Step)
    mock_step.requires = requires
    mock_step.required_by = required_by
    mock_step.last = last
    mock_step.start = mock_class()
    mock_step.stop = mock_class()
    if isinstance(include_if, bool):
        mock_step.include_if.return_value = include_if
    else:
        mock_step.include_if.side_effect = include_if

    return mock_step
