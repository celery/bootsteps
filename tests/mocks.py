import inspect
from asynctest import CoroutineMock


class TrioCoroutineMock(CoroutineMock):
    async def _mock_call(_mock_self, *args, **kwargs):
        result = super()._mock_call(*args, **kwargs)

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
