import sys
import signal
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

import watchdog


SCRIPT_DIR = Path(watchdog.__file__).resolve().parent


class _FakeProcess:
    def __init__(self, return_codes, poll_sequence=None, wait_extras=None):
        if not isinstance(return_codes, list):
            return_codes = [return_codes]
        self._pending_codes = list(return_codes)
        self._poll_sequence = list(poll_sequence) if poll_sequence else None
        self._wait_extras = list(wait_extras) if wait_extras else []
        self.pid = 12345
        self.returncode = None
        self._wait_call_count = 0
        self._poll_call_count = 0
        self.sent_signals = []
        self.killed = False
        self._stopped = False

    def _consume_code(self):
        if self._pending_codes:
            self.returncode = self._pending_codes.pop(0)
            self._stopped = True
        return self.returncode

    def poll(self):
        self._poll_call_count += 1
        if self._poll_sequence is not None:
            if self._poll_sequence:
                result = self._poll_sequence.pop(0)
                self.returncode = result
                if result is not None:
                    self._stopped = True
            return self.returncode
        if self._stopped or self.killed:
            return self.returncode
        return None

    def wait(self, timeout=None):
        self._wait_call_count += 1
        if self._wait_extras:
            extra = self._wait_extras.pop(0)
            if isinstance(extra, BaseException):
                raise extra
            if isinstance(extra, type) and issubclass(extra, BaseException):
                raise extra()
        if not self._stopped and not self.killed:
            self._consume_code()
        return self.returncode

    def send_signal(self, sig):
        self.sent_signals.append(sig)

    def kill(self):
        self.killed = True
        self._stopped = True
        self.returncode = -9


def _make_popen(processes):
    procs = list(processes)

    def _popen(cmd, cwd=None, **kwargs):
        assert procs, "Popen called more times than fake processes provided"
        p = procs.pop(0)
        p._cmd = list(cmd)
        p._cwd = cwd
        p._kwargs = dict(kwargs)
        return p

    return _popen, procs


class TestCLIAndPaths:
    def test_parser_exposes_timeout_option_with_default_86400(self):
        parser = watchdog.build_parser()
        args = parser.parse_args(["--keyword", "k", "--city", "c"])
        assert args.timeout == 86400

    def test_parser_accepts_custom_timeout(self):
        parser = watchdog.build_parser()
        args = parser.parse_args(["--keyword", "k", "--city", "c", "--timeout", "3600"])
        assert args.timeout == 3600

    def test_timeout_forwarded_to_scraper_once(self):
        captures = {}

        def popen(cmd, cwd=None, **kw):
            captures["cmd"] = list(cmd)
            captures["cwd"] = cwd
            return _FakeProcess([0])

        rc = watchdog.run_scraper(
            keyword="K", city="C", timeout=9999, max_attempts=1, _popen=popen
        )
        assert rc == 0
        cmd = captures["cmd"]
        assert "--timeout" in cmd
        idx = cmd.index("--timeout")
        assert cmd[idx + 1] == "9999"
        assert cmd.count("--timeout") == 1

    def test_child_launched_with_script_dir_cwd(self):
        captures = {}

        def popen(cmd, cwd=None, **kw):
            captures["cwd"] = cwd
            captures["cmd"] = list(cmd)
            return _FakeProcess([0])

        watchdog.run_scraper(
            keyword="K", city="C", max_attempts=1, _popen=popen
        )
        assert Path(captures["cwd"]).resolve() == SCRIPT_DIR

    def test_child_script_path_resolved_relative_to_watchdog(self):
        captures = {}

        def popen(cmd, cwd=None, **kw):
            captures["cmd"] = list(cmd)
            return _FakeProcess([0])

        watchdog.run_scraper(
            keyword="K", city="C", max_attempts=1, _popen=popen
        )
        script_arg = captures["cmd"][1]
        assert Path(script_arg).resolve() == (SCRIPT_DIR / "maps_scraper.py")

    def test_watchdog_log_path_resolved_relative_to_watchdog(self):
        expected = SCRIPT_DIR / "watchdog.log"
        assert watchdog.log_file_path.resolve() == expected


class TestGracefulStop:
    def test_send_graceful_stop_sends_sigint_first_and_waits_grace_period(self):
        p = _FakeProcess([0])
        with patch.object(watchdog, "GRACEFUL_SHUTDOWN_TIMEOUT", 30):
            rc = watchdog._send_graceful_stop(p)
        assert rc == 0
        expected_sig = signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
        assert p.sent_signals == [expected_sig]
        assert p.killed is False
        assert p._wait_call_count == 1

    def test_already_exited_process_no_signal_sent(self):
        p = _FakeProcess([7])
        p.wait()
        rc = watchdog._send_graceful_stop(p)
        assert rc == 7
        assert p.sent_signals == []
        assert p.killed is False

    def test_child_exits_after_sigint_is_not_killed(self):
        p = _FakeProcess([0])
        rc = watchdog._send_graceful_stop(p)
        assert rc == 0
        assert p.killed is False

    def test_child_stuck_after_sigint_is_killed_after_grace(self):
        wait_seq = [subprocess.TimeoutExpired(cmd=None, timeout=30)]
        p = _FakeProcess([-9], wait_extras=wait_seq)

        rc = watchdog._send_graceful_stop(p)

        expected_sig = signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
        assert expected_sig in p.sent_signals
        assert p.killed is True
        assert p._wait_call_count >= 2

    def test_process_exits_between_poll_and_signal_no_crash(self):
        polls = [None, 5]
        p = _FakeProcess([5], poll_sequence=polls)
        try:
            rc = watchdog._send_graceful_stop(p)
        except Exception as e:
            pytest.fail(f"_send_graceful_stop raised unexpectedly: {e}")


class TestExitCodesAndRetries:
    def test_exit_code_zero_not_retried_success(self):
        p = _FakeProcess([0])
        popen, remaining = _make_popen([p])
        sleeps = []
        rc = watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=3,
            _popen=popen,
            _sleep=lambda t: sleeps.append(t),
        )
        assert rc == 0
        assert len(remaining) == 0
        assert sleeps == []

    def test_exit_code_two_stops_without_retry(self):
        p = _FakeProcess([2])
        popen, remaining = _make_popen([p])
        sleeps = []
        rc = watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=5,
            _popen=popen,
            _sleep=lambda t: sleeps.append(t),
        )
        assert rc == 2
        assert len(remaining) == 0
        assert sleeps == []

    def test_quick_retryable_crash_increments_consecutive_counter(self):
        procs = [_FakeProcess([1]), _FakeProcess([1]), _FakeProcess([0])]
        popen, _ = _make_popen(procs)
        sleeps = []
        time_events = iter([0, 0, 1, 2, 3, 4])
        rc = watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=5,
            _popen=popen,
            _time=lambda: next(time_events),
            _sleep=lambda t: sleeps.append(t),
        )
        assert rc == 0
        assert len(sleeps) == 2
        assert sleeps[0] == 30
        assert sleeps[1] == 60

    def test_healthy_run_then_crash_resets_counter_and_backoff(self):
        initial_healthy = watchdog.HEALTHY_RUN_RESET_SECONDS + 10
        time_events = iter([
            0, initial_healthy,
            initial_healthy + 1, initial_healthy + 1 + 5,
            initial_healthy + 1 + 5 + 1, initial_healthy + 1 + 5 + 1 + 5,
            initial_healthy + 1 + 5 + 1 + 5 + 1, initial_healthy + 1 + 5 + 1 + 5 + 1 + 500,
        ])

        def time_fn():
            return next(time_events)

        rc_list = [1, 1, 1, 0]
        procs = [_FakeProcess([rc]) for rc in rc_list]
        popen, _ = _make_popen(procs)
        sleeps = []

        rc = watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=4,
            _popen=popen,
            _time=time_fn,
            _sleep=lambda t: sleeps.append(t),
        )
        assert rc == 0
        assert len(sleeps) == 3
        assert sleeps[0] == 30
        assert sleeps[1] == 60
        assert sleeps[2] == 120

    def test_healthy_run_reset_applies_backoff_reset(self):
        initial_healthy = watchdog.HEALTHY_RUN_RESET_SECONDS + 1
        second_healthy_start = initial_healthy + 1 + 1 + 1 + 1
        second_healthy_end = second_healthy_start + initial_healthy
        rc_list = [1, 1, 1, 1, 0]
        procs = [_FakeProcess([rc]) for rc in rc_list]
        popen, _ = _make_popen(procs)

        time_events = iter([
            0, initial_healthy,
            initial_healthy + 1, initial_healthy + 1 + 1,
            initial_healthy + 1 + 1 + 1, initial_healthy + 1 + 1 + 1 + 1,
            second_healthy_start, second_healthy_end,
            second_healthy_end + 1, second_healthy_end + 2,
        ])
        sleeps = []

        rc = watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=5,
            _popen=popen,
            _time=lambda: next(time_events),
            _sleep=lambda t: sleeps.append(t),
        )
        assert rc == 0
        assert sleeps == [30, 60, 120, 30]

    def test_last_failed_attempt_does_not_sleep(self):
        procs = [_FakeProcess([7]), _FakeProcess([7])]
        popen, remaining = _make_popen(procs)
        sleeps = []
        rc = watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=2,
            _popen=popen,
            _sleep=lambda t: sleeps.append(t),
        )
        assert rc == 7
        assert sleeps == [30]

    def test_subprocess_exception_still_gracefully_stops_child(self):
        stop_calls = []
        bad_p = MagicMock()
        bad_p.poll.return_value = None
        bad_p.wait.side_effect = RuntimeError("simulated wait error")
        bad_p.pid = 999

        def bad_popen(cmd, cwd=None, **kw):
            return bad_p

        def fake_stop(proc):
            stop_calls.append(id(proc))
            try:
                proc.kill()
            except Exception:
                pass
            return 1

        rc = watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=1,
            _popen=bad_popen,
            _stop_fn=fake_stop,
        )
        assert stop_calls, "child should be gracefully stopped on exception"


class TestCheckpointResumeFlag:
    def test_resume_flag_not_added_on_first_attempt(self):
        procs = [_FakeProcess([1]), _FakeProcess([0])]
        popen, _ = _make_popen(procs)
        cmds = []

        def capturing_popen(cmd, cwd=None, **kw):
            cmds.append(list(cmd))
            return popen(cmd, cwd=cwd, **kw)

        sleeps = []
        time_events = iter([0, 2, 3, 5])

        watchdog.run_scraper(
            keyword="K", city="C",
            max_attempts=3,
            _popen=capturing_popen,
            _time=lambda: next(time_events),
            _sleep=lambda t: sleeps.append(t),
        )
        assert "--resume" not in cmds[0]
        assert "--resume" in cmds[1]


class TestBuildCmd:
    def test_build_cmd_includes_all_flags_with_timeout(self):
        cmd = watchdog._build_cmd(
            keyword="Restaurants",
            city="NYC",
            max_results=42,
            headless=True,
            no_emails=True,
            timeout=7777,
        )
        assert sys.executable == cmd[0]
        assert "--timeout" in cmd
        assert cmd[cmd.index("--timeout") + 1] == "7777"
        assert "--keyword" in cmd
        assert cmd[cmd.index("--keyword") + 1] == "Restaurants"
        assert "--city" in cmd
        assert cmd[cmd.index("--city") + 1] == "NYC"
        assert "--max-results" in cmd
        assert cmd[cmd.index("--max-results") + 1] == "42"
        assert "--headless" in cmd
        assert "--no-emails" in cmd

    def test_build_cmd_omits_optional_flags_when_falsey(self):
        cmd = watchdog._build_cmd(
            keyword="K", city="C",
            max_results=None,
            headless=False,
            no_emails=False,
            timeout=100,
        )
        assert "--max-results" not in cmd
        assert "--headless" not in cmd
        assert "--no-emails" not in cmd
