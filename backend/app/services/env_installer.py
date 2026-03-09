from __future__ import annotations

import asyncio
import inspect
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Protocol
from uuid import uuid4

from ..executors.builtin_profiles import BUILTIN_PROFILE_PACKAGES
from .no_cuda_guard import find_cuda_violations_in_specs

PackageEventCallback = Callable[[dict], Optional[Awaitable[None] | None]]

_SPEC_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]*(?:[<>=!~].+)?$")


def iso_now() -> str:
	return datetime.now(timezone.utc).isoformat()


def _base_package_name(spec: str) -> str:
	base = re.split(r"[<>=!~]", str(spec), maxsplit=1)[0].strip().lower()
	return base.replace("_", "-")


def _default_allowlist() -> set[str]:
	names: set[str] = set()
	for pkgs in BUILTIN_PROFILE_PACKAGES.values():
		for pkg in pkgs:
			if isinstance(pkg, str) and pkg.strip():
				names.add(_base_package_name(pkg))
	return names


@dataclass
class CommandRunResult:
	returncode: int
	stdout: str
	stderr: str
	duration_ms: int


class CommandRunner(Protocol):
	async def run(
		self,
		command: list[str],
		on_event: Optional[PackageEventCallback] = None,
	) -> CommandRunResult: ...


async def _emit_event(callback: Optional[PackageEventCallback], payload: dict) -> None:
	if callback is None:
		return
	result = callback(payload)
	if inspect.isawaitable(result):
		await result


class AsyncSubprocessRunner:
	async def run(
		self,
		command: list[str],
		on_event: Optional[PackageEventCallback] = None,
	) -> CommandRunResult:
		start = time.perf_counter()
		proc = await asyncio.create_subprocess_exec(
			*command,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE,
		)
		stdout_chunks: list[str] = []
		stderr_chunks: list[str] = []

		async def _pump(stream: asyncio.StreamReader | None, sink: list[str], channel: str) -> None:
			if stream is None:
				return
			while True:
				line = await stream.readline()
				if not line:
					break
				text = line.decode("utf-8", errors="replace").rstrip("\r\n")
				sink.append(text)
				await _emit_event(on_event, {"type": "line", "stream": channel, "line": text})

		await asyncio.gather(
			_pump(proc.stdout, stdout_chunks, "stdout"),
			_pump(proc.stderr, stderr_chunks, "stderr"),
		)
		returncode = await proc.wait()
		duration_ms = int((time.perf_counter() - start) * 1000)
		return CommandRunResult(
			returncode=returncode,
			stdout="\n".join(stdout_chunks),
			stderr="\n".join(stderr_chunks),
			duration_ms=duration_ms,
		)


@dataclass
class InstallAudit:
	attempt_id: str
	started_at: str
	finished_at: str
	command: list[str]
	requested: list[str]
	allowed: list[str]
	blocked: list[str]
	returncode: Optional[int]
	status: str
	duration_ms: int
	stdout_tail: str
	stderr_tail: str


@dataclass
class InstallResult:
	installed: list[str]
	audit: InstallAudit


class EnvInstallError(Exception):
	def __init__(self, code: str, message: str, audit: InstallAudit):
		super().__init__(message)
		self.code = code
		self.audit = audit


class EnvInstallerService:
	def __init__(
		self,
		allowlist: Optional[Iterable[str]] = None,
		runner: Optional[CommandRunner] = None,
		python_executable: Optional[str] = None,
	):
		self.allowlist = {str(p).strip().lower() for p in (allowlist or _default_allowlist()) if str(p).strip()}
		self.runner: CommandRunner = runner or AsyncSubprocessRunner()
		self.python_executable = python_executable or sys.executable

	def _normalize_specs(self, packages: Iterable[str]) -> tuple[list[str], list[str]]:
		allowed: list[str] = []
		blocked: list[str] = []
		seen: set[str] = set()
		for raw in packages:
			spec = str(raw or "").strip()
			if not spec:
				continue
			if not _SPEC_RE.match(spec):
				blocked.append(spec)
				continue
			base = _base_package_name(spec)
			if base not in self.allowlist:
				blocked.append(spec)
				continue
			if find_cuda_violations_in_specs([spec], source="installer"):
				blocked.append(spec)
				continue
			if spec in seen:
				continue
			seen.add(spec)
			allowed.append(spec)
		return allowed, blocked

	def build_install_command(self, packages: Iterable[str]) -> list[str]:
		return [self.python_executable, "-m", "pip", "install", *list(packages)]

	async def install_packages(
		self,
		packages: Iterable[str],
		on_event: Optional[PackageEventCallback] = None,
	) -> InstallResult:
		requested = [str(p).strip() for p in packages if str(p).strip()]
		attempt_id = f"envinst_{uuid4().hex[:12]}"
		started = iso_now()
		start_perf = time.perf_counter()
		allowed, blocked = self._normalize_specs(requested)
		command = self.build_install_command(allowed)
		if blocked:
			audit = InstallAudit(
				attempt_id=attempt_id,
				started_at=started,
				finished_at=iso_now(),
				command=command,
				requested=requested,
				allowed=allowed,
				blocked=blocked,
				returncode=None,
				status="blocked",
				duration_ms=int((time.perf_counter() - start_perf) * 1000),
				stdout_tail="",
				stderr_tail="",
			)
			raise EnvInstallError("ENV_PROFILE_PACKAGE_BLOCKED", "One or more packages are not allowlisted", audit)
		if not allowed:
			audit = InstallAudit(
				attempt_id=attempt_id,
				started_at=started,
				finished_at=iso_now(),
				command=command,
				requested=requested,
				allowed=[],
				blocked=[],
				returncode=0,
				status="noop",
				duration_ms=int((time.perf_counter() - start_perf) * 1000),
				stdout_tail="",
				stderr_tail="",
			)
			return InstallResult(installed=[], audit=audit)

		await _emit_event(on_event, {"type": "started", "attemptId": attempt_id, "packages": allowed})
		run = await self.runner.run(command, on_event=on_event)
		status = "installed" if run.returncode == 0 else "failed"
		audit = InstallAudit(
			attempt_id=attempt_id,
			started_at=started,
			finished_at=iso_now(),
			command=command,
			requested=requested,
			allowed=allowed,
			blocked=[],
			returncode=run.returncode,
			status=status,
			duration_ms=run.duration_ms,
			stdout_tail=(run.stdout or "")[-4000:],
			stderr_tail=(run.stderr or "")[-4000:],
		)
		await _emit_event(
			on_event,
			{"type": "finished", "attemptId": attempt_id, "status": status, "returncode": run.returncode},
		)
		if run.returncode != 0:
			raise EnvInstallError("ENV_PROFILE_INSTALL_FAILED", "pip install failed", audit)
		return InstallResult(installed=allowed, audit=audit)
