from __future__ import annotations

import pytest

from app.services.env_installer import (
	CommandRunResult,
	EnvInstallError,
	EnvInstallerService,
)


class FakeRunner:
	def __init__(self, result: CommandRunResult):
		self.result = result
		self.commands: list[list[str]] = []

	async def run(self, command: list[str], on_event=None) -> CommandRunResult:
		self.commands.append(list(command))
		return self.result


def test_build_install_command_uses_python_and_pip():
	svc = EnvInstallerService(allowlist={"numpy"}, python_executable="/usr/bin/python3")
	cmd = svc.build_install_command(["numpy"])
	assert cmd == ["/usr/bin/python3", "-m", "pip", "install", "numpy"]


@pytest.mark.asyncio
async def test_install_packages_rejects_non_allowlisted_specs():
	runner = FakeRunner(CommandRunResult(returncode=0, stdout="", stderr="", duration_ms=1))
	svc = EnvInstallerService(allowlist={"numpy"}, runner=runner, python_executable="python")
	with pytest.raises(EnvInstallError) as exc_info:
		await svc.install_packages(["numpy", "totally-unknown-lib"])
	assert exc_info.value.code == "ENV_PROFILE_PACKAGE_BLOCKED"
	assert "totally-unknown-lib" in exc_info.value.audit.blocked
	assert runner.commands == []


@pytest.mark.asyncio
async def test_install_packages_rejects_cuda_linked_specs():
	runner = FakeRunner(CommandRunResult(returncode=0, stdout="", stderr="", duration_ms=1))
	svc = EnvInstallerService(allowlist={"torch"}, runner=runner, python_executable="python")
	with pytest.raises(EnvInstallError) as exc_info:
		await svc.install_packages(["torch==2.4.0+cu124"])
	assert exc_info.value.code == "ENV_PROFILE_PACKAGE_BLOCKED"
	assert "torch==2.4.0+cu124" in exc_info.value.audit.blocked
	assert runner.commands == []


@pytest.mark.asyncio
async def test_install_packages_rejects_excluded_cuda_only_extras():
	runner = FakeRunner(CommandRunResult(returncode=0, stdout="", stderr="", duration_ms=1))
	svc = EnvInstallerService(runner=runner, python_executable="python")
	with pytest.raises(EnvInstallError) as exc_info:
		await svc.install_packages(["bitsandbytes==0.44.1"])
	assert exc_info.value.code == "ENV_PROFILE_PACKAGE_BLOCKED"
	assert "bitsandbytes==0.44.1" in exc_info.value.audit.blocked
	assert runner.commands == []


@pytest.mark.asyncio
async def test_install_packages_maps_runner_failure_to_install_error():
	runner = FakeRunner(
		CommandRunResult(returncode=1, stdout="line1", stderr="pip failed", duration_ms=25)
	)
	svc = EnvInstallerService(allowlist={"numpy"}, runner=runner, python_executable="python")
	with pytest.raises(EnvInstallError) as exc_info:
		await svc.install_packages(["numpy"])
	assert exc_info.value.code == "ENV_PROFILE_INSTALL_FAILED"
	assert exc_info.value.audit.returncode == 1
	assert "pip failed" in exc_info.value.audit.stderr_tail
	assert runner.commands == [["python", "-m", "pip", "install", "numpy"]]
