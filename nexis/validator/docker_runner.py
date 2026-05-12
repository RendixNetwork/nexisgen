"""GPU-aware Docker runner used by the trainer command."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

logger = logging.getLogger(__name__)


@dataclass
class DockerRunResult:
    success: bool
    returncode: int
    stdout: str
    stderr: str


class DockerGPUPool:
    """Run docker containers concurrently while pinning each to a unique GPU index."""

    def __init__(self, num_gpus: int):
        if num_gpus < 1:
            raise ValueError("num_gpus must be >= 1")
        self._num_gpus = num_gpus
        self._available: asyncio.Queue[int] = asyncio.Queue()
        for idx in range(num_gpus):
            self._available.put_nowait(idx)

    @property
    def num_gpus(self) -> int:
        return self._num_gpus

    async def acquire(self) -> int:
        return await self._available.get()

    async def release(self, gpu_idx: int) -> None:
        await self._available.put(gpu_idx)

    async def run(
        self,
        *,
        image: str,
        command: Sequence[str] | None = None,
        volumes: list[tuple[Path | str, Path | str, str]],
        env: dict[str, str] | None = None,
        shm_size: str | None = None,
        timeout_sec: int | None = None,
        extra_args: Sequence[str] = (),
        pull_policy: str = "always",
    ) -> DockerRunResult:
        gpu_idx = await self.acquire()
        try:
            return await self._run_on_gpu(
                gpu_idx=gpu_idx,
                image=image,
                command=command,
                volumes=volumes,
                env=env,
                shm_size=shm_size,
                timeout_sec=timeout_sec,
                extra_args=extra_args,
                pull_policy=pull_policy,
            )
        finally:
            await self.release(gpu_idx)

    async def _run_on_gpu(
        self,
        *,
        gpu_idx: int,
        image: str,
        command: Sequence[str] | None,
        volumes: list[tuple[Path | str, Path | str, str]],
        env: dict[str, str] | None,
        shm_size: str | None,
        timeout_sec: int | None,
        extra_args: Sequence[str],
        pull_policy: str = "always",
    ) -> DockerRunResult:
        cmd = build_docker_command(
            image=image,
            command=command,
            volumes=volumes,
            env=env,
            shm_size=shm_size,
            gpu_spec=f"device={gpu_idx}",
            extra_args=extra_args,
            pull_policy=pull_policy,
        )
        logger.info("docker run gpu=%d image=%s cmd=%s", gpu_idx, image, " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            logger.error("docker run timeout gpu=%d image=%s", gpu_idx, image)
            proc.kill()
            await proc.wait()
            return DockerRunResult(
                success=False,
                returncode=-1,
                stdout="",
                stderr=f"timeout after {timeout_sec}s",
            )
        rc = proc.returncode if proc.returncode is not None else -1
        stdout = stdout_b.decode("utf-8", errors="replace")
        stderr = stderr_b.decode("utf-8", errors="replace")
        if rc != 0:
            logger.warning(
                "docker run failed gpu=%d image=%s rc=%d\nstderr:\n%s\nstdout:\n%s",
                gpu_idx,
                image,
                rc,
                stderr,
                stdout[-2000:],
            )
        return DockerRunResult(success=rc == 0, returncode=rc, stdout=stdout, stderr=stderr)


def build_docker_command(
    *,
    image: str,
    command: Sequence[str] | None,
    volumes: list[tuple[Path | str, Path | str, str]],
    env: dict[str, str] | None,
    shm_size: str | None,
    gpu_spec: str = "all",
    extra_args: Sequence[str] = (),
    pull_policy: str = "always",
) -> list[str]:
    """Build a `docker run` argv list.

    Hardcoded `pull_policy="always"` (passed as `docker run --pull always`):
    docker checks the registry's manifest on every run and pulls only the
    layers that changed. If the local digest already matches the remote, no
    layers are downloaded — the cost is just one manifest round-trip
    (~1-3s). This guarantees that `rendixnetwork/train:latest` and
    `rendixnetwork/vbench:latest` stay current without any external auto-update
    mechanism. The argument is exposed only so tests can override it.
    """
    cmd = ["docker", "run", "--rm", "--gpus", gpu_spec]
    if pull_policy:
        cmd.extend(["--pull", pull_policy])
    if shm_size:
        cmd.extend(["--shm-size", shm_size])
    for src, dst, mode in volumes:
        suffix = f":{mode}" if mode else ""
        cmd.extend(["-v", f"{src}:{dst}{suffix}"])
    for key, value in (env or {}).items():
        cmd.extend(["-e", f"{key}={value}"])
    cmd.extend(extra_args)
    cmd.append(image)
    if command:
        cmd.extend(command)
    return cmd


async def run_docker_one_off(
    *,
    image: str,
    command: Sequence[str] | None = None,
    volumes: list[tuple[Path | str, Path | str, str]],
    env: dict[str, str] | None = None,
    shm_size: str | None = None,
    gpu_spec: str = "all",
    timeout_sec: int | None = None,
    extra_args: Sequence[str] = (),
    pull_policy: str = "always",
) -> DockerRunResult:
    """Run a single docker container synchronously (no GPU pool)."""
    cmd = build_docker_command(
        image=image,
        command=command,
        volumes=volumes,
        env=env,
        shm_size=shm_size,
        gpu_spec=gpu_spec,
        extra_args=extra_args,
        pull_policy=pull_policy,
    )
    logger.info("docker run (one-off) image=%s gpus=%s cmd=%s", image, gpu_spec, " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        logger.error("docker one-off timeout image=%s", image)
        proc.kill()
        await proc.wait()
        return DockerRunResult(success=False, returncode=-1, stdout="", stderr="timeout")
    rc = proc.returncode if proc.returncode is not None else -1
    stdout = stdout_b.decode("utf-8", errors="replace")
    stderr = stderr_b.decode("utf-8", errors="replace")
    return DockerRunResult(success=rc == 0, returncode=rc, stdout=stdout, stderr=stderr)
