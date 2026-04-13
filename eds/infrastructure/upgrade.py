"""Secure binary upgrade service — mirrors internal/upgrade/upgrade.go.

Security properties (identical to the .NET port):
- Archive downloaded as bytes; PGP signature verified in memory before
  any bytes are written to disk (no TOCTOU window).
- TAR extraction skips symlinks and hardlinks; only extracts the EDS binary.
- Zip-slip guard applied for ZIP archives.
- Destination replaced via atomic rename from a sibling temp file.
"""

from __future__ import annotations

import gzip
import io
import logging
import os
import re
import stat
import sys
import tarfile
import zipfile
from pathlib import Path

import aiohttp

from eds.core.retry import execute as retry

_log = logging.getLogger(__name__)

_BINARY_NAMES = {"eds", "eds.exe", "EDS.Cli", "EDS.Cli.exe"}


async def upgrade(
    binary_url: str,
    signature_url: str,
    public_key_armor: str,
    destination: str,
) -> None:
    """Download, PGP-verify, and atomically install a new EDS binary."""

    _log.info("Downloading new binary from %s", binary_url)
    archive_bytes = await _download(binary_url)

    _log.info("Downloading signature from %s", signature_url)
    sig_bytes = await _download(signature_url)

    _log.info("Verifying PGP signature…")
    _verify_signature(archive_bytes, sig_bytes, public_key_armor)

    _log.info("Extracting binary to %s", destination)
    _extract_binary(archive_bytes, destination)

    if not sys.platform.startswith("win"):
        os.chmod(
            destination,
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
            | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH,
        )

    _log.info("Upgrade complete — restart to use the new version")


async def _download(url: str) -> bytes:
    async def _fetch() -> bytes:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as resp:
                resp.raise_for_status()
                return await resp.read()

    return await retry(_fetch, operation_name=f"download {Path(url).name}")


def _verify_signature(archive_bytes: bytes, sig_bytes: bytes, armored_public_key: str) -> None:
    # pgpy 0.6.x uses `imghdr` which was removed in Python 3.13.
    # Import lazily so startup is not affected; a compatible wheel or the
    # `cryptography`-based replacement can be swapped in here when available.
    try:
        import pgpy  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "PGP verification requires pgpy. On Python 3.13+ install a compatible build: "
            "pip install pgpy --pre"
        ) from exc

    key, _ = pgpy.PGPKey.from_blob(armored_public_key)
    sig, _ = pgpy.PGPSignature.from_blob(sig_bytes)
    if sig is None:
        raise PermissionError("PGP signature could not be parsed from the release asset.")
    msg = pgpy.PGPMessage.new(archive_bytes, sensitive=False)
    if not key.verify(msg, sig):
        raise PermissionError(
            "PGP signature verification failed. The binary may have been tampered with."
        )


def _extract_binary(archive_bytes: bytes, destination: str) -> None:
    if len(archive_bytes) < 2:
        raise ValueError("Archive too small to be valid")

    ms = io.BytesIO(archive_bytes)
    magic = archive_bytes[:2]

    if magic == b"PK":
        _extract_zip(ms, destination)
    elif magic == b"\x1f\x8b":
        _extract_tar_gz(ms, destination)
    else:
        # Plain binary
        tmp = destination + ".upgrade.tmp"
        try:
            with open(tmp, "wb") as f:
                f.write(archive_bytes)
            os.replace(tmp, destination)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise


def _extract_zip(source: io.BytesIO, destination: str) -> None:
    dest_dir = str(Path(destination).parent)
    with zipfile.ZipFile(source) as zf:
        target = next(
            (
                e for e in zf.infolist()
                if e.filename.endswith(".exe") or Path(e.filename).name in _BINARY_NAMES
            ),
            zf.infolist()[0] if zf.infolist() else None,
        )
        if target is None:
            raise ValueError("No suitable entry found in ZIP archive")

        # Zip-slip guard
        entry_path = os.path.realpath(os.path.join(dest_dir, target.filename))
        if not entry_path.startswith(os.path.realpath(dest_dir) + os.sep):
            if entry_path != os.path.realpath(destination):
                raise ValueError(f"Zip entry '{target.filename}' would escape destination")

        tmp = destination + ".upgrade.tmp"
        try:
            with zf.open(target) as src, open(tmp, "wb") as dst:
                dst.write(src.read())
            os.replace(tmp, destination)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise


def _extract_tar_gz(source: io.BytesIO, destination: str) -> None:
    with gzip.GzipFile(fileobj=source) as gz, tarfile.open(fileobj=gz) as tf:  # type: ignore[arg-type]
        for member in tf.getmembers():
            if not member.isfile():
                continue
            name = Path(member.name).name
            if name not in _BINARY_NAMES and not name.endswith(".exe"):
                continue

            tmp = destination + ".upgrade.tmp"
            try:
                extracted = tf.extractfile(member)
                if extracted is None:
                    continue
                with open(tmp, "wb") as f:
                    f.write(extracted.read())
                os.replace(tmp, destination)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            return

    raise ValueError("No EDS binary found in the upgrade archive")


def validate_version_string(version: str) -> None:
    if not re.match(r"^[\w.\-]+$", version) or ".." in version:
        raise ValueError(f"Invalid version string: '{version}'")
