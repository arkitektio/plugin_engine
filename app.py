"""The plugin engine: a kabinet *backend* that runs app flavours as Docker containers.

It registers one backend and one resource (the host whose Docker daemon it is given),
evaluates a flavour's selectors against the host's real hardware (the kabinet
server stores selectors but deliberately does not evaluate them, see the server's
``docs/selectors.md``), and reports every container it owns back to kabinet as a pod.

Approvals and credentials
-------------------------
The deployer installs only what a user pre-approved. Approving a release in the UI
creates a lok agent **mandate** (the user lets this deployer app issue keys that run that
exact release *as them*) and a kabinet **release approval** pointing at it, pinned to the
release's digest. Every install names an approval; kabinet refuses a deployment whose approval is
revoked, stale (the release was re-published differently) or not meant for this backend.

Right before ``docker run`` the deployer issues a **mandate key** for the container under
the approval's mandate (``issueChildMandateKey``): a child mandate of the approver's,
pinned to the approved identifier, version, scopes and requirements and to a device id
unique to this container (instances on one host would otherwise share a client). The
container logs in with it (``arkitekt run prod`` reads ``FAKTS_MANDATE_KEY``) and runs the
grant again whenever its access token expires, acting as the approver; lok checks the pin
and the mandate at every login. Once the app has logged in the monitor records its client
id on the pod. Removing a pod revokes its key, which deletes its client; revoking the
approval's mandate revokes every key issued under it.

State
-----
Every container belongs to a pod that exists before it: the deployer registers the pod
under a local id it chooses, then starts the container carrying that id as a label. The
server's pod (``status``, ``client_id``) is the record of what has been reported and
verified; the container labels carry the pod, deployment, flavour and key's mandate ids.

Docker events only update statuses. Removing and settling is the sweep's alone: every
``DEPLOYER_RECONCILE_SECONDS`` (and at start, which re-adopts what a previous run left
behind) it compares all containers of this host with all pods of this backend, both
ways, and acts only on what stayed unmatched for a grace period. A failed pod says why
in its logs. Images run only by digest, so what runs is what was approved.

Running in a container
----------------------
The engine needs the Docker socket, so it is not installed through kabinet: whoever runs
it (konstruktor, beside a hub) mounts the socket and sets the environment below. Two
values must then be set, because their defaults change whenever the container is
recreated: ``ARKITEKT_INSTANCE_ID`` (the hostname is the container id, and every plugin
labelled with the old one would be taken for someone else's) and ``ARKITEKT_DEVICE_ID``.
konstruktor sets both for every app it attaches.
The host is described from ``docker info``, which is the daemon's host. GPUs are read
with ``nvidia-smi``, and where the engine's own image has none, inside a throwaway
container the NVIDIA runtime hands it to.

Configuration (environment)
---------------------------
``ARKITEKT_INSTANCE_ID``       label value that marks containers as ours (default:
                               ``KABINET_DEPLOYER_ID``, then the hostname)
``ARKITEKT_GATEWAY``           the fakts URL handed to containers (default: go.arkitekt.live)
``ARKITEKT_NETWORK``           docker network containers join (default: next_default)
``FAKTS_ALLOW_INSECURE_TRANSPORT`` lets the engine log in over plain HTTP, and is passed on
                               to every app it starts (a hub's gateway on its own network)
``ARKITEKT_NETWORK_MODE``      a docker network mode for containers instead, e.g.
                               ``container:<mesh sidecar>`` to share its tailnet (unset)
``DEPLOYER_GPU_PROBE_IMAGE``   the image the GPU probe runs nvidia-smi in, when this
                               engine has none of its own (default: debian:stable-slim)
``DEPLOYER_ALLOWED_SCOPES``    comma list; operator veto on top of the approval: a flavour
                               requesting a scope outside it is refused (unset: allow all)
``DEPLOYER_ACCEPT_APPROVERS``  comma list of user subs whose approvals this host runs
                               (unset: any member of the organization)
``DEPLOYER_RECONCILE_SECONDS`` full container sweep interval behind the event stream (60)
``DEPLOYER_ORPHAN_GRACE_SECONDS`` how long a container without a pod, or a pod without a
                               container, is left alone before the sweep acts on it (120)
``DEPLOYER_UPGRADE_WAIT_SECONDS`` how long ``upgrade`` waits for the new app to enroll (300)
"""

from __future__ import annotations

import datetime
import logging
import os
import queue
import random
import re
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Annotated, Any, Iterable

import docker
import docker.errors
import docker.types
import koil
from docker import DockerClient
from docker.models.containers import Container
from docker.utils import parse_repository_tag
from koil.errors import ThreadCancelledError

from arkitekt import App, AssignPrompt, DeniedError, DescriptorOperator, Effects, Requires, run
from arkitekt.device_id import get_or_set_device_id
from kabinet import Kabinet
from rekuest.api.schema import Agent
from rekuest.arkitekt import SUPERVISOR, rekuest_service
from rekuest.client.client import Rekuest
from rekuest.protocol.schema import ImplementationAgentFilter, ImplementationFilter, OffsetPaginationInput
from kabinet.callers import REQUEST_APPROVAL
from kabinet import kabinet_service
from kabinet.api.schema import (
    Backend,
    Definition,
    Flavour,
    FlavourFilter,
    FlavourOrder,
    ListFlavour,
    Ordering,
    Pod,
    PodStatus,
    QualifierInput,
    ReleaseApproval,
    Resource,
)
from arkitekt import LogLevel
from arkitekt import Task
from unlok import Unlok
from unlok import unlok_service
from unlok.api.schema import IssuedMandateKey

logger = logging.getLogger(__name__)

# The version of the app. A release is the commit tagged with it (v1.2.3).
__version__ = "3.0.0"

AGENT_IDENTIFIER = "plugin_engine"
"""The app identifier a mandate must name for this deployer to issue keys under it."""

app = App(
    AGENT_IDENTIFIER,
    __version__,
    author="jhnnsrs",
    # No scopes of its own: it asks for what its kabinet and rekuest clients need (their SCOPES).
    description="Runs approved app releases as Docker containers on this host.",
    services=[kabinet_service, unlok_service, rekuest_service],
)

# --- logos ------------------------------------------------------------------------

# The app's logo, in colours of its own: app containers fitted together like puzzle
# pieces, with the engine's gear turning beside them.
app.blok(
    "plugin_engine_logo",
    """
    <AgentLogo viewBox="0 0 64 64" title="Plugin engine">
      <Defs>
        <LinearGradient id="ground" x1="0" y1="0" x2="1" y2="1">
          <Stop offset="0" stopColor="ink" />
          <Stop offset="1" stopColor="spectrum-violet" />
        </LinearGradient>
        <ClipPath id="tile"><Rect x="0" y="0" width="64" height="64" rx="14" /></ClipPath>
      </Defs>
      <G clipPath="url(#tile)">
        <Rect x="0" y="0" width="64" height="64" fill="url(#ground)" />
        <G animate="spin">
          <Circle cx="50" cy="14" r="7" fill="none" stroke="muted" strokeWidth="4" strokeDasharray="2.75 2.75" />
          <Circle cx="50" cy="14" r="6" fill="muted" />
          <Circle cx="50" cy="14" r="2.4" fill="ink" />
        </G>
        <Rect x="33" y="38" width="22" height="16" rx="2" fill="spectrum-violet" />
        <Path d="M38 41v10M43 41v10M48 41v10" stroke="spectrum-violet" strokeOpacity="0.35" strokeWidth="1.6" strokeLinecap="round" />
        <Rect x="9" y="38" width="22" height="16" rx="2" fill="spectrum-cyan" />
        <Circle cx="32" cy="46" r="3.6" fill="spectrum-cyan" />
        <Path d="M14 41v10M19 41v10M24 41v10" stroke="ink" strokeOpacity="0.35" strokeWidth="1.6" strokeLinecap="round" />
        <Rect x="21" y="20" width="22" height="16" rx="2" fill="spectrum-green" />
        <Circle cx="32" cy="36.5" r="3.6" fill="spectrum-green" />
        <Path d="M26 23v10M31 23v10M36 23v10" stroke="ink" strokeOpacity="0.35" strokeWidth="1.6" strokeLinecap="round" />
      </G>
      <Rect x="0.5" y="0.5" width="63" height="63" rx="13.5" fill="none" stroke="spectrum-violet" strokeOpacity="0.3" strokeWidth="1" />
    </AgentLogo>
    """,
    description="App containers fitted together like puzzle pieces, beside a turning gear.",
    default=True,
)

# The actions' logos, in the viewer's palette. Strokes are ink and round unless a shape
# says otherwise; fills are off, since SVG's own default fill is black.
_PEN = 'fill="none" stroke="ink" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"'


def _container(x: float, y: float, width: float, height: float, fill: str = "surface") -> str:
    """An app's container: a box with ribs, the way the app logo draws one."""
    ribs = "".join(f"M{x + width * share:.1f} {y + 2.5}v{height - 5}" for share in (0.3, 0.5, 0.7))
    return (
        f'<Rect x="{x}" y="{y}" width="{width}" height="{height}" rx="1.5" fill="{fill}" />'
        f'<Path d="{ribs}" stroke="muted" strokeWidth="1.4" />'
    )


def _action_logo(name: str, title: str, body: str) -> str:
    """Declare the action logo ``name`` that draws ``body`` on a 24 px grid; returns the name."""
    app.blok(
        name,
        f'<ActionLogo viewBox="0 0 24 24" title="{title}"><G {_PEN}>{body}</G></ActionLogo>',
        description=f"{title}.",
    )
    return name


INSTALL_LOGO = _action_logo(
    "plugin_engine_install_logo",
    "Install an approved release",
    _container(3, 11, 18, 10)
    + '<G animate="float"><Path d="M12 2.5v7.5M8.5 6.5l3.5 3.5 3.5-3.5" stroke="brand-strong" strokeWidth="2" /></G>',
)
DEPLOY_FLAVOUR_LOGO = _action_logo(
    "plugin_engine_deploy_flavour_logo",
    "Deploy one flavour",
    _container(2.5, 9, 15, 11)
    + '<Path d="M14.5 2.5h5.5a1.5 1.5 0 0 1 1.5 1.5v5.5l-5.5 5.5-7-7z" fill="brand" />'
    + '<Circle cx="18.2" cy="5.8" r="1.2" fill="surface" stroke="none" animate="pulse" />',
)
AUTO_INSTALL_LOGO = _action_logo(
    "plugin_engine_auto_install_logo",
    "Install whatever provides a definition",
    _container(3, 12, 18, 9.5)
    + '<Path d="M12 2l1.2 3.3 3.3 1.2-3.3 1.2L12 11l-1.2-3.3L7.5 6.5l3.3-1.2z" fill="brand" stroke="none" animate="pulse" />'
    + '<Path d="M19 2l.6 1.4 1.4.6-1.4.6-.6 1.4-.6-1.4-1.4-.6 1.4-.6z" fill="brand-strong" stroke="none" animate="blink" />'
    + '<Path d="M4.5 5l.6 1.4 1.4.6-1.4.6-.6 1.4-.6-1.4L2.5 7l1.4-.6z" fill="brand-strong" stroke="none" animate="blink" animateDelay="0.4" />',
)
REFRESH_LOGS_LOGO = _action_logo(
    "plugin_engine_refresh_logs_logo",
    "Refresh a pod's logs",
    '<Path d="M4.5 2.5h9l4.5 4.5v6" fill="surface" /><Path d="M4.5 2.5v19h8" />'
    + '<Path d="M13.5 2.5V7H18" /><Path d="M7.5 10h7M7.5 13h5M7.5 16h3" stroke="muted" strokeWidth="1.4" />'
    + '<G animate="spin"><Path d="M20.6 15.4a4 4 0 1 0 .4 3.6" stroke="brand-strong" />'
    + '<Path d="M21 12.5v3h-3" stroke="brand-strong" /></G>',
)
STOP_LOGO = _action_logo(
    "plugin_engine_stop_logo",
    "Stop a pod's container",
    _container(2.5, 5, 15, 11)
    + '<Circle cx="17.5" cy="17.5" r="5" fill="surface" />'
    + '<Rect x="15.6" y="15.6" width="3.8" height="3.8" rx="0.6" fill="brand-strong" stroke="none" animate="pulse" />',
)
RESTART_LOGO = _action_logo(
    "plugin_engine_restart_logo",
    "Restart a pod's container",
    '<Rect x="8" y="9" width="8" height="6" rx="1" fill="brand-soft" strokeWidth="1.5" />'
    + '<G animate="spin"><Path d="M20.5 12a8.5 8.5 0 1 1-2.5-6" stroke="brand-strong" strokeWidth="2" />'
    + '<Path d="M18.5 2v4h-4" stroke="brand-strong" strokeWidth="2" /></G>',
)
START_LOGO = _action_logo(
    "plugin_engine_start_logo",
    "Start a stopped container",
    _container(2.5, 5, 15, 11)
    + '<Circle cx="17.5" cy="17.5" r="5" fill="surface" />'
    + '<Path d="M16 15.2v4.6l3.8-2.3z" fill="brand-strong" stroke="none" animate="pulse" />',
)
REAUTHORIZE_LOGO = _action_logo(
    "plugin_engine_reauthorize_logo",
    "Give a pod a fresh key",
    '<Path d="M3.5 8.5A9.5 9.5 0 0 1 19 5.5" stroke="brand-strong" animate="draw" />'
    + '<Path d="M19.6 2.2v3.6H16" stroke="brand-strong" />'
    + '<Circle cx="7.5" cy="15" r="4" fill="brand-soft" />'
    + '<Circle cx="7.5" cy="15" r="1.3" fill="surface" strokeWidth="1.4" />'
    + '<Path d="M11.5 15h10M18.5 15v3M15.5 15v2.5" />',
)
REMOVE_LOGO = _action_logo(
    "plugin_engine_remove_logo",
    "Remove a pod",
    '<Path d="M5.5 7l1 13.5A1.5 1.5 0 0 0 8 22h8a1.5 1.5 0 0 0 1.5-1.5l1-13.5" fill="surface" />'
    + '<Path d="M10 11v7M14 11v7" stroke="brand" />'
    + '<G animate="float"><Path d="M3.5 7h17" /><Path d="M9 7V4.5a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1V7" /></G>',
)
UPGRADE_LOGO = _action_logo(
    "plugin_engine_upgrade_logo",
    "Upgrade a pod to a newer release",
    _container(3, 11, 18, 10)
    + '<G animate="float"><Path d="M12 10V2.5M8.5 6L12 2.5 15.5 6" stroke="brand-strong" strokeWidth="2" /></G>',
)

# --- configuration ------------------------------------------------------------

DEPLOYER_ID = (
    os.getenv("ARKITEKT_INSTANCE_ID")
    or os.getenv("KABINET_DEPLOYER_ID")
    or socket.gethostname()
)
ARKITEKT_GATEWAY = os.getenv("ARKITEKT_GATEWAY", "https://go.arkitekt.live")
ARKITEKT_NETWORK = os.getenv("ARKITEKT_NETWORK", "next_default")
ARKITEKT_NETWORK_MODE = os.getenv("ARKITEKT_NETWORK_MODE") or None
GPU_PROBE_IMAGE = os.getenv("DEPLOYER_GPU_PROBE_IMAGE", "debian:stable-slim")
#: Passed on to every app: a hub reached over plain HTTP (its gateway, on its own docker
#: network) is one the engine was told it may send a credential to, and so are its apps.
INSECURE_TRANSPORT = os.getenv("FAKTS_ALLOW_INSECURE_TRANSPORT") or None
_allowed = {s.strip() for s in os.getenv("DEPLOYER_ALLOWED_SCOPES", "").split(",") if s.strip()}
ALLOWED_SCOPES: frozenset[str] | None = frozenset(_allowed) if _allowed else None
_approvers = {s.strip() for s in os.getenv("DEPLOYER_ACCEPT_APPROVERS", "").split(",") if s.strip()}
ACCEPT_APPROVERS: frozenset[str] | None = frozenset(_approvers) if _approvers else None
RECONCILE_SECONDS = float(os.getenv("DEPLOYER_RECONCILE_SECONDS", "60"))
ORPHAN_GRACE_SECONDS = float(os.getenv("DEPLOYER_ORPHAN_GRACE_SECONDS", "120"))
UPGRADE_WAIT_SECONDS = float(os.getenv("DEPLOYER_UPGRADE_WAIT_SECONDS", "300"))
PAGE_SIZE = 100

LABEL = "arkitekt.live.kabinet"
LABEL_POD = f"{LABEL}.pod"
LABEL_DEPLOYMENT = f"{LABEL}.deployment"
LABEL_FLAVOUR = f"{LABEL}.flavour"
LABEL_APP = f"{LABEL}.app"
LABEL_VERSION = f"{LABEL}.version"
LABEL_APPROVAL = f"{LABEL}.approval"
LABEL_INSTANCE = f"{LABEL}.instance_device"
#: The child mandate whose key the container logs in with; revoking it retires the app.
LABEL_KEY_MANDATE = f"{LABEL}.key_mandate"

RESTART_POLICY = {"Name": "on-failure", "MaximumRetryCount": 5}
LOG_TAIL = 200


class PlacementError(Exception):
    """A flavour cannot (or must not) be placed on this host."""


# --- hardware → qualifiers ------------------------------------------------------

_ARCH = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}


_NVIDIA_QUERY = ["nvidia-smi", "--query-gpu=count,memory.total,compute_cap", "--format=csv,noheader,nounits"]


def _nvidia_smi_here() -> str | None:
    """The query's output from this machine's nvidia-smi, or None when there is none."""
    try:
        return subprocess.run(_NVIDIA_QUERY, capture_output=True, text=True, timeout=10, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return None


def _nvidia_smi_in_container(client: DockerClient) -> str | None:
    """The query's output from a throwaway container given every GPU, or None.

    The engine's own image has no nvidia-smi, but the NVIDIA container runtime mounts
    the driver's into any container that asks for the ``utility`` capability. The binary
    is linked against glibc, so the probe image must not be Alpine.
    """
    try:
        out = client.containers.run(
            GPU_PROBE_IMAGE,
            command=_NVIDIA_QUERY,
            remove=True,
            device_requests=[docker.types.DeviceRequest(count=-1, capabilities=[["gpu", "utility"]])],
        )
    except docker.errors.DockerException as e:
        logger.warning("could not probe the GPUs in %s: %s", GPU_PROBE_IMAGE, e)
        return None
    return out.decode() if isinstance(out, bytes) else str(out)


def _nvidia_qualifiers(out: str | None) -> dict[str, str]:
    """What the NVIDIA driver reported, as qualifiers. Empty when nothing was reported.

    One line per GPU: ``count, memory.total (MiB), compute_cap``. Memory and compute
    capability are reduced with ``min`` so a required selector is only satisfied when
    *every* GPU satisfies it — a container asking for all GPUs gets the weakest one too.
    """
    if not out:
        return {}
    rows = [[c.strip() for c in line.split(",")] for line in out.splitlines() if line.strip()]
    if not rows:
        return {}
    try:
        return {
            "gpu_count": str(len(rows)),
            "gpu_memory_mb": str(min(int(float(r[1])) for r in rows)),
            "compute_capability": min((r[2] for r in rows), key=_version_tuple),
        }
    except (IndexError, ValueError):
        return {}


def _host_qualifiers(client: DockerClient) -> dict[str, str]:
    """Describe this host from ``docker info`` (plus nvidia-smi when the runtime exists)."""
    info = client.info()
    arch = str(info.get("Architecture", ""))
    qualifiers = {
        "arch": _ARCH.get(arch, arch),
        "cpu_count": str(info.get("NCPU", 0)),
        "memory_mb": str(int(info.get("MemTotal", 0)) // (1024 * 1024)),
        "os": str(info.get("OSType", "linux")),
        "hostname": str(info.get("Name", DEPLOYER_ID)),
    }
    has_gpu = "nvidia" in (info.get("Runtimes") or {})
    qualifiers["gpu"] = "true" if has_gpu else "false"
    if has_gpu:
        qualifiers.update(_nvidia_qualifiers(_nvidia_smi_here() or _nvidia_smi_in_container(client)))
    return qualifiers


def _qualifier_dict(qualifiers: Any) -> dict[str, str]:
    """``Resource.qualifiers`` comes back as a list of ``{"key", "value"}`` dicts."""
    return {q["key"]: q["value"] for q in (qualifiers or []) if "key" in q}


# --- selector evaluation ------------------------------------------------------------


def _version_tuple(text: str) -> tuple[int, ...]:
    return tuple(int(p) for p in re.findall(r"\d+", text or ""))


def _selector_verdict(selector: Any, q: dict[str, str]) -> bool | None:
    """Whether the host described by ``q`` satisfies ``selector``.

    ``None`` means this deployer cannot evaluate the selector against what the host
    reports; for a *required* selector that is a refusal, never a guess.
    """
    kind = selector.kind
    if kind == "label":
        if selector.key not in q:
            return False
        return selector.value is None or q[selector.key] == selector.value
    if kind == "cpu":
        if getattr(selector, "frequency", None) is not None:
            return None  # docker info does not report clock speed
        if selector.arch and q.get("arch") != selector.arch:
            return False
        if selector.min_count is not None and int(q.get("cpu_count", 0)) < selector.min_count:
            return False
        return True
    if kind == "ram":
        return selector.min is None or int(q.get("memory_mb", 0)) >= selector.min
    if kind == "cuda":
        if q.get("gpu") != "true":
            return False
        if selector.cuda_version is not None or selector.cuda_cores is not None:
            return None  # not reported by nvidia-smi's query interface
        if selector.count is not None:
            if "gpu_count" not in q:
                return None
            if int(q["gpu_count"]) < selector.count:
                return False
        if selector.memory is not None:
            if "gpu_memory_mb" not in q:
                return None
            if int(q["gpu_memory_mb"]) < selector.memory:
                return False
        if selector.compute_capability:
            if "compute_capability" not in q:
                return None
            if _version_tuple(q["compute_capability"]) < _version_tuple(selector.compute_capability):
                return False
        return True
    return None  # rocm, oneapi, unknown kinds


@dataclass
class Placement:
    satisfied: bool
    score: int
    unenforceable: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


def _placement(flavour: ListFlavour, q: dict[str, str]) -> Placement:
    """Apply the selector contract: required selectors filter, weights rank."""
    placement = Placement(satisfied=True, score=0)
    for selector in flavour.selectors:
        verdict = _selector_verdict(selector, q)
        if selector.required:
            if verdict is None:
                placement.unenforceable.append(selector.kind)
                placement.satisfied = False
            elif verdict is False:
                placement.failed.append(selector.kind)
                placement.satisfied = False
        elif verdict:
            placement.score += selector.weight
    return placement


def _select_resource(ctx: DeployerContext, flavour: ListFlavour) -> Resource:
    """Pick the resource for ``flavour`` or raise :class:`PlacementError`."""
    candidates: list[tuple[int, Resource]] = []
    unenforceable: set[str] = set()
    failed: set[str] = set()
    for resource, q in ctx.resources:
        placement = _placement(flavour, q)
        unenforceable.update(placement.unenforceable)
        failed.update(placement.failed)
        if placement.satisfied:
            candidates.append((placement.score, resource))
    if unenforceable:
        raise PlacementError(
            f"Flavour '{flavour.name}' requires selector kind(s) {sorted(unenforceable)} that this "
            "deployer cannot evaluate against what the host reports; refusing to place it rather than guess."
        )
    if not candidates:
        raise PlacementError(
            f"No resource on this host satisfies the required selector(s) {sorted(failed)} of flavour '{flavour.name}'."
        )
    best = max(score for score, _ in candidates)
    return random.choice([resource for score, resource in candidates if score == best])


def _device_requests(flavour: ListFlavour) -> list[docker.types.DeviceRequest]:
    requests = []
    for selector in flavour.selectors:
        if selector.kind == "cuda":
            # An explicit count is honoured; unset keeps the "--gpus all" behaviour.
            requests.append(docker.types.DeviceRequest(count=selector.count or -1, capabilities=[["gpu"]]))
    return requests


def _flavour_scopes(flavour: ListFlavour) -> list[str]:
    manifest = flavour.manifest if isinstance(flavour.manifest, dict) else {}
    return [str(s) for s in (manifest.get("scopes") or [])]


def _check_image(flavour: ListFlavour) -> None:
    """Only a digest-pinned image runs: an approval pins the release's digest, and a tag
    could move to other content after that."""
    image = flavour.image.image_string
    if "@sha256:" not in image:
        raise PlacementError(
            f"Flavour '{flavour.name}' names its image by tag ({image}), not by digest. "
            "A tag can move after the release was approved, so this host only runs "
            "digest-pinned images (name@sha256:...)."
        )


def _check_scopes(flavour: ListFlavour) -> list[str]:
    """The scopes the container will request; refused when outside the allowlist."""
    scopes = _flavour_scopes(flavour)
    if ALLOWED_SCOPES is not None:
        extra = sorted(set(scopes) - ALLOWED_SCOPES)
        if extra:
            raise PlacementError(
                f"Flavour '{flavour.name}' requests scope(s) {', '.join(extra)} outside DEPLOYER_ALLOWED_SCOPES."
            )
    return scopes


def _check_approval(ctx: "DeployerContext", approval: ReleaseApproval) -> None:
    """Refuse early what kabinet and lok would refuse anyway, with a clearer reason."""
    name = f"{approval.release.app.identifier}:{approval.release.version}"
    if not approval.is_active:
        state = "revoked" if approval.revoked_at else "stale (the release changed since it was approved)"
        raise PlacementError(f"The approval of {name} is {state}.")
    if approval.agent != AGENT_IDENTIFIER:
        raise PlacementError(f"The approval of {name} is for the agent '{approval.agent}', not this deployer.")
    if approval.backends and ctx.backend.id not in {b.id for b in approval.backends}:
        raise PlacementError(f"The approval of {name} does not allow this backend.")
    if ACCEPT_APPROVERS is not None and approval.approver.sub not in ACCEPT_APPROVERS:
        raise PlacementError(f"This host does not run apps approved by '{approval.approver.sub}' (DEPLOYER_ACCEPT_APPROVERS).")


def _check_mandate(unlok: Unlok, approval: ReleaseApproval) -> None:
    """The kabinet approval must be backed by the lok mandate it names.

    kabinet stores the mandate id it is given, and lok's pin covers identity, scopes and
    requirements but not image content. So the deployer checks that the mandate is
    live, names this deployer, was granted by the approver, and carries the approved
    release's digest. Otherwise an approval could borrow someone else's mandate, or one
    granted for different image content.
    """
    release = approval.release
    name = f"{release.app.identifier}:{release.version}"
    try:
        mandate = unlok.get_mandate(approval.mandate_id)
    except Exception as e:  # noqa: BLE001 — revoked mandates stay readable; missing ones do not
        raise PlacementError(f"The mandate behind the approval of {name} cannot be read: {e}") from e
    subject = mandate.subject
    problems = []
    if not mandate.is_live:
        problems.append("it is revoked or expired")
    if mandate.agent_identifier != AGENT_IDENTIFIER:
        problems.append(f"it names the agent '{mandate.agent_identifier}'")
    if str(mandate.grantor.id) != str(approval.approver.sub):
        problems.append("it was granted by someone other than the approver")
    if (subject.identifier, subject.version) != (release.app.identifier, release.version):
        problems.append(f"it is for {subject.identifier}:{subject.version}")
    if mandate.attestation != approval.digest:
        problems.append("it attests a different release digest")
    if problems:
        raise PlacementError(f"The mandate behind the approval of {name} does not match: {'; '.join(problems)}.")


def _usable_approvals(kabinet: Kabinet, ctx: "DeployerContext") -> list[ReleaseApproval]:
    """Every approval this host may install under right now."""
    usable = []
    for approval in kabinet.list_release_approvals():
        try:
            _check_approval(ctx, approval)
        except PlacementError:
            continue
        usable.append(approval)
    return usable


def _approval_from_callers(rekuest: Rekuest, task: Task, release: Any) -> ReleaseApproval:
    """Ask whoever called this task to approve ``release`` for this engine.

    Only a person can approve (a lok mandate is theirs to grant), so the engine asks the app
    they called it from: the nearest caller offering ``REQUEST_APPROVAL`` (orkestrator, when
    it is engaged). The call is an ordinary child: unless the run is in auto-mode, the person
    first agrees to be asked. What comes back is checked like any other approval.
    """
    name = f"{release.app.identifier}:{release.version}"
    callers = rekuest.list_implementations(
        filters=ImplementationFilter(
            agent=ImplementationAgentFilter(caller_of=task.id), action_demand=REQUEST_APPROVAL, active=True
        ),
        pagination=OffsetPaginationInput(limit=1, offset=0),
    )
    if not callers:
        raise PlacementError(
            f"{name} is not approved for {AGENT_IDENTIFIER}, and nothing that called this can ask for it: "
            "approve it in orkestrator, then install again."
        )
    prompt = AssignPrompt(
        title=f"Approve {name}",
        description=f"{AGENT_IDENTIFIER} needs your approval to run {name}; it then acts as you.",
    )
    try:
        return task.call(callers[0], str(release.id), AGENT_IDENTIFIER, prompt=prompt)
    except DeniedError as denied:
        said = f": {denied.answer}" if denied.answer else "."
        raise PlacementError(f"Approving {name} was declined{said}") from denied


# --- context, health & startup ----------------------------------------------------------


@app.context
@dataclass
class DeployerContext:
    docker: DockerClient
    backend: Backend
    resources: list[tuple[Resource, dict[str, str]]]
    device_id: str
    gateway: str = ARKITEKT_GATEWAY
    network: str = ARKITEKT_NETWORK
    network_mode: str | None = ARKITEKT_NETWORK_MODE

    @property
    def resource(self) -> Resource:
        return self.resources[0][0]


@app.state(name="health")
class DeployerHealth:
    """How the monitor last found this host.

    ``last_sweep_at`` advances once per sweep; when it stops advancing, the monitor is
    not running and kabinet's view of this host's pods is going stale.
    """

    last_sweep_at: str | None = None
    containers: int = 0
    pods: int = 0
    last_error: str | None = None


@app.startup
async def on_startup(kabinet: Kabinet) -> tuple[DeployerContext, DeployerHealth]:
    """Connect to Docker, describe this host, and register backend + resource."""
    client = docker.from_env()
    qualifiers = _host_qualifiers(client)
    logger.info("host qualifiers: %s", qualifiers)

    backend = await kabinet.adeclare_backend(name=f"Docker on {DEPLOYER_ID}", kind="docker")
    resource = await kabinet.adeclare_resource(
        backend=backend,
        local_id=f"{DEPLOYER_ID}:docker",
        name=f"{DEPLOYER_ID} (docker)",
        qualifiers=[QualifierInput(key=k, value=v) for k, v in qualifiers.items()],
    )
    device_id = get_or_set_device_id() or DEPLOYER_ID
    context = DeployerContext(docker=client, backend=backend, resources=[(resource, qualifiers)], device_id=device_id)
    return context, DeployerHealth()


# --- containers and their pods ------------------------------------------------------------

_STATUS = {
    "created": PodStatus.PENDING,
    "restarting": PodStatus.PENDING,
    "running": PodStatus.RUNNING,
    "paused": PodStatus.STOPPED,
    "removing": PodStatus.STOPPING,
    "exited": PodStatus.STOPPED,
    "dead": PodStatus.FAILED,
}

#: Final states: a container that is not running does not overwrite them.
_SETTLED = (PodStatus.STOPPED, PodStatus.FAILED)


def _my_containers(ctx: DeployerContext) -> list[Container]:
    return ctx.docker.containers.list(all=True, filters={"label": f"{LABEL}={DEPLOYER_ID}"})


def _pod_key(container: Container) -> str:
    """The local id of the container's pod: the pod label it was started with.

    Containers started before pods were registered first carry no pod label; their
    pod was registered under the container id, so that is their key.
    """
    return container.labels.get(LABEL_POD) or str(container.id)


def _find_container(ctx: DeployerContext, pod_key: str) -> Container | None:
    """The container of the pod ``pod_key`` on this host, if there is one."""
    found = ctx.docker.containers.list(all=True, filters={"label": f"{LABEL_POD}={pod_key}"})
    if found:
        return found[0]
    try:  # a container from before pod labels: its id is its pod key
        container = ctx.docker.containers.get(pod_key)
    except docker.errors.NotFound:
        return None
    return container if container.labels.get(LABEL) == DEPLOYER_ID else None


def _container_of(ctx: DeployerContext, pod: Pod) -> Container:
    container = _find_container(ctx, pod.pod_id)
    if container is None:
        raise PlacementError(f"Pod {pod.id} has no container on this host (it was removed).")
    return container


def _all_my_pods(kabinet: Kabinet, ctx: DeployerContext) -> list[Pod]:
    """Every pod of this backend. Paged: the server returns at most a page at a time,
    and a pod missed here would make its container look like an orphan."""
    pods: list[Pod] = []
    while page := kabinet.my_pods(ctx.backend.id, limit=PAGE_SIZE, offset=len(pods)):
        pods.extend(page)
        if len(page) < PAGE_SIZE:
            break
    return pods


def _pod_status(container: Container) -> PodStatus:
    status = _STATUS.get(container.status, PodStatus.UNKOWN)
    if container.status == "exited" and container.attrs.get("State", {}).get("ExitCode", 0) != 0:
        status = PodStatus.FAILED
    return status


def _exit_reason(container: Container) -> str:
    state = container.attrs.get("State", {})
    if state.get("OOMKilled"):
        return "The container was killed: it ran out of memory."
    if container.status == "dead":
        return "The container is dead: docker could not stop or remove it cleanly."
    return f"The container exited with code {state.get('ExitCode')}."


def _age_seconds(container: Container) -> float:
    """How long ago docker created the container; 0 when docker does not say."""
    created = str(container.attrs.get("Created", ""))
    try:
        # Docker reports nanoseconds; datetime takes microseconds.
        when = datetime.datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", created).replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    return (datetime.datetime.now(datetime.timezone.utc) - when).total_seconds()


def _container_logs(container: Container | None, tail: int = LOG_TAIL) -> str:
    if container is None:
        return ""
    try:
        return container.logs(tail=tail).decode("utf-8", errors="replace")
    except docker.errors.APIError:
        return ""


def _push_logs(kabinet: Kabinet, pod_id: str, container: Container | None, header: str = "") -> None:
    """Write ``header`` and the container's last lines to the pod's logs (best effort)."""
    text = "\n\n".join(part for part in (header and f"[deployer] {header}", _container_logs(container)) if part)
    try:
        kabinet.dump_logs(pod_id, text)
    except ThreadCancelledError:
        raise
    except Exception as e:  # noqa: BLE001 — logs are best effort
        logger.warning("could not dump logs of pod %s: %s", pod_id, e)


def _fail(kabinet: Kabinet, pod_key: str, pod_id: str, reason: str, container: Container | None = None) -> None:
    """Mark a pod FAILED and say why: the reason first, then the container's last lines."""
    kabinet.update_pod(local_id=pod_key, status=PodStatus.FAILED)
    logger.warning("pod %s failed: %s", pod_key, reason)
    _push_logs(kabinet, pod_id, container, header=reason)


def _discard(container: Container) -> None:
    """Remove a container, without letting a failed removal hide why it was removed."""
    try:
        container.remove(force=True)
    except docker.errors.NotFound:
        pass
    except docker.errors.APIError as e:  # e.g. 409: a removal is already in progress
        logger.warning("could not remove %s: %s", container.name, e)


def _revoke_key(unlok: Unlok, mandate_id: str | None) -> None:
    """Revoke a container's key: lok deletes the client it logged in, and it can never
    log in again. Never the approval's mandate, only the child issued for the container."""
    if not mandate_id:
        return
    try:
        unlok.revoke_mandate(mandate_id)
    except ThreadCancelledError:
        raise
    except Exception as e:  # noqa: BLE001 — already gone (its parent was revoked) is fine
        logger.warning("could not revoke key mandate %s: %s", mandate_id, e)


def _verify_credentials(
    kabinet: Kabinet, unlok: Unlok, ctx: DeployerContext, container: Container, pod: Pod
) -> None:
    """Record the client the container's key logged in.

    Runs while the pod is RUNNING and has no ``client_id`` yet, so it is retried by the
    next event or sweep until the app has logged in. There is nothing to check about
    WHAT logged in: lok enforces the key's pin (the approved identifier, version, scopes,
    requirements and this container's device id) at every login. One outcome ends the
    pod, with its reason in the pod's logs: the key stopped working before the app
    logged in (revoked, or its mandate expired), so it never can; the container is stopped.
    """
    mandate_id = container.labels.get(LABEL_KEY_MANDATE)
    if not mandate_id:
        return
    try:
        mandate = unlok.get_mandate(mandate_id)
    except ThreadCancelledError:
        raise
    except Exception:  # noqa: BLE001 — retried on the next event / sweep
        logger.warning("%s: its key mandate %s cannot be read; cannot record its client", container.name, mandate_id)
        return
    if not mandate.clients:  # not logged in yet
        if not mandate.is_live:
            _fail(
                kabinet,
                pod.pod_id,
                pod.id,
                "The app's key was revoked or expired before it logged in, so it can never enroll. "
                "Reauthorize the pod to give it a fresh one.",
                container,
            )
            container.stop()
        return
    client_id = mandate.clients[0].client_id
    deployment_id = container.labels.get(LABEL_DEPLOYMENT)
    if deployment_id:
        # create_pod upserts on (backend, pod_id): this only fills in the client id.
        kabinet.create_pod(
            deployment=deployment_id,
            local_id=pod.pod_id,
            resource=ctx.resource,
            client_id=client_id,
        )
    logger.info("%s: logged in as client %s", container.name, client_id)


def _converge(kabinet: Kabinet, unlok: Unlok, ctx: DeployerContext, container: Container, pod: Pod) -> None:
    """Bring kabinet's pod in line with its container. Stateless: the pod's own
    ``status`` and ``client_id`` say what was already reported and verified."""
    status = _pod_status(container)
    if pod.status in _SETTLED and status != PodStatus.RUNNING:
        pass  # a failure or stop stays reported until the container runs again
    elif pod.status != status:
        if status == PodStatus.FAILED:
            _fail(kabinet, pod.pod_id, pod.id, _exit_reason(container), container)
        else:
            kabinet.update_pod(local_id=pod.pod_id, status=status)
            _push_logs(kabinet, pod.id, container)
        logger.info("%s: %s → %s", container.name, container.status, status.value)

    if container.status == "running" and pod.client_id is None:
        try:
            _verify_credentials(kabinet, unlok, ctx, container, pod)
        except ThreadCancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — retried on the next event / sweep
            logger.warning("credential check for %s failed: %s", container.name, e)


def _sync_container(kabinet: Kabinet, unlok: Unlok, ctx: DeployerContext, container: Container) -> None:
    """React to one docker event. Never removes anything: a container without a pod is
    for the sweep to judge, with the full picture and a grace period."""
    try:
        container.reload()
    except docker.errors.NotFound:
        return
    pod = kabinet.my_pod_at(_pod_key(container))
    if pod is not None:
        _converge(kabinet, unlok, ctx, container, pod)


def _container_gone(kabinet: Kabinet, pod_key: str) -> None:
    """A container of ours was destroyed. Its pod, if it still has one, is stopped."""
    pod = kabinet.my_pod_at(pod_key)
    if pod is not None and pod.status not in _SETTLED:
        kabinet.update_pod(local_id=pod_key, status=PodStatus.STOPPED)
        _push_logs(kabinet, pod.id, None, header="The container was removed.")


def _remove_if_orphan(kabinet: Kabinet, container: Container) -> None:
    """Remove a container of ours that no pod wants: only once it is older than the
    grace period, and only after asking for its pod directly as well as listing."""
    if _age_seconds(container) < ORPHAN_GRACE_SECONDS:
        return
    if kabinet.my_pod_at(_pod_key(container)) is not None:
        return  # it has a pod after all (registered after the listing)
    logger.warning("removing orphaned container %s: no pod wants it", container.name)
    _discard(container)


def _pod_without_container(kabinet: Kabinet, pod: Pod) -> None:
    """A pod whose container is gone: say so, once."""
    if pod.status in _SETTLED:
        return
    if pod.status == PodStatus.PENDING:
        _fail(
            kabinet,
            pod.pod_id,
            pod.id,
            "Its container never started: the deployer stopped between registering the pod "
            "and running it. Install it again.",
        )
    else:
        kabinet.update_pod(local_id=pod.pod_id, status=PodStatus.STOPPED)
        _push_logs(kabinet, pod.id, None, header="Its container is gone: it was removed outside the deployer.")


@dataclass
class _Sweep:
    """All the monitor remembers between sweeps: since when each pod has had no container."""

    missing_since: dict[str, float] = field(default_factory=dict)


def _reconcile(kabinet: Kabinet, unlok: Unlok, ctx: DeployerContext, health: DeployerHealth, sweep: _Sweep) -> None:
    """Compare every container of ours with every pod of ours, both ways.

    One item failing does not keep the others from being handled.
    """
    containers = _my_containers(ctx)
    pods = _all_my_pods(kabinet, ctx)
    by_key = {pod.pod_id: pod for pod in pods}
    errors: list[str] = []

    seen: set[str] = set()
    for container in containers:
        key = _pod_key(container)
        seen.add(key)
        try:
            pod = by_key.get(key)
            if pod is not None:
                _converge(kabinet, unlok, ctx, container, pod)
            else:
                _remove_if_orphan(kabinet, container)
        except ThreadCancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — retried on the next event / sweep
            logger.warning("could not sync %s: %s", container.name, e)
            errors.append(f"{container.name}: {e}")

    now = time.monotonic()
    sweep.missing_since = {key: since for key, since in sweep.missing_since.items() if key in by_key and key not in seen}
    for pod in pods:
        if pod.pod_id in seen:
            continue
        # A pod is registered just before its container runs: give the launch time.
        if now - sweep.missing_since.setdefault(pod.pod_id, now) < ORPHAN_GRACE_SECONDS:
            continue
        try:
            _pod_without_container(kabinet, pod)
        except ThreadCancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — retried on the next sweep
            logger.warning("could not settle pod %s: %s", pod.pod_id, e)
            errors.append(f"pod {pod.pod_id}: {e}")

    health.containers = len(containers)
    health.pods = len(pods)
    health.last_error = errors[-1] if errors else None
    health.last_sweep_at = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


class _EventPump:
    """Docker events for our containers, as ``(container id, pod key)`` on :attr:`events`.

    The pod key comes from the event's labels, so a destroyed container can still be
    matched to its pod. A daemon thread reads the stream and reopens it on error;
    :meth:`close` closes the stream, which ends the thread, so a stopped (or reloaded)
    app leaves no reader behind.
    """

    def __init__(self, client: DockerClient) -> None:
        self.client = client
        self.events: queue.Queue[tuple[str, str]] = queue.Queue()
        self._closed = threading.Event()
        self._stream: Any = None

    def start(self) -> "_EventPump":
        threading.Thread(target=self._pump, name="deployer-events", daemon=True).start()
        return self

    def _pump(self) -> None:
        while not self._closed.is_set():
            try:
                self._stream = self.client.events(
                    filters={"type": "container", "label": f"{LABEL}={DEPLOYER_ID}"}, decode=True
                )
                if self._closed.is_set():  # closed while the stream was being opened
                    self._stream.close()
                    return
                for event in self._stream:
                    actor = event.get("Actor", {})
                    container_id = event.get("id") or actor.get("ID")
                    if container_id:
                        self.events.put((container_id, actor.get("Attributes", {}).get(LABEL_POD) or container_id))
            except Exception as e:  # noqa: BLE001 — reopen the stream
                if self._closed.is_set():
                    return
                logger.warning("docker event stream error: %s; reconnecting", e)
                self._closed.wait(2)

    def close(self) -> None:
        self._closed.set()
        if self._stream is not None:
            self._stream.close()


@app.background
def watch_containers(context: DeployerContext, health: DeployerHealth, kabinet: Kabinet, unlok: Unlok) -> None:
    """Keep kabinet's view of our containers current.

    Docker events give instant status changes; a periodic two-sided sweep behind them
    catches anything the stream missed, and is the only place anything is removed or
    settled. It runs once at start, so what a previous run left behind is re-adopted
    or cleaned up.

    This runs in a worker thread, which learns that the app is stopping only where it
    asks: it waits for events at most a second at a time and checks in between. A
    cancellation is never caught as an error, so a stopping monitor makes no further
    calls (nor removes anything on a call that failed because of it).
    """
    pump = _EventPump(context.docker).start()
    sweep = _Sweep()
    last_sweep = 0.0
    try:
        while True:
            koil.check_cancelled()
            try:
                if time.monotonic() - last_sweep >= RECONCILE_SECONDS:
                    # Set first: a sweep that fails waits for the next interval, not 2 s.
                    last_sweep = time.monotonic()
                    _reconcile(kabinet, unlok, context, health, sweep)
                try:
                    container_id, pod_key = pump.events.get(timeout=1.0)
                except queue.Empty:
                    continue
                try:
                    container = context.docker.containers.get(container_id)
                except docker.errors.NotFound:
                    _container_gone(kabinet, pod_key)
                    continue
                _sync_container(kabinet, unlok, context, container)
            except ThreadCancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — the monitor must never die
                logger.warning("monitor error: %s", e)
                health.last_error = f"{type(e).__name__}: {e}"
                koil.sleep(2)
    finally:
        pump.close()


# --- deploying ---------------------------------------------------------------------------


def _instance_device_id(ctx: DeployerContext) -> str:
    """A device id for one container. A lok client is unique per (release, user, device,
    hub), so two containers of one release on this host must not share a device id."""
    return f"{ctx.device_id}:{uuid.uuid4().hex[:12]}"


def _issue_key(unlok: Unlok, approval: ReleaseApproval, device_id: str) -> IssuedMandateKey:
    """A mandate key for one container: a child of the approval's mandate, pinned to the
    container's device id, acting as the approver. lok shows the key only here."""
    return unlok.issue_child_mandate_key(
        mandate=approval.mandate_id, device_id=device_id, label=f"{AGENT_IDENTIFIER} {device_id}"
    )


def _pull_image(task: Task, ctx: DeployerContext, image: str, start: int = 10, end: int = 60) -> None:
    """Pull with real progress: completed layers over layers seen so far."""
    repository, tag = parse_repository_tag(image)
    layers: dict[str, bool] = {}
    for event in ctx.docker.api.pull(repository, tag=tag or "latest", stream=True, decode=True):
        if "error" in event:
            raise RuntimeError(f"pull failed: {event['error']}")
        layer, status = event.get("id"), event.get("status", "")
        if not layer:
            continue
        layers.setdefault(layer, False)
        if status in ("Pull complete", "Already exists"):
            layers[layer] = True
        done = sum(layers.values())
        task.progress(start + int((end - start) * done / max(len(layers), 1)), f"Pulling {image}: {done}/{len(layers)} layers")


def _slug(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", text).strip("-").lower()[:60]


def _start_container(
    ctx: DeployerContext,
    name: str,
    pod_key: str,
    flavour: Flavour,
    deployment_id: str,
    approval: ReleaseApproval,
    device_id: str,
    key: IssuedMandateKey,
) -> Container:
    release = flavour.release
    return ctx.docker.containers.run(
        flavour.image.image_string,
        # The CLI reads FAKTS_MANDATE_KEY / FAKTS_URL itself; nothing secret on argv.
        command="arkitekt run prod",
        name=name,
        detach=True,
        restart_policy=RESTART_POLICY,
        # A network mode (another container's namespace) replaces the network.
        **({"network_mode": ctx.network_mode} if ctx.network_mode else {"network": ctx.network}),
        labels={
            LABEL: DEPLOYER_ID,
            LABEL_POD: pod_key,
            LABEL_DEPLOYMENT: str(deployment_id),
            LABEL_FLAVOUR: str(flavour.id),
            LABEL_APP: release.app.identifier,
            LABEL_VERSION: release.version,
            LABEL_APPROVAL: str(approval.id),
            LABEL_INSTANCE: device_id,
            LABEL_KEY_MANDATE: str(key.mandate.id),
        },
        environment={
            "FAKTS_MANDATE_KEY": key.key,
            "FAKTS_URL": ctx.gateway,
            # The key is pinned to this device id; the app puts it in its manifest.
            "ARKITEKT_DEVICE_ID": device_id,
            "ARKITEKT_HEADLESS": "1",
            # The app states it on Register: pickers of this engine's agent actions narrow on it.
            "ARKITEKT_SUPERVISOR": AGENT_IDENTIFIER,
            **({"FAKTS_ALLOW_INSECURE_TRANSPORT": INSECURE_TRANSPORT} if INSECURE_TRANSPORT else {}),
        },
        device_requests=_device_requests(flavour),
    )


def _launch(
    kabinet: Kabinet,
    ctx: DeployerContext,
    flavour: Flavour,
    deployment_id: str,
    approval: ReleaseApproval,
    device_id: str,
    key: IssuedMandateKey,
    resource: Resource,
) -> tuple[Container, Pod]:
    """Register the pod, then start its container: pod first, container second.

    The pod's local id is chosen here and carried by the container as a label, so a
    container of ours has a pod from its very first docker event; the monitor never
    meets one it could mistake for an orphan. If the container cannot be started, the
    pod (and anything docker created for it) goes again, and the docker error is what
    is raised. A deployer that dies in between leaves a PENDING pod with no container,
    which the sweep marks FAILED.
    """
    release = flavour.release
    pod_key = uuid.uuid4().hex
    name = f"kabinet-{_slug(release.app.identifier)}-{_slug(release.version)}-{pod_key[:8]}"
    pod = kabinet.create_pod(deployment=deployment_id, local_id=pod_key, resource=resource)
    try:
        container = _start_container(ctx, name, pod_key, flavour, deployment_id, approval, device_id, key)
    except Exception:
        for leftover in ctx.docker.containers.list(all=True, filters={"label": f"{LABEL_POD}={pod_key}"}):
            _discard(leftover)
        try:
            kabinet.delete_pod(pod.id)
        except ThreadCancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — the sweep marks it FAILED instead
            logger.warning("could not delete pod %s after a failed start: %s", pod_key, e)
        raise
    return container, pod


def _launch_keyed(
    kabinet: Kabinet,
    unlok: Unlok,
    ctx: DeployerContext,
    flavour: Flavour,
    deployment_id: str,
    approval: ReleaseApproval,
    device_id: str,
    key: IssuedMandateKey,
    resource: Resource,
) -> tuple[Container, Pod]:
    """``_launch``, revoking the key again if no container came of it: a key nobody runs
    must not stay live."""
    try:
        return _launch(kabinet, ctx, flavour, deployment_id, approval, device_id, key, resource)
    except Exception:
        _revoke_key(unlok, str(key.mandate.id))
        raise


def _deploy_flavour(
    kabinet: Kabinet, unlok: Unlok, task: Task, ctx: DeployerContext, flavour: Flavour, approval: ReleaseApproval
) -> Pod:
    """The deploy path. Nothing is created before the policy checks pass."""
    image = flavour.image.image_string
    task.progress(0, f"Checking {flavour.release.app.identifier}:{flavour.release.version} ({flavour.name})")
    _check_approval(ctx, approval)
    _check_mandate(unlok, approval)
    if flavour.release.id != approval.release.id:
        raise PlacementError(f"Flavour '{flavour.name}' does not belong to the approved release.")
    _check_image(flavour)
    _check_scopes(flavour)
    resource = _select_resource(ctx, flavour)

    # Pull first: no key is issued for an image this host cannot fetch.
    task.progress(5, f"Pulling {image}")
    _pull_image(task, ctx, image, start=5, end=55)

    task.progress(60, "Registering deployment")
    deployment = kabinet.create_deployment(
        flavour=flavour, local_id=image, approval=approval.id, last_pulled=datetime.datetime.now()
    )

    task.progress(65, "Issuing a key under the approval's mandate")
    device_id = _instance_device_id(ctx)
    key = _issue_key(unlok, approval, device_id)

    task.progress(70, "Starting container")
    container, pod = _launch_keyed(kabinet, unlok, ctx, flavour, deployment.id, approval, device_id, key, resource)

    task.progress(100, f"Started {container.name}")
    logger.info("deployed %s as %s (pod %s, approval %s)", image, container.name, pod.id, approval.id)
    return pod


def _best_flavour(ctx: DeployerContext, flavours: Iterable[ListFlavour]) -> ListFlavour:
    """The placeable flavour with the highest preference score, or a PlacementError
    naming why each one was rejected."""
    reasons: list[str] = []
    ranked: list[tuple[int, ListFlavour]] = []
    for flavour in flavours:
        try:
            _check_image(flavour)
            _check_scopes(flavour)
            _select_resource(ctx, flavour)
        except PlacementError as e:
            reasons.append(str(e))
            continue
        score = max(_placement(flavour, q).score for _, q in ctx.resources)
        ranked.append((score, flavour))
    if not ranked:
        raise PlacementError("No flavour can be placed on this host:\n" + "\n".join(reasons))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[0][1]


def _install(kabinet: Kabinet, unlok: Unlok, task: Task, ctx: DeployerContext, approval: ReleaseApproval) -> Pod:
    release = approval.release
    if not release.flavours:
        raise PlacementError(f"Release {release.app.identifier}:{release.version} has no flavours.")
    chosen = _best_flavour(ctx, release.flavours)
    return _deploy_flavour(kabinet, unlok, task, ctx, kabinet.get_flavour(chosen.id), approval)


@app.action(effects=Effects.IRREVERSIBLE, logo=INSTALL_LOGO)
def install(
    approval: ReleaseApproval, context: DeployerContext, kabinet: Kabinet, unlok: Unlok, task: Task
) -> Pod:
    """Install

    Runs an approved release on this host, picking the flavour whose selectors this
    host satisfies best. The app acts as the user who approved it.
    """
    return _install(kabinet, unlok, task, context, approval)


@app.action(effects=Effects.IRREVERSIBLE, logo=DEPLOY_FLAVOUR_LOGO)
def deploy_flavour(
    flavour: Flavour, approval: ReleaseApproval, context: DeployerContext, kabinet: Kabinet, unlok: Unlok, task: Task
) -> Pod:
    """Deploy Flavour

    Runs one specific flavour of an approved release as a container on this host.
    """
    return _deploy_flavour(kabinet, unlok, task, context, flavour, approval)


@app.action(effects=Effects.IRREVERSIBLE, logo=AUTO_INSTALL_LOGO)
def auto_install(
    definition: Definition, context: DeployerContext, kabinet: Kabinet, unlok: Unlok, rekuest: Rekuest, task: Task
) -> Pod:
    """Auto Install

    Finds the newest approved flavour that provides the definition and can be placed
    on this host, and deploys it. When none is approved, asks the app it was called
    from to have the newest placeable one approved.
    """
    flavours = kabinet.list_flavours(
        filters=FlavourFilter(has_definitions=[definition.id]),
        ordering=[FlavourOrder(released_at=Ordering.DESC)],
    )
    if not flavours:
        raise PlacementError(f"No flavour provides definition '{definition.name}'.")
    approvals = {a.release.id: a for a in _usable_approvals(kabinet, context)}
    unapproved: Flavour | None = None
    for flavour in flavours:
        try:
            _check_image(flavour)
            _check_scopes(flavour)
            _select_resource(context, flavour)
        except PlacementError as e:
            logger.info("skipping flavour %s: %s", flavour.name, e)
            continue
        full = kabinet.get_flavour(flavour.id)
        approval = approvals.get(full.release.id)
        if approval is None:
            logger.info("skipping flavour %s for now: its release has no usable approval", flavour.name)
            unapproved = unapproved or full
            continue
        return _deploy_flavour(kabinet, unlok, task, context, full, approval)
    if unapproved is not None:
        approval = _approval_from_callers(rekuest, task, unapproved.release)
        return _deploy_flavour(kabinet, unlok, task, context, unapproved, approval)
    raise PlacementError(f"No flavour providing '{definition.name}' can be placed on this host.")


# --- lifecycle ------------------------------------------------------------------------------


def _recreate(kabinet: Kabinet, unlok: Unlok, task: Task, ctx: DeployerContext, pod: Pod, old: Container) -> Pod:
    """Replace a container with one holding a fresh key.

    Environment cannot be changed on an existing container, so this is a new container
    (and therefore a new pod) for the same deployment, under the same approval — which
    must still be active: a revoked or stale approval is not silently renewed.
    """
    flavour = pod.deployment.flavour
    deployment_id = old.labels.get(LABEL_DEPLOYMENT)
    approval_id = old.labels.get(LABEL_APPROVAL) or (pod.deployment.approval.id if pod.deployment.approval else None)
    if not deployment_id or not approval_id:
        raise PlacementError(f"Container {old.name} carries no deployment or approval; install it again instead.")
    approval = kabinet.get_release_approval(approval_id)
    _check_approval(ctx, approval)
    _check_mandate(unlok, approval)
    resource = _select_resource(ctx, flavour)
    task.progress(10, "Issuing a fresh key")
    device_id = _instance_device_id(ctx)
    key = _issue_key(unlok, approval, device_id)
    task.progress(40, f"Removing {old.name}")
    old_key = old.labels.get(LABEL_KEY_MANDATE)
    old.remove(force=True)
    _revoke_key(unlok, old_key)
    kabinet.delete_pod(pod.id)
    task.progress(60, "Starting replacement container")
    container, new_pod = _launch_keyed(kabinet, unlok, ctx, flavour, deployment_id, approval, device_id, key, resource)
    task.progress(100, f"Replaced {old.name} with {container.name}")
    return new_pod


@app.action(effects=Effects.REPEATABLE, logo=REFRESH_LOGS_LOGO)
def refresh_logs(pod: Pod, context: DeployerContext, kabinet: Kabinet) -> Pod:
    """Refresh Logs

    Pushes the container's most recent log lines to kabinet.
    """
    _push_logs(kabinet, pod.id, _container_of(context, pod))
    return pod


def _stop_pod(context: DeployerContext, kabinet: Kabinet, pod: Pod) -> Pod:
    _container_of(context, pod).stop()
    kabinet.update_pod(local_id=pod.pod_id, status=PodStatus.STOPPED)
    return pod


def _start_pod(context: DeployerContext, kabinet: Kabinet, pod: Pod) -> Pod:
    _container_of(context, pod).start()
    kabinet.update_pod(local_id=pod.pod_id, status=PodStatus.PENDING)
    return pod


def _restart_pod(context: DeployerContext, kabinet: Kabinet, pod: Pod) -> Pod:
    _container_of(context, pod).restart()
    kabinet.update_pod(local_id=pod.pod_id, status=PodStatus.PENDING)
    return pod


@app.action(effects=Effects.REPEATABLE, logo=STOP_LOGO)
def stop(pod: Pod, context: DeployerContext, kabinet: Kabinet) -> Pod:
    """Stop

    Stops the container. The on-failure restart policy does not restart a clean stop.
    """
    return _stop_pod(context, kabinet, pod)


@app.action(effects=Effects.REPEATABLE, logo=RESTART_LOGO)
def restart(pod: Pod, context: DeployerContext, kabinet: Kabinet) -> Pod:
    """Restart

    Restarts the container. It keeps its identity: it logs in again with the same key.
    """
    return _restart_pod(context, kabinet, pod)


# --- the apps, as agents ------------------------------------------------------------
#
# The same lifecycle, on the agent an app became: what a user sees of it in rekuest. An
# app this engine starts states ARKITEKT_SUPERVISOR=plugin_engine on Register, which is
# what the picker below narrows on. That is the app's own claim, so it decides nothing:
# the agent is acted on only when its lok client is one this engine recorded on one of
# its own pods, and only for a caller of that agent's organization.

OwnAgent = Annotated[Agent, Requires(key=SUPERVISOR, operator=DescriptorOperator.EQUALS, value=AGENT_IDENTIFIER)]


class NotMine(PlacementError):
    """An agent that is not an app this engine runs, or a caller who may not act on it."""


def _pod_of(kabinet: Kabinet, context: DeployerContext, agent: Agent) -> Pod:
    """The pod of this engine whose verified lok client is the agent's."""
    client_id = agent.client.client_id
    for pod in _all_my_pods(kabinet, context):
        if pod.client_id is not None and pod.client_id == client_id:
            return pod
    raise NotMine(f"{agent.name} is not an app this engine runs")


def _check_caller(task: Task, agent: Agent) -> None:
    """Only a caller of the agent's own organization may act on it."""
    if task.org != agent.organization.slug:
        raise NotMine(f"{agent.name} belongs to another organization than the caller's")


@app.action(effects=Effects.REPEATABLE, logo=STOP_LOGO)
def stop_app(agent: OwnAgent, context: DeployerContext, kabinet: Kabinet, task: Task) -> Pod:
    """Stop App

    Stops the container of an app this engine runs.
    """
    _check_caller(task, agent)
    return _stop_pod(context, kabinet, _pod_of(kabinet, context, agent))


@app.action(effects=Effects.REPEATABLE, logo=START_LOGO)
def start_app(agent: OwnAgent, context: DeployerContext, kabinet: Kabinet, task: Task) -> Pod:
    """Start App

    Starts the stopped container of an app this engine runs. It comes back as the same
    agent: it logs in again with the key it was started with.
    """
    _check_caller(task, agent)
    return _start_pod(context, kabinet, _pod_of(kabinet, context, agent))


@app.action(effects=Effects.REPEATABLE, logo=RESTART_LOGO)
def restart_app(agent: OwnAgent, context: DeployerContext, kabinet: Kabinet, task: Task) -> Pod:
    """Restart App

    Restarts the container of an app this engine runs, keeping its identity.
    """
    _check_caller(task, agent)
    return _restart_pod(context, kabinet, _pod_of(kabinet, context, agent))


@app.action(effects=Effects.IRREVERSIBLE, logo=REAUTHORIZE_LOGO)
def reauthorize(
    pod: Pod, context: DeployerContext, kabinet: Kabinet, unlok: Unlok, task: Task
) -> Pod:
    """Reauthorize

    Re-creates the container with a fresh key, for a pod whose key was revoked or
    expired. Yields a new pod.
    """
    return _recreate(kabinet, unlok, task, context, pod, _container_of(context, pod))


@app.action(effects=Effects.REPEATABLE, logo=REMOVE_LOGO)
def remove(pod: Pod, context: DeployerContext, kabinet: Kabinet, unlok: Unlok) -> Pod:
    """Remove

    Removes the container, revokes its key (lok deletes its client) and deletes the pod.
    """
    container = _find_container(context, pod.pod_id)
    if container is None:
        # Without the container its key is unknown here; it dies with the approval's mandate.
        logger.warning("container of pod %s already gone; its key stays until the approval is revoked", pod.pod_id)
    else:
        _discard(container)
        _revoke_key(unlok, container.labels.get(LABEL_KEY_MANDATE))
    kabinet.delete_pod(pod.id)
    return pod


def _await_enrolled(
    kabinet: Kabinet, unlok: Unlok, task: Task, ctx: DeployerContext, pod: Pod, timeout: float
) -> None:
    """Wait until ``pod``'s app has enrolled as what was approved (its ``client_id`` is
    set), or raise. A running container is not enough: the app may still fail to log in."""
    deadline = time.monotonic() + timeout
    while True:
        # A cancelled wait must stop here, before anything acts on its outcome.
        task.check_cancelled()
        current = kabinet.my_pod_at(pod.pod_id)
        if current is None:
            raise PlacementError(f"Pod {pod.id} disappeared before its app enrolled.")
        if current.client_id:
            return
        if current.status == PodStatus.FAILED:
            raise PlacementError(f"Pod {pod.id} failed before its app enrolled; its logs say why.")
        container = _find_container(ctx, pod.pod_id)
        if container is not None and container.status == "running":
            _verify_credentials(kabinet, unlok, ctx, container, current)
        if time.monotonic() > deadline:
            raise PlacementError(f"Pod {pod.id}'s app did not enroll within {timeout:.0f} s.")
        koil.sleep(2)


@app.action(effects=Effects.IRREVERSIBLE, logo=UPGRADE_LOGO)
def upgrade(
    pod: Pod, context: DeployerContext, kabinet: Kabinet, unlok: Unlok, task: Task
) -> Pod:
    """Upgrade

    Deploys the newest release of the pod's app that the *same user* approved, and
    removes the old pod once the new app has enrolled. Returns the pod unchanged if no
    newer approved release exists: a newer release nobody approved is not installed.
    If the new pod fails, the old one is kept and the new one stays for its logs.
    """
    current = pod.deployment.flavour.release
    # The container's own label is authoritative: it names the approval it was started under.
    approval_id = _container_of(context, pod).labels.get(LABEL_APPROVAL) or (
        pod.deployment.approval.id if pod.deployment.approval else None
    )
    if approval_id is None:
        raise PlacementError("This pod was deployed without an approval; install an approved release instead.")
    approver = kabinet.get_release_approval(approval_id).approver.sub
    candidates = [
        a
        for a in _usable_approvals(kabinet, context)
        if a.release.app.identifier == current.app.identifier and a.approver.sub == approver
    ]
    newest = max(candidates, key=lambda a: _version_tuple(a.release.version), default=None)
    if newest is None or _version_tuple(newest.release.version) <= _version_tuple(current.version):
        task.log(
            f"No newer approved release of {current.app.identifier} than {current.version}; approve one first.",
            level=LogLevel.INFO,
        )
        return pod

    new_pod = _install(kabinet, unlok, task, context, newest)
    task.log(f"Waiting for {newest.release.version} to enroll before removing {current.version}.", level=LogLevel.INFO)
    _await_enrolled(kabinet, unlok, task, context, new_pod, UPGRADE_WAIT_SECONDS)

    old = _find_container(context, pod.pod_id)
    if old is not None:
        _discard(old)
        _revoke_key(unlok, old.labels.get(LABEL_KEY_MANDATE))
    kabinet.delete_pod(pod.id)
    return new_pod


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(app)
