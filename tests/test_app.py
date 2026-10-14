"""The plugin engine against fake docker, kabinet and unlok clients.

What is held here is what broke in the lab, or would have: a monitor that removes
running apps, a launch the monitor mistakes for an orphan, a failure that says
nothing, a stop that hangs on a blocking monitor. The monitor tests run
``watch_containers`` the way the runtime does, in a koil worker thread, and cancel it.
"""

import asyncio
import datetime
import threading
import time
from types import SimpleNamespace

import docker.errors
import pytest
from koil.bridge import run_threaded
from koil.errors import ThreadCancelledError
from rath.operation import GraphQLException

import app as D
from kabinet.api.schema import PodStatus

OLD = "2020-01-01T00:00:00.123456789Z"


def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# --- fakes -------------------------------------------------------------------------------


class FakeStream:
    def __init__(self) -> None:
        self.closed = threading.Event()

    def __iter__(self):
        self.closed.wait()
        return iter(())

    def close(self) -> None:
        self.closed.set()


class FakeContainer:
    def __init__(self, cid, pod_key=None, status="running", exit_code=0, created=OLD, **labels):
        self.id = cid
        self.name = f"c-{cid}"
        self.status = status
        self.attrs = {"State": {"ExitCode": exit_code}, "Created": created}
        self.labels = {D.LABEL: D.DEPLOYER_ID, **labels}
        if pod_key:
            self.labels[D.LABEL_POD] = pod_key
        self.removed = False
        self.stopped = False

    def reload(self):
        pass

    def remove(self, force=False):
        self.removed = True

    def stop(self):
        self.stopped = True
        self.status = "exited"

    def logs(self, tail=200):
        return b"last lines of the app"


class FakeDocker:
    def __init__(self, containers=()):
        self.all = list(containers)
        self.streams: list[FakeStream] = []
        self.calls: list[str] = []
        self.run_error: Exception | None = None
        self.run_kwargs: dict = {}
        self.containers = SimpleNamespace(list=self._list, get=self._get, run=self._run)

    def _live(self):
        return [c for c in self.all if not c.removed]

    def _list(self, all=False, filters=None):
        key, _, value = (filters or {}).get("label", "").partition("=")
        return [c for c in self._live() if c.labels.get(key) == value]

    def _get(self, cid):
        for c in self._live():
            if c.id == cid:
                return c
        raise docker.errors.NotFound(cid)

    def _run(self, image, name, labels, **kwargs):
        self.calls.append("docker run")
        self.run_kwargs = kwargs
        container = FakeContainer(f"new-{len(self.all)}", status="created", created=now_iso())
        container.labels.update(labels)
        container.name = name
        self.all.append(container)
        if self.run_error:
            raise self.run_error
        return container

    def events(self, **_):
        stream = FakeStream()
        self.streams.append(stream)
        return stream


def make_pod(key, status=PodStatus.RUNNING, client_id="client", app=("app", "1.0")):
    release = SimpleNamespace(app=SimpleNamespace(identifier=app[0]), version=app[1])
    return SimpleNamespace(
        id=f"pod-id-{key}",
        pod_id=key,
        status=status,
        client_id=client_id,
        deployment=SimpleNamespace(flavour=SimpleNamespace(release=release)),
    )


class FakeKabinet:
    def __init__(self, pods=(), calls=None):
        self.pods = {p.pod_id: p for p in pods}
        self.logs: dict[str, list[str]] = {}
        self.calls = calls if calls is not None else []
        self.fail_listing: Exception | None = None

    def my_pods(self, backend, limit=None, offset=0):
        if self.fail_listing:
            raise self.fail_listing
        pods = list(self.pods.values())
        return tuple(pods[offset : offset + (limit or len(pods))])

    def my_pod_at(self, local_id):
        return self.pods.get(local_id)

    def update_pod(self, local_id, status):
        self.pods[local_id].status = status
        return self.pods[local_id]

    def create_pod(self, deployment, local_id, resource=None, client_id=None):
        self.calls.append("create_pod")
        pod = self.pods.setdefault(local_id, make_pod(local_id, status=PodStatus.PENDING, client_id=None))
        if client_id:
            pod.client_id = client_id
        return pod

    def delete_pod(self, pod_id):
        self.calls.append("delete_pod")
        self.pods = {k: p for k, p in self.pods.items() if p.id != pod_id}
        return pod_id

    def dump_logs(self, pod_id, logs):
        self.logs.setdefault(pod_id, []).append(logs)


def make_ctx(docker_client):
    resource = SimpleNamespace(id="resource")
    return D.DeployerContext(
        docker=docker_client, backend=SimpleNamespace(id="backend"), resources=[(resource, {})], device_id="dev"
    )


def reconcile(kabinet, ctx, unlok=None, sweep=None):
    health = D.DeployerHealth()
    sweep = sweep or D._Sweep()
    D._reconcile(kabinet, unlok or SimpleNamespace(), ctx, health, sweep)
    return health, sweep


# --- the monitor stops when the app stops ---------------------------------------------------


def run_monitor(ctx, kabinet, seconds):
    """Run watch_containers as the runtime does, cancel it, and say how it ended."""
    health = D.DeployerHealth()

    async def main():
        task = asyncio.ensure_future(
            run_threaded(D.watch_containers, context=ctx, health=health, kabinet=kabinet, unlok=SimpleNamespace())
        )
        await asyncio.sleep(seconds)
        started = time.monotonic()
        task.cancel()
        try:
            await task
            outcome = "returned"
        except asyncio.CancelledError:
            outcome = "cancelled"
        except ThreadCancelledError:
            outcome = "thread-cancelled"
        return outcome, time.monotonic() - started

    outcome, took = asyncio.run(main())
    return outcome, took, health


def test_a_cancelled_monitor_stops_promptly_and_leaves_no_reader(monkeypatch):
    monkeypatch.setattr(D, "RECONCILE_SECONDS", 0.2)
    docker_client = FakeDocker([FakeContainer("a", pod_key="a")])
    kabinet = FakeKabinet([make_pod("a")])

    outcome, took, health = run_monitor(make_ctx(docker_client), kabinet, 0.5)

    assert outcome in ("cancelled", "thread-cancelled")
    assert took < 2.5
    assert docker_client.streams and all(s.closed.is_set() for s in docker_client.streams)
    time.sleep(0.2)
    assert not [t for t in threading.enumerate() if t.name == "deployer-events"]
    assert health.last_sweep_at is not None and health.containers == 1 and health.pods == 1


def test_a_failed_listing_removes_nothing(monkeypatch):
    monkeypatch.setattr(D, "RECONCILE_SECONDS", 0.2)
    container = FakeContainer("a", pod_key="a")
    kabinet = FakeKabinet([make_pod("a")])
    kabinet.fail_listing = GraphQLException("connection reset")

    _, _, health = run_monitor(make_ctx(FakeDocker([container])), kabinet, 0.5)

    assert not container.removed
    assert "connection reset" in (health.last_error or "")


def test_a_cancelled_kabinet_call_removes_nothing():
    container = FakeContainer("a", pod_key="a")
    kabinet = FakeKabinet()
    kabinet.fail_listing = ThreadCancelledError("Task was cancelled")

    with pytest.raises(ThreadCancelledError):
        reconcile(kabinet, make_ctx(FakeDocker([container])))
    assert not container.removed


# --- orphans: only the sweep removes, after a grace period, after asking twice -------------


def test_an_old_container_no_pod_wants_is_removed():
    container = FakeContainer("a", pod_key="gone")
    reconcile(FakeKabinet(), make_ctx(FakeDocker([container])))
    assert container.removed


def test_a_young_container_without_a_pod_is_left_alone():
    container = FakeContainer("a", pod_key="launching", created=now_iso())
    reconcile(FakeKabinet(), make_ctx(FakeDocker([container])))
    assert not container.removed


def test_a_container_whose_pod_was_registered_after_the_listing_is_kept():
    container = FakeContainer("a", pod_key="late")
    kabinet = FakeKabinet()
    kabinet.my_pods = lambda backend, limit=None, offset=0: ()  # listed before the pod existed
    kabinet.pods["late"] = make_pod("late")

    reconcile(kabinet, make_ctx(FakeDocker([container])))

    assert not container.removed


def test_docker_events_never_remove():
    container = FakeContainer("a", pod_key="gone")
    D._sync_container(FakeKabinet(), SimpleNamespace(), make_ctx(FakeDocker([container])), container)
    assert not container.removed


def test_a_container_from_before_pod_labels_is_adopted_by_its_id():
    container = FakeContainer("legacy-id", status="exited", exit_code=0)  # no pod label
    kabinet = FakeKabinet([make_pod("legacy-id", status=PodStatus.RUNNING)])
    ctx = make_ctx(FakeDocker([container]))

    reconcile(kabinet, ctx)

    assert not container.removed
    assert kabinet.pods["legacy-id"].status == PodStatus.STOPPED
    assert D._find_container(ctx, "legacy-id") is container


def test_one_failing_container_does_not_stop_the_sweep():
    bad, good = FakeContainer("bad", pod_key="bad", status="exited"), FakeContainer("good", pod_key="good", status="exited")
    kabinet = FakeKabinet([make_pod("bad"), make_pod("good")])
    original = kabinet.update_pod

    def update_pod(local_id, status):
        if local_id == "bad":
            raise GraphQLException("boom")
        return original(local_id, status)

    kabinet.update_pod = update_pod
    health, _ = reconcile(kabinet, make_ctx(FakeDocker([bad, good])))

    assert kabinet.pods["good"].status == PodStatus.STOPPED
    assert "boom" in (health.last_error or "")


def test_the_sweep_reads_every_page(monkeypatch):
    monkeypatch.setattr(D, "PAGE_SIZE", 2)
    containers = [FakeContainer(f"c{i}", pod_key=f"p{i}") for i in range(5)]
    kabinet = FakeKabinet([make_pod(f"p{i}") for i in range(5)])

    health, _ = reconcile(kabinet, make_ctx(FakeDocker(containers)))

    assert health.pods == 5
    assert not any(c.removed for c in containers)


# --- pods without containers -------------------------------------------------------------


def test_a_pending_pod_whose_container_never_started_fails_after_the_grace(monkeypatch):
    kabinet = FakeKabinet([make_pod("p", status=PodStatus.PENDING, client_id=None)])
    ctx = make_ctx(FakeDocker())

    _, sweep = reconcile(kabinet, ctx)
    assert kabinet.pods["p"].status == PodStatus.PENDING  # within the grace: a launch in flight

    monkeypatch.setattr(D, "ORPHAN_GRACE_SECONDS", 0)
    reconcile(kabinet, ctx, sweep=sweep)

    assert kabinet.pods["p"].status == PodStatus.FAILED
    assert "never started" in kabinet.logs["pod-id-p"][-1]


def test_a_running_pod_whose_container_vanished_is_stopped(monkeypatch):
    monkeypatch.setattr(D, "ORPHAN_GRACE_SECONDS", 0)
    kabinet = FakeKabinet([make_pod("p", status=PodStatus.RUNNING)])

    reconcile(kabinet, make_ctx(FakeDocker()))

    assert kabinet.pods["p"].status == PodStatus.STOPPED
    assert "removed outside the deployer" in kabinet.logs["pod-id-p"][-1]


# --- failures say why, and stay reported ---------------------------------------------------


def test_a_crashed_container_fails_its_pod_with_the_exit_code_and_logs():
    container = FakeContainer("a", pod_key="a", status="exited", exit_code=3)
    kabinet = FakeKabinet([make_pod("a")])

    reconcile(kabinet, make_ctx(FakeDocker([container])))

    assert kabinet.pods["a"].status == PodStatus.FAILED
    dump = kabinet.logs["pod-id-a"][-1]
    assert "exited with code 3" in dump and "last lines of the app" in dump


def test_a_failure_is_not_overwritten_by_a_stop():
    container = FakeContainer("a", pod_key="a", status="exited", exit_code=0)
    kabinet = FakeKabinet([make_pod("a", status=PodStatus.FAILED)])

    reconcile(kabinet, make_ctx(FakeDocker([container])))

    assert kabinet.pods["a"].status == PodStatus.FAILED


def token(client=None, expires_at=None):
    return SimpleNamespace(client=client, expires_at=expires_at)


def test_a_credential_that_expired_unredeemed_fails_the_pod_and_stops_the_container():
    container = FakeContainer("a", pod_key="a", **{D.LABEL_TOKEN: "t1"})
    kabinet = FakeKabinet([make_pod("a", client_id=None)])
    past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)
    unlok = SimpleNamespace(mandate_token=lambda _id: token(expires_at=past))

    reconcile(kabinet, make_ctx(FakeDocker([container])), unlok=unlok)

    assert kabinet.pods["a"].status == PodStatus.FAILED
    assert "did not redeem its credential" in kabinet.logs["pod-id-a"][-1]
    assert container.stopped


def test_an_unredeemed_credential_within_its_lifetime_is_waited_for():
    container = FakeContainer("a", pod_key="a", **{D.LABEL_TOKEN: "t1"})
    kabinet = FakeKabinet([make_pod("a", client_id=None)])
    future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
    unlok = SimpleNamespace(mandate_token=lambda _id: token(expires_at=future))

    reconcile(kabinet, make_ctx(FakeDocker([container])), unlok=unlok)

    assert kabinet.pods["a"].status == PodStatus.RUNNING and not container.stopped


def test_a_redeemed_credential_that_cannot_be_verified_never_stops_the_app():
    """A client whose release does not come back has redeemed: past the token's expiry
    that is "cannot verify", never "never enrolled"."""
    container = FakeContainer("a", pod_key="a", **{D.LABEL_TOKEN: "t1"})
    kabinet = FakeKabinet([make_pod("a", client_id=None)])
    past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)
    redeemed = SimpleNamespace(client_id="c1", release=None)
    unlok = SimpleNamespace(mandate_token=lambda _id: token(client=redeemed, expires_at=past))

    reconcile(kabinet, make_ctx(FakeDocker([container])), unlok=unlok)

    assert not container.stopped
    assert kabinet.pods["a"].status == PodStatus.RUNNING


def test_an_app_that_enrolled_as_something_else_is_released_and_removed():
    container = FakeContainer("a", pod_key="a", **{D.LABEL_TOKEN: "t1", D.LABEL_DEPLOYMENT: "d"})
    kabinet = FakeKabinet([make_pod("a", client_id=None, app=("approved", "1.0"))])
    released = []
    impostor = SimpleNamespace(client_id="c9", release=SimpleNamespace(app=SimpleNamespace(identifier="other"), version="6.6"))
    unlok = SimpleNamespace(mandate_token=lambda _id: token(client=impostor), release_mandate_client=released.append)

    reconcile(kabinet, make_ctx(FakeDocker([container])), unlok=unlok)

    assert released == ["c9"]
    assert container.removed
    assert kabinet.pods["a"].status == PodStatus.FAILED
    assert "enrolled as other:6.6" in kabinet.logs["pod-id-a"][-1]


# --- launching: pod first, container second ------------------------------------------------


def launch(docker_client, kabinet, ctx=None):
    release = SimpleNamespace(app=SimpleNamespace(identifier="app"), version="1.0")
    flavour = SimpleNamespace(id="f", release=release, image=SimpleNamespace(image_string="app@sha256:abc"), selectors=[])
    return D._launch(
        kabinet,
        ctx or make_ctx(docker_client),
        flavour,
        "deployment",
        SimpleNamespace(id="approval"),
        "dev",
        SimpleNamespace(id="t", token="secret"),
        SimpleNamespace(id="resource"),
    )


def test_the_pod_exists_before_its_container_runs():
    calls: list[str] = []
    docker_client, kabinet = FakeDocker(), FakeKabinet(calls=calls)
    docker_client.calls = calls

    container, pod = launch(docker_client, kabinet)

    assert calls == ["create_pod", "docker run"]
    assert container.labels[D.LABEL_POD] == pod.pod_id
    # The monitor's first look at the new container finds its pod.
    reconcile(kabinet, make_ctx(docker_client))
    assert not container.removed


def test_a_container_that_cannot_start_takes_its_pod_with_it():
    docker_client, kabinet = FakeDocker(), FakeKabinet()
    docker_client.run_error = docker.errors.APIError("could not select device driver")

    with pytest.raises(docker.errors.APIError, match="device driver"):
        launch(docker_client, kabinet)

    assert kabinet.pods == {}
    assert all(c.removed for c in docker_client.all)


def test_a_plugin_joins_the_engines_network():
    docker_client = FakeDocker()

    launch(docker_client, FakeKabinet())

    assert docker_client.run_kwargs["network"] == D.ARKITEKT_NETWORK
    assert "network_mode" not in docker_client.run_kwargs


def test_a_plugin_logs_in_over_plain_http_only_where_the_engine_may(monkeypatch):
    docker_client = FakeDocker()
    launch(docker_client, FakeKabinet())
    assert "FAKTS_ALLOW_INSECURE_TRANSPORT" not in docker_client.run_kwargs["environment"]

    # On a hub's own network the gateway answers plain HTTP, and the engine was told so.
    monkeypatch.setattr(D, "INSECURE_TRANSPORT", "1")
    launch(docker_client, FakeKabinet())
    assert docker_client.run_kwargs["environment"]["FAKTS_ALLOW_INSECURE_TRANSPORT"] == "1"


def test_a_network_mode_replaces_the_network():
    # On the mesh the engine shares a sidecar's namespace, and so must every plugin.
    docker_client = FakeDocker()
    ctx = make_ctx(docker_client)
    ctx.network_mode = "container:mesh"

    launch(docker_client, FakeKabinet(), ctx)

    assert docker_client.run_kwargs["network_mode"] == "container:mesh"
    assert "network" not in docker_client.run_kwargs


# --- describing the host --------------------------------------------------------------------


class ProbeDocker:
    """A daemon with the NVIDIA runtime, whose probe container answers ``answer``."""

    def __init__(self, answer):
        self.answer = answer
        self.probes: list[dict] = []
        self.containers = SimpleNamespace(run=self._run)

    def info(self):
        return {"Architecture": "x86_64", "NCPU": 8, "MemTotal": 16 * 1024**3, "Name": "host", "Runtimes": {"nvidia": {}}}

    def _run(self, image, command, **kwargs):
        self.probes.append({"image": image, "command": command, **kwargs})
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


def test_without_nvidia_smi_of_its_own_the_engine_reads_the_gpus_in_a_probe_container(monkeypatch):
    monkeypatch.setattr(D, "_nvidia_smi_here", lambda: None)
    client = ProbeDocker(b"1, 24576, 8.6\n1, 16384, 7.5\n")

    qualifiers = D._host_qualifiers(client)

    assert qualifiers["gpu"] == "true"
    assert (qualifiers["gpu_count"], qualifiers["gpu_memory_mb"], qualifiers["compute_capability"]) == ("2", "16384", "7.5")
    [probe] = client.probes
    assert probe["image"] == D.GPU_PROBE_IMAGE
    assert probe["remove"] is True
    # Without the utility capability the runtime does not mount nvidia-smi.
    assert probe["device_requests"][0]["Capabilities"] == [["gpu", "utility"]]


def test_a_failed_gpu_probe_still_says_there_is_a_gpu(monkeypatch):
    monkeypatch.setattr(D, "_nvidia_smi_here", lambda: None)
    client = ProbeDocker(docker.errors.ImageNotFound("debian:stable-slim"))

    qualifiers = D._host_qualifiers(client)

    assert qualifiers["gpu"] == "true"
    assert "gpu_count" not in qualifiers


def test_the_engines_own_nvidia_smi_needs_no_probe(monkeypatch):
    monkeypatch.setattr(D, "_nvidia_smi_here", lambda: "1, 8192, 8.0\n")
    client = ProbeDocker(AssertionError("no probe expected"))

    assert D._host_qualifiers(client)["gpu_count"] == "1"
    assert client.probes == []


# --- the declaration ------------------------------------------------------------------------


def test_the_engine_offers_its_actions_as_plugin_engine():
    assert D.app.identifier == "plugin_engine"
    assert D.AGENT_IDENTIFIER == D.app.identifier
    assert {
        "install",
        "deploy_flavour",
        "auto_install",
        "refresh_logs",
        "stop",
        "restart",
        "reauthorize",
        "remove",
        "upgrade",
    } <= set(D.app.registry.implementations)


# --- only approved content runs -------------------------------------------------------------


def test_only_digest_pinned_images_are_placed():
    tagged = SimpleNamespace(name="vanilla", image=SimpleNamespace(image_string="org/app:1.0"))
    pinned = SimpleNamespace(name="vanilla", image=SimpleNamespace(image_string="org/app@sha256:abc"))

    with pytest.raises(D.PlacementError, match="by digest"):
        D._check_image(tagged)
    D._check_image(pinned)


# --- upgrade waits for the new app to enroll -------------------------------------------------


class FakeTask:
    def __init__(self, cancel_after=None):
        self.checks = 0
        self.cancel_after = cancel_after

    def check_cancelled(self):
        self.checks += 1
        if self.cancel_after is not None and self.checks > self.cancel_after:
            raise ThreadCancelledError("cancelled")


def test_an_upgrade_proceeds_once_the_new_app_enrolled(monkeypatch):
    monkeypatch.setattr(D.koil, "sleep", lambda _s: None)
    kabinet = FakeKabinet([make_pod("new", status=PodStatus.RUNNING, client_id="c1")])
    D._await_enrolled(kabinet, SimpleNamespace(), FakeTask(), make_ctx(FakeDocker()), make_pod("new"), timeout=5)


def test_an_upgrade_whose_new_pod_failed_keeps_the_old_one(monkeypatch):
    monkeypatch.setattr(D.koil, "sleep", lambda _s: None)
    kabinet = FakeKabinet([make_pod("new", status=PodStatus.FAILED, client_id=None)])
    with pytest.raises(D.PlacementError, match="failed before its app enrolled"):
        D._await_enrolled(kabinet, SimpleNamespace(), FakeTask(), make_ctx(FakeDocker()), make_pod("new"), timeout=5)


def test_a_cancelled_upgrade_stops_waiting(monkeypatch):
    monkeypatch.setattr(D.koil, "sleep", lambda _s: None)
    kabinet = FakeKabinet([make_pod("new", status=PodStatus.PENDING, client_id=None)])
    with pytest.raises(ThreadCancelledError):
        D._await_enrolled(
            kabinet, SimpleNamespace(), FakeTask(cancel_after=3), make_ctx(FakeDocker()), make_pod("new"), timeout=60
        )
