# plugin_engine

A kabinet backend that runs approved app releases as Docker containers.

The engine registers one backend and one resource: the host of the Docker daemon it is
given. A user approves a release in the UI. The engine then places the release's
best-fitting flavour, image by digest only. It hands the container a single-use redeem
token, provisioned under the approver's mandate. Each container is reported to kabinet as
a pod, and a sweep keeps containers and pods in step. The module docstring in `app.py`
has the whole contract.

## Running it

The engine needs the Docker socket, so it is **not** something kabinet installs. Its
image declares no such requirement, and none can be declared. Whoever runs it mounts
the socket. Konstruktor runs one beside a hub, or as a standalone engine.

```sh
docker run -d --restart unless-stopped \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e FAKTS_URL=https://your.hub -e FAKTS_REDEEM_TOKEN=... \
  -e KABINET_DEPLOYER_ID=my-host -e ARKITEKT_DEVICE_ID=my-host \
  -e ARKITEKT_GATEWAY=https://your.hub -e ARKITEKT_NETWORK=your_network \
  ghcr.io/arkitektio/plugin_engine:<version>-vanilla arkitekt run prod
```

Set `KABINET_DEPLOYER_ID` and `ARKITEKT_DEVICE_ID` to values that outlive the container.
Their defaults are derived from the container itself. When those change, every plugin the
engine started is taken for someone else's.

If the daemon has the NVIDIA runtime, the GPUs are read with `nvidia-smi`. This image has
none, so the engine runs it in a throwaway `debian:stable-slim` container, set by
`DEPLOYER_GPU_PROBE_IMAGE`, given every GPU.

The other settings are listed in `app.py`.

## Developing

```sh
uv run pytest          # fakes for docker, kabinet and unlok; no server
arkitekt run dev       # against a hub, logging in in a browser
```

## Releasing

Every push to `main` is tested and published to `ghcr.io/arkitektio/plugin_engine`.
semantic-release reads the commits: a `fix:` cuts a patch, a `feat:` a minor, and a `!`
a major. Any other push publishes the latest build of the `main` channel.
