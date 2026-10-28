# CHANGELOG

<!-- version list -->

## v3.0.0 (2026-10-28)

### Features

- On arkitekt 9 (blok v2, the organization-identifier servers)
  ([`2b3f0fe`](https://github.com/arkitektio/plugin_engine/commit/2b3f0feefefdd0e64dd426ae2d971bfe6b9ad13e))

### Breaking Changes

- Needs a hub with the servers of that wave (rekuest 11, mikro 12, ...).


## v2.3.1 (2026-10-23)

### Bug Fixes

- **deps**: Arkitekt 8, arkitekt-runtime 5.1
  ([`dbc63ae`](https://github.com/arkitektio/plugin_engine/commit/dbc63ae27f98fd98fa1795f651bda310adf0ac49))


## v2.3.0 (2026-10-20)

### Features

- An unapproved release is approved by whoever called the engine
  ([`1db178a`](https://github.com/arkitektio/plugin_engine/commit/1db178a0d464287de31ba3108d66e0d61692a156))


## v2.2.1 (2026-10-20)

### Bug Fixes

- The engine verifies a hub's signed challenges
  ([`693b56d`](https://github.com/arkitektio/plugin_engine/commit/693b56d74f7ecaeb8758f0c58c447bd7e34ff46d))


## v2.2.0 (2026-10-20)

### Features

- The engine marks its containers with the instance id it is attached as
  ([`b08ea13`](https://github.com/arkitektio/plugin_engine/commit/b08ea13090e968ecc7cf9eb11d2c598201990fc4))


## v2.1.0 (2026-10-20)

### Features

- The engine declares the container engine it needs, for an installer to grant
  ([`fc47095`](https://github.com/arkitektio/plugin_engine/commit/fc47095521d0db83597f90a6cc97265c040edba0))


## v2.0.0 (2026-10-16)

### Features

- Run apps on mandate keys, and act on them as agents
  ([`c225fdc`](https://github.com/arkitektio/plugin_engine/commit/c225fdc256deea2dcd9ae78ebcd3086d924d43fb))

### Breaking Changes

- The engine itself logs in with FAKTS_MANDATE_KEY (a key for plugin_engine issued by a member), not
  FAKTS_REDEEM_TOKEN. DEPLOYER_TOKEN_TTL_MINUTES is gone. Pods started under redeem tokens were
  signed out by lok's migration and need installing again.


## v1.1.0 (2026-10-15)

### Bug Fixes

- Build on arkitekt 6.7
  ([`d4ee917`](https://github.com/arkitektio/plugin_engine/commit/d4ee917eeda90fe7f06e6459f348ef7689b0cb19))

### Features

- The engine and its actions have logos
  ([`c0abacd`](https://github.com/arkitektio/plugin_engine/commit/c0abacd29ff634268134ec071ff74d46927ce2a0))


## v1.0.1 (2026-10-15)

### Bug Fixes

- An app the engine starts may log in over plain HTTP where the engine may
  ([`100f89d`](https://github.com/arkitektio/plugin_engine/commit/100f89d239b50dca7630fdd211d5795ac84c29bd))


## v1.0.0 (2026-10-14)

- Initial Release
