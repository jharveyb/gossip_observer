{
  description = "gossip_observer dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  # x86_64-linux only: .cargo/config.toml hard-codes target-cpu=x86-64-v3, and
  # the pinned service binaries below are linux/amd64 artifacts.
  outputs = { self, nixpkgs, rust-overlay }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ rust-overlay.overlays.default ];
      };

      # Honors rust-toolchain.toml (channel 1.95.0, default profile), so the
      # toolchain pin has a single source of truth shared with rustup users.
      rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

      # The integration test asserts garage v2.3.0 exactly
      # (integration_tests/tests/controller_collector.rs). This is the same
      # static musl artifact infra/ansible/tasks/garage.yml deploys.
      garage-bin = pkgs.stdenvNoCC.mkDerivation rec {
        pname = "garage-bin";
        version = "2.3.0";
        src = pkgs.fetchurl {
          url = "https://garagehq.deuxfleurs.fr/_releases/v${version}/x86_64-unknown-linux-musl/garage";
          hash = "sha256-+Y0xeUK7NBFRondRYgFrtQz4a4ZdAQjeA+tdsW4hIM0=";
        };
        dontUnpack = true;
        installPhase = "install -Dm755 $src $out/bin/garage";
      };

      # Matches the production pin (nats_version in
      # infra/ansible/inventory/group_vars/all.yml); same release artifact as
      # infra/ansible/tasks/nats.yml. Static Go binary, no patching needed.
      nats-server-bin = pkgs.stdenvNoCC.mkDerivation rec {
        pname = "nats-server-bin";
        version = "2.12.14";
        src = pkgs.fetchurl {
          url = "https://github.com/nats-io/nats-server/releases/download/v${version}/nats-server-v${version}-linux-amd64.tar.gz";
          hash = "sha256-hOclH2p0XllQua3yFZY4op+/3OwEDBNnSyNio0RrTqo=";
        };
        installPhase = "install -Dm755 nats-server $out/bin/nats-server";
      };

      # MCP server behind the `bkb` registration (Bitcoin Knowledge Base —
      # CLAUDE.md's BOLT/BIP lookup tools). Pinned to the same rev as the
      # laptop's `cargo install` (see ~/.cargo/.crates.toml).
      bkb-mcp = pkgs.rustPlatform.buildRustPackage {
        pname = "bkb-mcp";
        version = "0.1.0";
        src = pkgs.fetchFromGitHub {
          owner = "tnull";
          repo = "bitcoin-knowledge-base";
          rev = "3d917d6aa61d8ea5bfab0f3dadfc9a1173bd8b85";
          hash = "sha256-rRwo+wWksq7mTcbo6EoTzcb3+vE8wU7Bj0cmiWO95I0=";
        };
        cargoHash = "sha256-4kUq0LZjrVQrUIzAmvJpxNJkxjeP6puvSZL1xZU0tPc=";
        nativeBuildInputs = [ pkgs.pkg-config ];
        buildInputs = [ pkgs.openssl ];
      };

      # For analysis scripts with deps that uv cannot provide: graph-tool is
      # not pip-installable (query_results/channel_graph_analysis_graphtool.py).
      # The uv-managed venvs in analysis/ and query_results/ are unaffected.
      analysisPython = pkgs.python3.withPackages (ps: [
        ps.graph-tool
        ps.scikit-learn
      ]);
    in
    {
      packages.${system} = { inherit garage-bin nats-server-bin bkb-mcp; };

      devShells.${system}.default = pkgs.mkShell {
        packages = with pkgs; [
          rustToolchain
          mold

          # Build-time native deps: observer_common/build.rs needs protoc on
          # every build; DuckDB/zstd/sqlite are bundled C/C++ sources.
          protobuf
          pkg-config
          openssl
          cmake

          # Workspace tooling
          just
          sqlx-cli
          taplo
          git-cliff
          bkb-mcp # MCP server for the `bkb` BOLT/BIP lookup tools (CLAUDE.md)

          # Integration test service binaries (also exported as *_EXE below)
          bitcoind
          garage-bin
          nats-server-bin

          # Ops / analysis CLIs
          duckdb
          postgresql_18 # matches the prod pin (client tools: psql, pg_dump, ...)
          rclone
          grpcurl
          jq
          rsync
          zstd
          wireshark-cli # capinfos, for scripts/pull_pcaps.sh

          # Python: uv drives analysis/ and query_results/ (uv.lock each)
          uv
          analysisPython

          tmux
        ];

        env = {
          # Test harness overrides (integration_tests); PATH lookup would also
          # work, but explicit paths survive PATH edits inside the shell.
          BITCOIND_EXE = "${pkgs.bitcoind}/bin/bitcoind";
          NATS_SERVER_EXE = "${nats-server-bin}/bin/nats-server";
          GARAGE_EXE = "${garage-bin}/bin/garage";
          # justfile sets this too; exported here so bare cargo builds outside
          # just don't try to reach a database.
          SQLX_OFFLINE = "true";
        };
      };
    };
}
