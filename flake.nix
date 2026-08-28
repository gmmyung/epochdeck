{
  description = "Runloom development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      supportedSystems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              actionlint
              cargo
              clippy
              just
              nodejs_24
              openssl
              pkg-config
              pnpm
              python313
              rust-analyzer
              rustc
              rustfmt
              sqlite
              uv
            ];

            shellHook = ''
              export RUNLOOM_DATA_DIR="$PWD/data"
              export RUNLOOM_METRICS_DIR="$PWD/data/metrics"
              export RUNLOOM_BLOBS_DIR="$PWD/data/blobs"
              export RUST_BACKTRACE=1
            '';
          };
        }
      );
    };
}
