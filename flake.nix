{
  description = "TBA to R2 ETL using uv";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          # Only system-level tools go here
          packages = with pkgs; [
            python311
            uv
            duckdb
            # Common libraries needed for Polars/Arrow wheels on Linux
            stdenv.cc.cc.lib
            zlib
          ];

          shellHook = ''
            # Fixes common library loading issues for Python wheels
            export LD_LIBRARY_PATH="${pkgs.stdenv.cc.cc.lib}/lib:$LD_LIBRARY_PATH"

            echo "--- 🤖 TBA ETL Shell (uv Mode) ---"

            # Auto-initialize uv environment if it doesn't exist
            if [ ! -f "pyproject.toml" ]; then
              echo "Initializing uv project..."
              uv init --python 3.11
              uv add polars httpx boto3 python-dotenv
            fi

            echo "Python: $(python --version)"
            echo "uv:     $(uv --version)"
            echo "DuckDB: $(duckdb --version)"
            echo "----------------------------------"
          '';
        };
      }
    );
}
