{
  pkgs ? import <nixpkgs> { },
}:

let
  python = pkgs.python312;
in
pkgs.mkShell {
  buildInputs = with pkgs; [
    python
    uv
    cyclonedds
  ];

  # Shell hook to set up environment
  shellHook = ''
    export TMPDIR=/tmp
    export UV_PYTHON="${python}/bin/python"
    export CYCLONEDDS_HOME="${pkgs.cyclonedds}"
    export CMAKE_PREFIX_PATH="${pkgs.cyclonedds}:$CMAKE_PREFIX_PATH"
    export LD_LIBRARY_PATH="$CYCLONEDDS_HOME/lib:$LD_LIBRARY_PATH"
  '';
}
