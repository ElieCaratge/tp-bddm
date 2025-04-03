{
  description = "Devshell";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      ...
    }@inputs:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        # Add a package for notebook conversion
        packages.convert-notebook = pkgs.writeShellScriptBin "convert-notebook" ''
          set -euo pipefail

          if [ $# -eq 0 ]; then
            echo "Usage: nix run .#convert-notebook -- <path/to/notebook.ipynb>"
            exit 1
          fi

          notebook_path="$1"
          notebook_dir=$(dirname "$notebook_path")
          notebook_name=$(basename "$notebook_path" .ipynb)
          md_output="$notebook_dir/$notebook_name.md"
          pdf_output="$notebook_dir/$notebook_name.pdf"

          echo "Converting notebook to Markdown..."
          ${pkgs.python313Packages.nbconvert}/bin/jupyter-nbconvert --to markdown "$notebook_path"

          echo "Converting Markdown to PDF..."
          ${pkgs.pandoc}/bin/pandoc \
            --pdf-engine=xelatex \
            --variable mainfont="DejaVu Serif" \
            --variable sansfont="DejaVu Sans" \
            --variable monofont="DejaVu Sans Mono" \
            -V geometry:margin=1in \
            "$md_output" -o "$pdf_output"

          echo "Conversion complete. Output files:"
          echo "- Markdown: $md_output"
          echo "- PDF: $pdf_output"
        '';

        # Optional: make this the default package
        defaultPackage = self.packages.${system}.convert-notebook;

        # Development shell with all necessary tools
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            python313Full
            uv
            python313Packages.nbconvert
            pandoc
            texlive.combined.scheme-medium
            dejavu_fonts
          ];
          shellHook = ''
            echo "Hello from devshell!"
          '';
        };
      }
    );
}