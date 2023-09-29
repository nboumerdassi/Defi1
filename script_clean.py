

import nbformat
import sys

def clear_outputs(notebook_path):
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = nbformat.read(f, as_version=4)

    for cell in notebook.cells:
        if cell.cell_type == 'code':
            cell.outputs = []
            cell.execution_count = None

    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(notebook, f)

# Utilisation:
# python clear_outputs.py VOTRE_NOTEBOOK.ipynb

if __name__ == "__main__":
    if len(sys.argv) > 1:
        clear_outputs(sys.argv[1])
    else:
        print("Veuillez spécifier le chemin du notebook à réinitialiser.")
