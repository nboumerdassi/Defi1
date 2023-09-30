

import nbconvert
import sys

def convert_ipynb_to_py(ipynb_file, py_file):
    """
    Convertit un fichier .ipynb en fichier .py.
    
    Parameters:
    - ipynb_file: chemin vers le fichier .ipynb
    - py_file: chemin de sortie pour le fichier .py
    """
    exporter = nbconvert.PythonExporter()
    output, _ = exporter.from_filename(ipynb_file)
    
    with open(py_file, 'w') as outfile:
        outfile.write(output)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} source.ipynb destination.py")
        sys.exit(1)

    ipynb_file = sys.argv[1]
    py_file = sys.argv[2]
    
    convert_ipynb_to_py(ipynb_file, py_file)
