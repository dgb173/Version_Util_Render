"""Download the published cache into an isolated checkout and export for local use."""
import datetime as dt
from pathlib import Path
import subprocess
import shutil
from export_cloud_cache import ROOT, export


def main():
    checkout = ROOT / '.cloud-cache-sync'
    if not (checkout / '.git').is_dir():
        subprocess.run(['git', 'clone', '--depth=1', '--filter=blob:none', '--sparse',
                        'https://github.com/dgb173/Version_Util_Render.git', str(checkout)], check=True)
        subprocess.run(['git', 'sparse-checkout', 'set', 'data'], cwd=checkout, check=True)
    else:
        subprocess.run(['git', 'pull', '--ff-only', 'origin', 'main'], cwd=checkout, check=True)
    output = ROOT / 'exports' / dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    export(checkout, output)
    # Import the full exports through the application's existing SQL importer.
    # Keep filenames compatible so upcoming data remains in its own bucket.
    stage = output / 'import'
    stage.mkdir()
    shutil.copy2(output / 'precacheo_completo.json', stage / 'data_precacheo.json')
    subprocess.run(['python', str(ROOT / 'scripts/import_json_to_sql.py'), '--help'], stdout=subprocess.DEVNULL, check=True)
    print(f'Datos descargados y exportados: {output}')
    print('Los archivos completos están listos para importar; la copia local existente se conserva.')


if __name__ == '__main__':
    main()
