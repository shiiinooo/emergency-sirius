import os
import logging
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URLs des fichiers à télécharger pour chaque année
URLS = {
    "2019": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/529/ssr_2019_couts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/529/ssr_2019_couts_moyens_par_gme_oqn.xlsx"
    },
    "2018": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/488/ssr_2018_couts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/488/ssr_2018_couts_moyens_par_gme_oqn.xlsx"
    },
    "2017": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/480/ssr_2017_couts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/480/ssr_2017_couts_moyens_par_gme_oqn.xlsx"
    },
    "2016": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/430/ssr_2016_couts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/430/ssr_2016_couts_moyens_par_gme_oqn.xlsx"
    },
    "2015": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/373/ssr_2015_couts_moyens_par_gme_daf_0.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/373/ssr_2015_couts_moyens_par_gme_oqn_0.xlsx"
    },
    "2014": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/311/ssr_2014_couts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/311/ssr_2014_couts_moyens_par_gme_oqn.xlsx"
    },
    "2013": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/274/ssr_2013_couts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/274/ssr_2013_couts_moyens_par_gme_oqn.xlsx"
    },
    "2012": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/210/ssr_2012_couts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/210/ssr_2012_couts_moyens_par_gme_oqn.xlsx"
    },
    "2011": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/212/ssr_2011_co_ts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/212/ssr_2011_co_ts_moyens_par_gme_oqn.xlsx"
    },
    "2010": {
        "daf": "https://www.scansante.fr/sites/www.scansante.fr/files/content/213/ssr_2010_co_ts_moyens_par_gme_daf.xlsx",
        "oqn": "https://www.scansante.fr/sites/www.scansante.fr/files/content/213/ssr_2010_co_ts_moyens_par_gme_oqn.xlsx"
    }
}

# Répertoire de sortie
OUTPUT_DIR = "/home/airflow/data/bronze/scansante"

def download_file(url, output_filename):
    """Télécharge un fichier depuis une URL et le sauvegarde dans le répertoire spécifié."""
    try:
        # Assurez-vous que le répertoire de sortie existe
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Téléchargez le fichier
        response = requests.get(url)
        response.raise_for_status()  # Vérifiez les erreurs HTTP
        logging.info(f"Fichier téléchargé avec succès depuis {url}")

        # Sauvegardez le fichier
        filepath = os.path.join(OUTPUT_DIR, output_filename)
        with open(filepath, "wb") as file:
            file.write(response.content)
        logging.info(f"Fichier sauvegardé dans : {filepath}")

        return filepath

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau ou téléchargement pour {url} : {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Une erreur inattendue est survenue pour {url} : {str(e)}")
        raise

def fetch_scansante_data():
    """Télécharge tous les fichiers DAF et OQN pour chaque année."""
    try:
        # Itérer sur les années et télécharger les fichiers DAF et OQN
        for year, urls in URLS.items():
            daf_url = urls["daf"]
            oqn_url = urls["oqn"]

            # Téléchargez le fichier DAF
            download_file(daf_url, f"scansante_data_daf_{year}.xlsx")

            # Téléchargez le fichier OQN
            download_file(oqn_url, f"scansante_data_oqn_{year}.xlsx")

    except Exception as e:
        logging.error(f"Une erreur est survenue lors du téléchargement des fichiers : {str(e)}")

# Exécutez le script si ce fichier est appelé directement
if __name__ == "__main__":
    fetch_scansante_data()