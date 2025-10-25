#!/usr/bin/env python3
"""
Interface web simplifiée pour la configuration et le lancement de la synchronisation Grist → DS
Version finale avec persistance et détection des modifications
"""

from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, session
import requests
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from sync_engine import SyncEngine, SyncConfig
from grist_client import GristClient
from ds_client import DSClient

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this'

# Fichier de configuration persistante
CONFIG_FILE = 'app_config.json'

def load_persistent_config():
    """Charge la configuration depuis le fichier"""
    default_config = {
        'ds_token': '',
        'instructeur_id': '',
        'grist_token': '',
        'grist_doc_id': '',
        'demarche_number': '',
        'grist_base_url': 'https://grist.numerique.gouv.fr/api'
    }
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                saved_config = json.load(f)
                default_config.update(saved_config)
        except Exception as e:
            logger.error(f"Erreur chargement config: {e}")
    
    return default_config

def save_persistent_config(config):
    """Sauvegarde la configuration dans le fichier"""
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erreur sauvegarde config: {e}")

# Configuration globale avec persistance
config = load_persistent_config()

@app.route('/')
def index():
    return render_template('index.html', config=config)

@app.route('/configure', methods=['POST'])
def configure():
    """Configure les tokens et identifiants avec sauvegarde persistante"""
    config['ds_token'] = request.form.get('ds_token', '').strip()
    config['instructeur_id'] = request.form.get('instructeur_id', '').strip()
    config['grist_token'] = request.form.get('grist_token', '').strip()
    config['grist_doc_id'] = request.form.get('grist_doc_id', '').strip()
    config['demarche_number'] = request.form.get('demarche_number', '').strip()
    
    if not all([config['ds_token'], config['instructeur_id'], config['grist_token'], config['grist_doc_id']]):
        flash('Les champs DS Token, Instructeur ID, Grist Token et Doc ID sont requis', 'error')
        return redirect(url_for('index'))
    
    # Sauvegarder de façon persistante
    save_persistent_config(config)
    
    # Sauvegarder en session aussi
    session.update(config)
    
    flash('Configuration mise à jour et sauvegardée avec succès', 'success')
    return redirect(url_for('index'))

@app.route('/test_connections')
def test_connections():
    """Test les connexions DS et Grist"""
    if not all([config['grist_token'], config['grist_doc_id']]):
        return jsonify({'error': 'Configuration Grist incomplète'}), 400
    
    results = {}
    
    # Test Grist
    grist_client = GristClient(config['grist_base_url'], config['grist_token'], config['grist_doc_id'])
    grist_success, grist_data = grist_client.test_connection()
    results['grist'] = {
        'success': grist_success,
        'data': grist_data
    }
    
    # Test DS seulement si le token est configuré
    if config['ds_token']:
        ds_client = DSClient(config['ds_token'], config['instructeur_id'])
        ds_success, ds_data = ds_client.test_connection()
        results['ds'] = {
            'success': ds_success,
            'data': ds_data
        }
    else:
        results['ds'] = {
            'success': False,
            'data': 'Token DS non configuré'
        }
    
    return jsonify(results)

@app.route('/get_grist_tables')
def get_grist_tables():
    """Récupère les tables Grist"""
    if not config['grist_token'] or not config['grist_doc_id']:
        return jsonify({'error': 'Configuration Grist incomplète'}), 400
    
    try:
        grist_client = GristClient(config['grist_base_url'], config['grist_token'], config['grist_doc_id'])
        success, data = grist_client.get_tables()
        
        if success:
            return jsonify({'success': True, 'tables': data['tables']})
        else:
            logger.error(f"Erreur Grist get_tables: {data}")
            return jsonify({'success': False, 'error': str(data)}), 500
    except Exception as e:
        logger.error(f"Exception get_grist_tables: {e}")
        return jsonify({'success': False, 'error': f'Erreur serveur: {str(e)}'}), 500

@app.route('/get_table_columns/<table_id>')
def get_table_columns(table_id):
    """Récupère les colonnes d'une table Grist"""
    if not config['grist_token'] or not config['grist_doc_id']:
        return jsonify({'error': 'Configuration Grist incomplète'}), 400
    
    try:
        grist_client = GristClient(config['grist_base_url'], config['grist_token'], config['grist_doc_id'])
        success, data = grist_client.get_table_columns(table_id)
        
        if success:
            # Filtrer et valider les colonnes
            filtered_columns = []
            
            for col in data['columns']:
                try:
                    col_id = col.get('id', 'Unknown')
                    
                    if 'fields' in col and 'type' in col['fields']:
                        col_type = col['fields']['type']
                        col_label = col['fields'].get('label', col_id)
                    else:
                        col_type = col.get('type', 'Text')
                        col_label = col.get('label', col_id)
                    
                    clean_col = {
                        'id': col_id,
                        'type': col_type,
                        'label': col_label
                    }
                    
                    # Filtrer seulement les types supportés
                    if col_type in ['Text', 'Date', 'DateTime', 'Numeric', 'Int', 'Choice', 'Bool']:
                        filtered_columns.append(clean_col)
                        
                except Exception as e:
                    logger.error(f"Erreur traitement colonne {col}: {e}")
                    continue
            
            return jsonify({'success': True, 'columns': filtered_columns})
        else:
            logger.error(f"Erreur Grist get_table_columns: {data}")
            return jsonify({'success': False, 'error': str(data)}), 500
    except Exception as e:
        logger.error(f"Exception get_table_columns: {e}")
        return jsonify({'success': False, 'error': f'Erreur serveur: {str(e)}'}), 500

@app.route('/get_table_data/<table_id>')
def get_table_data(table_id):
    """Récupère un échantillon des données d'une table"""
    if not config['grist_token'] or not config['grist_doc_id']:
        return jsonify({'error': 'Configuration Grist incomplète'}), 400
    
    try:
        limit = request.args.get('limit', 5, type=int)
        grist_client = GristClient(config['grist_base_url'], config['grist_token'], config['grist_doc_id'])
        success, data = grist_client.get_table_data(table_id, limit=limit)
        
        if success:
            return jsonify({'success': True, 'records': data['records']})
        else:
            logger.error(f"Erreur Grist get_table_data: {data}")
            return jsonify({'success': False, 'error': str(data)}), 500
    except Exception as e:
        logger.error(f"Exception get_table_data: {e}")
        return jsonify({'success': False, 'error': f'Erreur serveur: {str(e)}'}), 500

@app.route('/get_instructeurs')
def get_instructeurs():
    """Récupère la liste des instructeurs de la démarche"""
    if not config['ds_token'] or not config['demarche_number']:
        return jsonify({'error': 'Configuration DS incomplète (token et numéro de démarche requis)'}), 400

    try:
        logger.info(f"Début get_instructeurs - Démarche: {config['demarche_number']}")

        ds_client = DSClient(config['ds_token'], config.get('instructeur_id', ''))

        # Test de connexion d'abord
        conn_success, conn_data = ds_client.test_connection()
        if not conn_success:
            logger.error(f"Connexion DS échouée: {conn_data}")
            return jsonify({'success': False, 'error': f'Connexion DS échouée: {conn_data}'}), 500

        logger.info("Connexion DS OK, récupération des instructeurs...")

        # Récupérer les instructeurs
        success, instructeurs_data = ds_client.get_instructeurs(int(config['demarche_number']))
        if not success:
            logger.error(f"Erreur get_instructeurs: {instructeurs_data}")
            return jsonify({'success': False, 'error': f'Erreur récupération instructeurs: {instructeurs_data}'}), 500

        if not instructeurs_data:
            logger.warning("Aucun instructeur trouvé")
            return jsonify({'success': False, 'error': 'Aucun instructeur trouvé dans cette démarche'}), 400

        logger.info(f"Instructeurs trouvés: {len(instructeurs_data)}")

        return jsonify({
            'success': True,
            'instructeurs': instructeurs_data,
            'total': len(instructeurs_data)
        })

    except Exception as e:
        logger.error(f"Exception globale get_instructeurs: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': f'Erreur serveur: {str(e)}'}), 500

@app.route('/get_sample_annotations')
def get_sample_annotations():
    """Récupère les annotations d'un dossier échantillon"""
    if not config['ds_token'] or not config['demarche_number']:
        return jsonify({'error': 'Configuration DS incomplète'}), 400
    
    try:
        logger.info(f"Début get_sample_annotations - Démarche: {config['demarche_number']}")
        
        ds_client = DSClient(config['ds_token'], config['instructeur_id'])
        
        # Test de connexion d'abord
        conn_success, conn_data = ds_client.test_connection()
        if not conn_success:
            logger.error(f"Connexion DS échouée: {conn_data}")
            return jsonify({'success': False, 'error': f'Connexion DS échouée: {conn_data}'}), 500
        
        logger.info("Connexion DS OK, récupération des dossiers...")
        
        # Récupérer quelques dossiers pour trouver un échantillon avec des annotations
        success, dossiers_data = ds_client.get_dossiers(int(config['demarche_number']), limit=10)
        if not success:
            logger.error(f"Erreur get_dossiers: {dossiers_data}")
            return jsonify({'success': False, 'error': f'Erreur récupération dossiers: {dossiers_data}'}), 500
        
        if not dossiers_data:
            logger.warning("Aucun dossier trouvé")
            return jsonify({'success': False, 'error': 'Aucun dossier trouvé dans cette démarche'}), 400
        
        logger.info(f"Dossiers trouvés: {len(dossiers_data)}")
        
        # Essayer de récupérer les annotations du premier dossier avec des annotations
        sample_annotations = []
        used_dossier_number = None
        errors_log = []
        
        for i, dossier in enumerate(dossiers_data[:5]):  # Tester jusqu'à 5 dossiers
            dossier_num = dossier['number']
            logger.info(f"Test dossier {i+1}/5: #{dossier_num}")
            
            try:
                success, annotations = ds_client.get_dossier_annotations(dossier_num)
                if success and annotations and len(annotations) > 0:
                    sample_annotations = annotations
                    used_dossier_number = dossier_num
                    logger.info(f"✅ Annotations trouvées dans le dossier {dossier_num}: {len(annotations)} annotations")
                    break
                else:
                    if success:
                        logger.info(f"⚠️ Dossier {dossier_num}: {len(annotations) if annotations else 0} annotations")
                    else:
                        logger.warning(f"❌ Dossier {dossier_num}: Erreur - {annotations}")
                        errors_log.append(f"Dossier {dossier_num}: {annotations}")
            except Exception as e:
                logger.error(f"💥 Exception dossier {dossier_num}: {e}")
                errors_log.append(f"Dossier {dossier_num}: Exception - {str(e)}")
                continue
        
        if not sample_annotations:
            error_msg = f'Aucune annotation trouvée dans les dossiers échantillons. Erreurs: {"; ".join(errors_log[:3])}'
            logger.error(error_msg)
            return jsonify({'success': False, 'error': error_msg}), 400
        
        logger.info(f"Traitement de {len(sample_annotations)} annotations")
        
        # Extraire les informations des annotations
        annotation_labels = []
        for annotation in sample_annotations:
            try:
                annotation_labels.append({
                    'id': annotation['id'],
                    'label': annotation['label'],
                    'current_value': annotation.get('stringValue', ''),
                    'champDescriptorId': annotation.get('champDescriptorId', ''),
                    'ds_type': annotation.get('ds_type', 'text'),
                    '__typename': annotation.get('__typename', 'Unknown')
                })
            except Exception as e:
                logger.error(f"Erreur traitement annotation {annotation}: {e}")
                continue
        
        logger.info(f"Retour de {len(annotation_labels)} annotations traitées")
        
        return jsonify({
            'success': True,
            'annotations': annotation_labels,
            'sample_dossier_number': used_dossier_number,
            'source': 'dossier_annotations',
            'debug_info': {
                'total_dossiers': len(dossiers_data),
                'tested_dossiers': min(5, len(dossiers_data)),
                'errors_count': len(errors_log)
            }
        })
        
    except Exception as e:
        logger.error(f"💥 Exception globale get_sample_annotations: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': f'Erreur serveur: {str(e)}'}), 500

@app.route('/execute_sync', methods=['POST'])
def execute_sync():
    """Exécute la synchronisation en utilisant le moteur de synchronisation"""
    try:
        data = request.get_json()
        
        # Créer la configuration de synchronisation
        sync_config = SyncConfig(
            grist_base_url=config['grist_base_url'],
            grist_token=config['grist_token'],
            grist_doc_id=config['grist_doc_id'],
            grist_table_id=data.get('table_id'),
            dossier_number_column=data.get('dossier_id_column'),
            ds_token=config['ds_token'],
            ds_instructeur_id=config['instructeur_id'],
            ds_demarche_number=int(config['demarche_number']),
            column_mapping=data.get('column_mapping', {}),
            annotation_types=data.get('annotation_types', {}),
            limit=data.get('limit', 50),
            update_grist_status=data.get('update_grist_status', True),
            dry_run=data.get('dry_run', False),
            detect_changes=data.get('detect_changes', True)  # Nouvelle option
        )
        
        # Créer et exécuter le moteur de synchronisation
        sync_engine = SyncEngine(sync_config)
        result = sync_engine.execute_sync()
        
        # Convertir le résultat en format JSON
        return jsonify({
            'success': result.success,
            'processed': result.processed,
            'successful': result.successful,
            'errors': result.errors,
            'results': result.results,
            'error_details': result.error_details,
            'execution_time': result.execution_time
        })
        
    except Exception as e:
        logger.error(f"Erreur synchronisation: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/validate_compatibility', methods=['POST'])
def validate_compatibility():
    """Valide la compatibilité des données avant synchronisation"""
    try:
        data = request.get_json()
        
        # Créer un client DS temporaire pour les vérifications
        ds_client = DSClient(config['ds_token'], config['instructeur_id'])
        grist_client = GristClient(config['grist_base_url'], config['grist_token'], config['grist_doc_id'])
        
        # Récupérer quelques échantillons de données
        success, sample_data = grist_client.get_table_data(data.get('table_id'), limit=5)
        if not success:
            return jsonify({'success': False, 'error': f'Erreur récupération données: {sample_data}'}), 500
        
        compatibility_results = []
        
        for grist_col, annotation_label in data.get('column_mapping', {}).items():
            # Récupérer le type de la colonne Grist
            grist_type = grist_client.get_column_type(data.get('table_id'), grist_col)
            
            # Récupérer le type de l'annotation DS depuis annotation_types passé en paramètre
            ds_type = data.get('annotation_types', {}).get(annotation_label, 'text')
            
            # Tester avec des valeurs échantillon
            sample_values = []
            for record in sample_data['records']:
                if grist_col in record['fields']:
                    sample_values.append(record['fields'][grist_col])
            
            # Vérifier la compatibilité
            compatibilities = []
            for value in sample_values:
                if value is not None and value != '':
                    compatibility = ds_client.check_compatibility(grist_type, ds_type, value)
                    compatibilities.append(compatibility)
            
            # Déterminer la compatibilité globale
            if not compatibilities:
                overall_compatibility = 'compatible'
            elif 'incompatible' in compatibilities:
                overall_compatibility = 'incompatible'
            elif 'needs_conversion' in compatibilities:
                overall_compatibility = 'needs_conversion'
            else:
                overall_compatibility = 'compatible'
            
            compatibility_results.append({
                'grist_column': grist_col,
                'annotation_label': annotation_label,
                'grist_type': grist_type,
                'ds_type': ds_type,
                'compatibility': overall_compatibility,
                'sample_values': sample_values[:3]
            })
        
        return jsonify({
            'success': True,
            'compatibility_results': compatibility_results
        })
        
    except Exception as e:
        logger.error(f"Erreur validation compatibilité: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/export_config', methods=['POST'])
def export_config():
    """Exporte la configuration pour utilisation en CLI SANS les tokens sensibles"""
    try:
        data = request.get_json()
        
        config_export = {
            "grist_base_url": config['grist_base_url'],
            # "grist_token": config['grist_token'],  # ← SUPPRIMÉ pour sécurité
            "grist_doc_id": config['grist_doc_id'],
            "grist_table_id": data.get('table_id'),
            "dossier_number_column": data.get('dossier_id_column'),
            # "ds_token": config['ds_token'],  # ← SUPPRIMÉ pour sécurité
            # "ds_instructeur_id": config['instructeur_id'],  # ← SUPPRIMÉ pour sécurité
            "ds_demarche_number": int(config['demarche_number']),
            "column_mapping": data.get('column_mapping', {}),
            "annotation_types": data.get('annotation_types', {}),
            "limit": data.get('limit', 50),
            "update_grist_status": True,
            "dry_run": False,
            "detect_changes": True,
            "exported_at": datetime.now().isoformat(),
            "exported_from": "web_interface",
            "_security_note": "Tokens exclus - À configurer via secrets GitHub ou variables d'environnement"
        }
        
        return jsonify({
            'success': True,
            'config': config_export
        })
        
    except Exception as e:
        logger.error(f"Erreur export config: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/sync')
def sync_page():
    """Page de synchronisation automatique"""
    if not all([config['ds_token'], config['grist_token'], config['grist_doc_id']]):
        flash('Configuration incomplète. Veuillez d\'abord configurer vos tokens.', 'error')
        return redirect(url_for('index'))
    
    return render_template('sync.html', config=config)

@app.route('/clear_config', methods=['POST'])
def clear_config():
    """Efface la configuration sauvegardée"""
    try:
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)
        
        # Réinitialiser la config en mémoire
        global config
        config = load_persistent_config()
        
        flash('Configuration effacée avec succès', 'success')
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erreur effacement config: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print(f"Configuration chargée depuis: {os.path.abspath(CONFIG_FILE)}")
    print("Nouvelles fonctionnalités:")
    print("- Configuration persistante automatique")
    print("- Détection automatique des modifications des données")
    print("- Colonne sync_hash pour optimiser les synchronisations") 
    app.run(debug=True, host='0.0.0.0', port=5000)