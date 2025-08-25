#!/usr/bin/env python3
"""
Script CLI pour la synchronisation Grist → DS
Utilisation: python sync_cli.py --config config.json [options]
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime

from sync_engine import SyncEngine, SyncConfig

def setup_logging(level=logging.INFO, log_file=None):
    """Configure le logging"""
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def load_config(config_path: str) -> SyncConfig:
    """Charge la configuration depuis un fichier JSON"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # Valider les champs requis
        required_fields = [
            'grist_base_url', 'grist_token', 'grist_doc_id', 'grist_table_id',
            'dossier_number_column', 'ds_token', 'ds_instructeur_id', 
            'ds_demarche_number', 'column_mapping'
        ]
        
        for field in required_fields:
            if field not in config_data:
                raise ValueError(f"Champ requis manquant dans la configuration: {field}")
        
        return SyncConfig(**config_data)
        
    except Exception as e:
        print(f"Erreur chargement configuration: {e}")
        sys.exit(1)

def create_sample_config(output_path: str):
    """Crée un fichier de configuration exemple"""
    sample_config = {
        "grist_base_url": "https://grist.numerique.gouv.fr/api",
        "grist_token": "YOUR_GRIST_TOKEN",
        "grist_doc_id": "YOUR_DOC_ID",
        "grist_table_id": "YOUR_TABLE_NAME",
        "dossier_number_column": "dossier_number",
        "ds_token": "YOUR_DS_TOKEN",
        "ds_instructeur_id": "YOUR_INSTRUCTEUR_ID",
        "ds_demarche_number": 12345,
        "column_mapping": {
            "nom_entreprise": "Nom de l'entreprise",
            "montant_demande": "Montant demandé",
            "date_depot": "Date de dépôt"
        },
        "annotation_types": {
            "Nom de l'entreprise": "text",
            "Montant demandé": "decimal_number",
            "Date de dépôt": "date"
        },
        "limit": 50,
        "update_grist_status": True,
        "dry_run": False
    }
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(sample_config, f, indent=2, ensure_ascii=False)
        print(f"Configuration exemple créée: {output_path}")
        print("Éditez ce fichier avec vos paramètres avant utilisation.")
    except Exception as e:
        print(f"Erreur création configuration exemple: {e}")
        sys.exit(1)

def print_sync_report(result):
    """Affiche un rapport de synchronisation"""
    print("\n" + "="*60)
    print("RAPPORT DE SYNCHRONISATION")
    print("="*60)
    
    if result.success:
        print(f"✅ SUCCÈS - Synchronisation terminée en {result.execution_time:.2f}s")
    else:
        print(f"❌ ÉCHEC - Synchronisation interrompue après {result.execution_time:.2f}s")
    
    print(f"\n📊 STATISTIQUES:")
    print(f"   • Enregistrements traités: {result.processed}")
    print(f"   • Synchronisations réussies: {result.successful}")
    print(f"   • Erreurs: {result.errors}")
    
    if result.successful > 0:
        print(f"\n✅ SUCCÈS ({result.successful}):")
        for i, res in enumerate(result.results, 1):
            updates_count = len(res['updates'])
            print(f"   {i}. Dossier #{res['dossier_number']} - {updates_count} annotations mises à jour")
    
    if result.errors > 0:
        print(f"\n❌ ERREURS ({result.errors}):")
        for i, error in enumerate(result.error_details, 1):
            if 'dossier_number' in error:
                print(f"   {i}. Dossier #{error.get('dossier_number', 'N/A')} - {len(error.get('errors', []))} erreur(s)")
                for err in error.get('errors', [])[:2]:  # Limiter à 2 erreurs par dossier
                    print(f"      • {err}")
            else:
                print(f"   {i}. {error.get('error', 'Erreur inconnue')}")

def main():
    parser = argparse.ArgumentParser(
        description='Synchronisation automatique Grist → DS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  # Créer une configuration exemple
  python sync_cli.py --create-config config.json
  
  # Synchronisation normale
  python sync_cli.py --config config.json
  
  # Test sans modification (dry run)
  python sync_cli.py --config config.json --dry-run
  
  # Avec logging détaillé
  python sync_cli.py --config config.json --verbose --log-file sync.log
  
  # Limiter le nombre d'enregistrements
  python sync_cli.py --config config.json --limit 10
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Chemin vers le fichier de configuration JSON'
    )
    
    parser.add_argument(
        '--create-config',
        type=str,
        metavar='FICHIER',
        help='Crée un fichier de configuration exemple'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Mode test : valide les données sans les envoyer'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limite le nombre d\'enregistrements à traiter'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Active le logging détaillé'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Fichier de log (optionnel)'
    )
    
    parser.add_argument(
        '--test-connections',
        action='store_true',
        help='Teste uniquement les connexions'
    )
    
    args = parser.parse_args()
    
    # Créer configuration exemple
    if args.create_config:
        create_sample_config(args.create_config)
        return
    
    # Vérifier qu'un fichier de config est fourni
    if not args.config:
        parser.print_help()
        print("\nErreur: --config est requis (ou utilisez --create-config pour créer un exemple)")
        sys.exit(1)
    
    # Configurer le logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level, args.log_file)
    
    logger = logging.getLogger(__name__)
    logger.info("Début du script de synchronisation")
    
    # Charger la configuration
    config = load_config(args.config)
    
    # Appliquer les options de ligne de commande
    if args.dry_run:
        config.dry_run = True
        print("🧪 MODE TEST ACTIVÉ - Aucune modification ne sera effectuée")
    
    if args.limit:
        config.limit = args.limit
        logger.info(f"Limite fixée à {args.limit} enregistrements")
    
    # Créer le moteur de synchronisation
    sync_engine = SyncEngine(config)
    
    # Test des connexions seulement
    if args.test_connections:
        print("🔗 Test des connexions...")
        results = sync_engine.test_connections()
        
        print(f"Grist: {'✅ OK' if results['grist']['success'] else '❌ ÉCHEC'}")
        if not results['grist']['success']:
            print(f"   Erreur: {results['grist']['data']}")
        
        print(f"DS: {'✅ OK' if results['ds']['success'] else '❌ ÉCHEC'}")
        if not results['ds']['success']:
            print(f"   Erreur: {results['ds']['data']}")
        
        if results['overall_success']:
            print("✅ Toutes les connexions sont OK")
            sys.exit(0)
        else:
            print("❌ Une ou plusieurs connexions ont échoué")
            sys.exit(1)
    
    # Valider la configuration
    print("⚙️  Validation de la configuration...")
    config_valid, config_errors = sync_engine.validate_config()
    if not config_valid:
        print("❌ Configuration invalide:")
        for error in config_errors:
            print(f"   • {error}")
        sys.exit(1)
    
    print("✅ Configuration valide")
    
    # Afficher un résumé avant synchronisation
    print("\n📋 RÉSUMÉ DE LA SYNCHRONISATION:")
    print(f"   • Table Grist: {config.grist_table_id}")
    print(f"   • Colonne dossiers: {config.dossier_number_column}")
    print(f"   • Démarche DS: #{config.ds_demarche_number}")
    print(f"   • Colonnes à synchroniser: {len(config.column_mapping)}")
    for grist_col, ds_annotation in config.column_mapping.items():
        print(f"     - {grist_col} → {ds_annotation}")
    print(f"   • Limite: {config.limit} enregistrements")
    print(f"   • Mise à jour statut Grist: {'Oui' if config.update_grist_status else 'Non'}")
    
    if not args.dry_run:
        response = input("\n❓ Continuer la synchronisation ? [y/N]: ")
        if response.lower() not in ['y', 'yes', 'o', 'oui']:
            print("Synchronisation annulée")
            sys.exit(0)
    
    # Exécuter la synchronisation
    print("\n🚀 Début de la synchronisation...")
    try:
        result = sync_engine.execute_sync()
        print_sync_report(result)
        
        # Code de sortie basé sur le résultat
        if result.success and result.errors == 0:
            sys.exit(0)  # Succès total
        elif result.success and result.successful > 0:
            sys.exit(2)  # Succès partiel
        else:
            sys.exit(1)  # Échec
            
    except KeyboardInterrupt:
        print("\n⚠️  Synchronisation interrompue par l'utilisateur")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        print(f"\n💥 Erreur inattendue: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()