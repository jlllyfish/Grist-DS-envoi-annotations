"""
Moteur de synchronisation Grist → DS
Version simplifiée avec détection automatique des modifications
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass

from ds_client import DSClient
from grist_client import GristClient

logger = logging.getLogger(__name__)

@dataclass
class SyncConfig:
    """Configuration pour la synchronisation"""
    # Grist
    grist_base_url: str
    grist_token: str
    grist_doc_id: str
    grist_table_id: str
    dossier_number_column: str
    
    # DS
    ds_token: str
    ds_instructeur_id: str
    ds_demarche_number: int
    
    # Mapping des colonnes
    column_mapping: Dict[str, str]  # {grist_column: ds_annotation_label}
    annotation_types: Dict[str, str] = None  # {annotation_label: ds_type}
    
    # Options
    limit: int = 50
    update_grist_status: bool = True
    dry_run: bool = False
    detect_changes: bool = True  # Nouvelle option pour détecter les modifications

@dataclass
class SyncResult:
    """Résultat d'une synchronisation"""
    success: bool
    processed: int
    successful: int
    errors: int
    results: List[Dict]
    error_details: List[Dict]
    execution_time: float

class SyncEngine:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.grist_client = GristClient(
            config.grist_base_url,
            config.grist_token,
            config.grist_doc_id
        )
        self.ds_client = DSClient(
            config.ds_token,
            config.ds_instructeur_id
        )
        
    def test_connections(self) -> Dict[str, Any]:
        """Test les connexions à Grist et DS"""
        results = {}
        
        # Test Grist
        grist_success, grist_data = self.grist_client.test_connection()
        results['grist'] = {
            'success': grist_success,
            'data': grist_data
        }
        
        # Test DS
        ds_success, ds_data = self.ds_client.test_connection()
        results['ds'] = {
            'success': ds_success,
            'data': ds_data
        }
        
        results['overall_success'] = grist_success and ds_success
        return results
    
    def validate_config(self) -> Tuple[bool, List[str]]:
        """Valide la configuration"""
        errors = []
        
        # Vérifier les paramètres requis
        required_fields = [
            'grist_base_url', 'grist_token', 'grist_doc_id', 'grist_table_id',
            'dossier_number_column', 'ds_token', 'ds_instructeur_id', 
            'ds_demarche_number', 'column_mapping'
        ]
        
        for field in required_fields:
            if not getattr(self.config, field):
                errors.append(f"Champ requis manquant: {field}")
        
        # Vérifier que le mapping n'est pas vide
        if not self.config.column_mapping:
            errors.append("Le mapping des colonnes est vide")
        
        # Vérifier les connexions
        connection_results = self.test_connections()
        if not connection_results['grist']['success']:
            errors.append(f"Connexion Grist échouée: {connection_results['grist']['data']}")
        
        if not connection_results['ds']['success']:
            errors.append(f"Connexion DS échouée: {connection_results['ds']['data']}")
        
        return len(errors) == 0, errors
    
    def get_annotation_types_mapping(self) -> Dict[str, str]:
        """Récupère le mapping des types d'annotations DS"""
        if self.config.annotation_types:
            return self.config.annotation_types
        
        logger.warning("Pas de types d'annotations dans la config - ils seront récupérés dynamiquement depuis les dossiers")
        return {}
    
    def validate_data_compatibility(self, grist_records: List[Dict]) -> Dict[str, Any]:
        """Valide la compatibilité des données avant synchronisation"""
        compatibility_report = {
            'compatible_count': 0,
            'needs_conversion_count': 0,
            'incompatible_count': 0,
            'details': []
        }
        
        # Récupérer les types de colonnes Grist
        grist_column_types = {}
        for grist_col in self.config.column_mapping.keys():
            grist_column_types[grist_col] = self.grist_client.get_column_type(
                self.config.grist_table_id, grist_col
            )
        
        # Récupérer les types d'annotations DS
        annotation_types = self.get_annotation_types_mapping()
        
        # Vérifier la compatibilité pour chaque mapping
        for grist_col, annotation_label in self.config.column_mapping.items():
            grist_type = grist_column_types.get(grist_col, 'Text')
            ds_type = annotation_types.get(annotation_label, 'text')
            
            # Tester avec des valeurs échantillon
            sample_values = []
            for record in grist_records[:5]:  # Prendre 5 échantillons
                if grist_col in record['fields']:
                    sample_values.append(record['fields'][grist_col])
            
            # Vérifier la compatibilité
            compatibilities = []
            for value in sample_values:
                if value is not None and value != '':
                    compatibility = self.ds_client.check_compatibility(grist_type, ds_type, value)
                    compatibilities.append(compatibility)
            
            # Déterminer la compatibilité globale
            if not compatibilities:
                overall_compatibility = 'compatible'  # Pas de données à tester
            elif 'incompatible' in compatibilities:
                overall_compatibility = 'incompatible'
            elif 'needs_conversion' in compatibilities:
                overall_compatibility = 'needs_conversion'
            else:
                overall_compatibility = 'compatible'
            
            # Compter
            if overall_compatibility == 'compatible':
                compatibility_report['compatible_count'] += 1
            elif overall_compatibility == 'needs_conversion':
                compatibility_report['needs_conversion_count'] += 1
            else:
                compatibility_report['incompatible_count'] += 1
            
            compatibility_report['details'].append({
                'grist_column': grist_col,
                'annotation_label': annotation_label,
                'grist_type': grist_type,
                'ds_type': ds_type,
                'compatibility': overall_compatibility,
                'sample_values': sample_values[:3]  # Garder quelques échantillons
            })
        
        return compatibility_report
    
    def sync_record(self, grist_record: Dict, dossier_mapping: Dict[int, str]) -> Dict[str, Any]:
        """Synchronise un enregistrement Grist vers DS"""
        record_id = grist_record['id']
        fields = grist_record['fields']
        
        result = {
            'grist_record_id': record_id,
            'dossier_number': None,
            'dossier_uuid': None,
            'updates': [],
            'errors': [],
            'status': 'pending'
        }
        
        try:
            # Récupérer et valider le numéro de dossier
            if self.config.dossier_number_column not in fields:
                result['errors'].append(f"Colonne {self.config.dossier_number_column} introuvable")
                result['status'] = 'error'
                return result
            
            raw_dossier_number = fields[self.config.dossier_number_column]
            if not raw_dossier_number:
                result['errors'].append("Numéro de dossier vide")
                result['status'] = 'error'
                return result
            
            # Nettoyer et convertir le numéro de dossier
            try:
                if isinstance(raw_dossier_number, str):
                    dossier_number = int(raw_dossier_number.strip())
                elif isinstance(raw_dossier_number, (int, float)):
                    dossier_number = int(raw_dossier_number)
                else:
                    raise ValueError(f"Type non supporté: {type(raw_dossier_number)}")
                
                if dossier_number <= 0:
                    raise ValueError("Le numéro de dossier doit être positif")
                    
            except (ValueError, TypeError) as e:
                result['errors'].append(f"Numéro de dossier invalide: {raw_dossier_number} ({e})")
                result['status'] = 'error'
                return result
            
            result['dossier_number'] = dossier_number
            
            # Récupérer l'UUID du dossier
            if dossier_number not in dossier_mapping:
                result['errors'].append(f"Dossier {dossier_number} non trouvé dans la démarche")
                result['status'] = 'error'
                return result
            
            dossier_uuid = dossier_mapping[dossier_number]
            result['dossier_uuid'] = dossier_uuid
            
            # Récupérer les annotations du dossier avec les types automatiques
            success, annotations = self.ds_client.get_dossier_annotations(dossier_number)
            if not success:
                result['errors'].append(f"Erreur récupération annotations: {annotations}")
                result['status'] = 'error'
                return result
            
            # Créer un mapping des annotations par label avec leurs types
            annotations_by_label = {}
            dynamic_annotation_types = {}
            
            for ann in annotations:
                annotations_by_label[ann['label']] = ann
                # Utiliser le type ds_type automatiquement ajouté par get_dossier_annotations
                if 'ds_type' in ann:
                    dynamic_annotation_types[ann['label']] = ann['ds_type']
                else:
                    logger.warning(f"Annotation {ann['label']} sans ds_type, utilisation de 'text'")
                    dynamic_annotation_types[ann['label']] = 'text'
            
            logger.info(f"Types d'annotations récupérés dynamiquement: {dynamic_annotation_types}")
            
            # Utiliser les types dynamiques en priorité, puis ceux de la config
            final_annotation_types = {**self.get_annotation_types_mapping(), **dynamic_annotation_types}
            
            # Traiter chaque colonne à synchroniser
            for grist_col, annotation_label in self.config.column_mapping.items():
                if grist_col not in fields:
                    continue
                
                value = fields[grist_col]
                if value is None or (value == '' and final_annotation_types.get(annotation_label, 'text') != 'checkbox'):
                    continue  # Ignorer les valeurs vides sauf pour les checkboxes
                
                # Trouver l'annotation
                if annotation_label not in annotations_by_label:
                    result['errors'].append(f"Annotation '{annotation_label}' non trouvée")
                    continue
                
                annotation = annotations_by_label[annotation_label]
                ds_type = final_annotation_types.get(annotation_label, 'text')
                grist_type = self.grist_client.get_column_type(self.config.grist_table_id, grist_col)
                
                # Vérifier la compatibilité
                compatibility = self.ds_client.check_compatibility(grist_type, ds_type, value)
                if compatibility == 'incompatible':
                    result['errors'].append(f"Types incompatibles pour {grist_col}: {grist_type} → {ds_type}")
                    continue
                
                # Mode dry run
                if self.config.dry_run:
                    result['updates'].append({
                        'grist_column': grist_col,
                        'annotation_label': annotation_label,
                        'annotation_id': annotation['id'],
                        'value': value,
                        'ds_type': ds_type,
                        'grist_type': grist_type,
                        'compatibility': compatibility,
                        'status': 'dry_run'
                    })
                    continue
                
                # Effectuer la mise à jour avec le bon type
                success, update_result = self.ds_client.update_annotation_by_type(
                    dossier_uuid, annotation['id'], value, ds_type, grist_type
                )
                
                if success:
                    result['updates'].append({
                        'grist_column': grist_col,
                        'annotation_label': annotation_label,
                        'annotation_id': annotation['id'],
                        'value': value,
                        'ds_type': ds_type,
                        'grist_type': grist_type,
                        'compatibility': compatibility,
                        'status': 'success'
                    })
                else:
                    result['errors'].append(f"Erreur mise à jour {annotation_label}: {update_result}")
            
            # Déterminer le statut final
            if result['errors']:
                result['status'] = 'partial_error' if result['updates'] else 'error'
            else:
                result['status'] = 'success'
            
        except Exception as e:
            logger.error(f"Erreur synchronisation record {record_id}: {e}")
            result['errors'].append(str(e))
            result['status'] = 'error'
        
        return result
    
    def execute_sync(self) -> SyncResult:
        """Exécute la synchronisation complète"""
        start_time = datetime.now()
        
        logger.info("Début de la synchronisation")
        
        # Valider la configuration
        config_valid, config_errors = self.validate_config()
        if not config_valid:
            return SyncResult(
                success=False,
                processed=0,
                successful=0,
                errors=len(config_errors),
                results=[],
                error_details=[{'error': err, 'status': 'config_error'} for err in config_errors],
                execution_time=0
            )
        
        try:
            # Récupérer les données Grist à synchroniser avec détection des modifications
            if self.config.update_grist_status:
                # Passer les colonnes à synchroniser pour la détection des changements
                columns_to_sync = list(self.config.column_mapping.keys())
                success, grist_records = self.grist_client.get_records_to_sync(
                    self.config.grist_table_id,
                    self.config.dossier_number_column,
                    columns_to_sync=columns_to_sync,
                    detect_changes=self.config.detect_changes
                )
            else:
                success, grist_data = self.grist_client.get_table_data(
                    self.config.grist_table_id,
                    limit=self.config.limit
                )
                grist_records = grist_data['records'] if success else []
            
            if not success:
                return SyncResult(
                    success=False,
                    processed=0,
                    successful=0,
                    errors=1,
                    results=[],
                    error_details=[{'error': f'Erreur Grist: {grist_records}', 'status': 'grist_error'}],
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
            
            if not grist_records:
                logger.info("Aucun enregistrement à synchroniser")
                return SyncResult(
                    success=True,
                    processed=0,
                    successful=0,
                    errors=0,
                    results=[],
                    error_details=[],
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
            
            # Limiter le nombre d'enregistrements
            if len(grist_records) > self.config.limit:
                grist_records = grist_records[:self.config.limit]
            
            logger.info(f"Synchronisation de {len(grist_records)} enregistrements")
            
            # Valider la compatibilité des données
            compatibility_report = self.validate_data_compatibility(grist_records)
            logger.info(f"Rapport de compatibilité: {compatibility_report['compatible_count']} compatibles, "
                       f"{compatibility_report['needs_conversion_count']} avec conversion, "
                       f"{compatibility_report['incompatible_count']} incompatibles")
            
            # Récupérer tous les dossiers de la démarche pour créer le mapping
            success, dossiers_data = self.ds_client.get_dossiers(self.config.ds_demarche_number, limit=1000)
            if not success:
                return SyncResult(
                    success=False,
                    processed=0,
                    successful=0,
                    errors=1,
                    results=[],
                    error_details=[{'error': f'Erreur récupération dossiers DS: {dossiers_data}', 'status': 'ds_error'}],
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
            
            # Créer le mapping numéro → UUID
            dossier_mapping = {dossier['number']: dossier['id'] for dossier in dossiers_data}
            
            # Synchroniser chaque enregistrement
            results = []
            error_details = []
            successful_count = 0
            
            for grist_record in grist_records:
                sync_result = self.sync_record(grist_record, dossier_mapping)
                
                if sync_result['status'] == 'success':
                    results.append(sync_result)
                    successful_count += 1
                else:
                    error_details.append(sync_result)
                
                # Mettre à jour le statut dans Grist si configuré
                if self.config.update_grist_status and not self.config.dry_run:
                    success_status = sync_result['status'] == 'success'
                    message = f"Synchronized {len(sync_result['updates'])} annotations" if success_status else f"Errors: {len(sync_result['errors'])}"
                    
                    # Utiliser le hash des données pour la détection des changements
                    data_hash = grist_record.get('_current_hash')
                    
                    self.grist_client.update_sync_status(
                        self.config.grist_table_id,
                        grist_record['id'],
                        success_status,
                        message,
                        data_hash=data_hash
                    )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Synchronisation terminée: {successful_count}/{len(grist_records)} réussis en {execution_time:.2f}s")
            
            return SyncResult(
                success=True,
                processed=len(grist_records),
                successful=successful_count,
                errors=len(error_details),
                results=results,
                error_details=error_details,
                execution_time=execution_time
            )
            
        except Exception as e:
            logger.error(f"Erreur lors de la synchronisation: {e}")
            return SyncResult(
                success=False,
                processed=0,
                successful=0,
                errors=1,
                results=[],
                error_details=[{'error': str(e), 'status': 'unexpected_error'}],
                execution_time=(datetime.now() - start_time).total_seconds()
            )