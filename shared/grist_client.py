"""
Client pour l'API Grist
Module autonome pour la gestion des données Grist
Version améliorée avec détection des modifications
"""

import requests
import logging
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional

logger = logging.getLogger(__name__)

class GristClient:
    def __init__(self, base_url: str, token: str, doc_id: str):
        self.base_url = base_url
        self.token = token
        self.doc_id = doc_id
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def test_connection(self) -> Tuple[bool, Any]:
        """Test la connexion à Grist"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                try:
                    json_data = response.json()
                    return True, json_data
                except ValueError as e:
                    logger.error(f"Erreur JSON parse: {e}")
                    return False, f"Réponse non-JSON: {response.text[:200]}"
            else:
                logger.error(f"Status code: {response.status_code}, Text: {response.text[:500]}")
                return False, f"HTTP {response.status_code}: {response.text[:200]}"
        except Exception as e:
            logger.error(f"Erreur test connexion Grist: {e}")
            return False, str(e)
    
    def get_tables(self) -> Tuple[bool, Any]:
        """Récupère la liste des tables"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                try:
                    json_data = response.json()
                    return True, json_data
                except ValueError as e:
                    logger.error(f"Erreur JSON parse tables: {e}")
                    return False, f"Réponse non-JSON: {response.text[:200]}"
            else:
                logger.error(f"Get tables error - Status: {response.status_code}")
                return False, f"HTTP {response.status_code}: {response.text[:200]}"
        except Exception as e:
            logger.error(f"Erreur récupération tables: {e}")
            return False, str(e)
    
    def get_table_columns(self, table_id: str) -> Tuple[bool, Any]:
        """Récupère les colonnes d'une table"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/columns"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                try:
                    json_data = response.json()
                    return True, json_data
                except ValueError as e:
                    logger.error(f"Erreur JSON parse columns: {e}")
                    return False, f"Réponse non-JSON: {response.text[:200]}"
            else:
                logger.error(f"Get columns error - Status: {response.status_code}")
                return False, f"HTTP {response.status_code}: {response.text[:200]}"
        except Exception as e:
            logger.error(f"Erreur récupération colonnes: {e}")
            return False, str(e)
    
    def get_table_data(self, table_id: str, limit: Optional[int] = None) -> Tuple[bool, Any]:
        """Récupère les données d'une table"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
            params = {}
            if limit:
                params['limit'] = limit
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            if response.status_code == 200:
                return True, response.json()
            return False, response.text
        except Exception as e:
            logger.error(f"Erreur récupération données: {e}")
            return False, str(e)
    
    def update_record(self, table_id: str, record_id: int, fields: Dict[str, Any]) -> Tuple[bool, Any]:
        """Met à jour un enregistrement Grist"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
            
            data = {
                "records": [
                    {
                        "id": record_id,
                        "fields": fields
                    }
                ]
            }
            
            response = requests.patch(url, headers=self.headers, json=data, timeout=30)
            
            if response.status_code == 200:
                return True, response.json()
            else:
                logger.error(f"Erreur mise à jour record: {response.status_code} - {response.text}")
                return False, f"HTTP {response.status_code}: {response.text}"
                
        except Exception as e:
            logger.error(f"Erreur mise à jour record Grist: {e}")
            return False, str(e)
    
    def calculate_data_hash(self, record_fields: Dict[str, Any], columns_to_sync: List[str]) -> str:
        """Calcule un hash des données à synchroniser pour détecter les changements"""
        # Extraire seulement les colonnes qui sont synchronisées
        sync_data = {}
        for col in columns_to_sync:
            if col in record_fields:
                sync_data[col] = record_fields[col]
        
        # Créer un hash stable
        data_str = json.dumps(sync_data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()[:12]  # 12 caractères suffisent
    
    def update_sync_status(self, table_id: str, record_id: int, success: bool, message: str = "", timestamp: datetime = None, data_hash: str = None) -> Tuple[bool, Any]:
        """Met à jour le statut de synchronisation dans Grist avec hash des données"""
        if timestamp is None:
            timestamp = datetime.now()
            
        fields = {
            "sync_status": "success" if success else "error",
            "sync_date": timestamp.isoformat(),
            "sync_message": message[:500]  # Limiter la taille du message
        }
        
        # Ajouter le hash des données si fourni
        if data_hash:
            fields["sync_hash"] = data_hash
        
        return self.update_record(table_id, record_id, fields)
    
    def get_records_to_sync(self, table_id: str, dossier_number_column: str, status_column: str = "sync_status", columns_to_sync: List[str] = None, detect_changes: bool = True) -> Tuple[bool, Any]:
        """Récupère les enregistrements qui doivent être synchronisés avec détection automatique des changements"""
        try:
            success, data = self.get_table_data(table_id)
            if not success:
                return False, data
            
            # Filtrer les enregistrements qui ont un numéro de dossier et doivent être synchronisés
            records_to_sync = []
            for record in data['records']:
                fields = record['fields']
                
                # Vérifier qu'il y a un numéro de dossier
                if dossier_number_column not in fields or not fields[dossier_number_column]:
                    continue
                
                should_sync = False
                
                # Vérifier le statut (si la colonne existe)
                if status_column in fields:
                    status = fields[status_column]
                    # Synchroniser si pas encore fait ou en erreur
                    if status in [None, "", "error", "pending"]:
                        should_sync = True
                        logger.info(f"Record {record['id']}: sync car statut = '{status}'")
                    elif detect_changes and columns_to_sync and status == "success":
                        # Détecter les changements depuis la dernière sync
                        current_hash = self.calculate_data_hash(fields, columns_to_sync)
                        stored_hash = fields.get("sync_hash", "")
                        
                        if current_hash != stored_hash:
                            should_sync = True
                            logger.info(f"Record {record['id']}: sync car données modifiées (hash: {stored_hash} → {current_hash})")
                        else:
                            logger.debug(f"Record {record['id']}: pas de changement (hash: {current_hash})")
                else:
                    # Pas de colonne de statut = synchroniser
                    should_sync = True
                    logger.info(f"Record {record['id']}: sync car pas de colonne statut")
                
                if should_sync:
                    # Ajouter le hash actuel pour utilisation ultérieure
                    if columns_to_sync:
                        record['_current_hash'] = self.calculate_data_hash(fields, columns_to_sync)
                    records_to_sync.append(record)
            
            logger.info(f"Détection terminée: {len(records_to_sync)}/{len(data['records'])} enregistrements à synchroniser")
            return True, records_to_sync
            
        except Exception as e:
            logger.error(f"Erreur récupération records à synchroniser: {e}")
            return False, str(e)
    
    def get_column_type(self, table_id: str, column_id: str) -> str:
        """Récupère le type d'une colonne spécifique"""
        try:
            success, columns_data = self.get_table_columns(table_id)
            if not success:
                return 'Text'  # Type par défaut
            
            for column in columns_data.get('columns', []):
                if column['id'] == column_id:
                    # Gestion de la structure Grist
                    if 'fields' in column and 'type' in column['fields']:
                        return column['fields']['type']
                    else:
                        return column.get('type', 'Text')
            
            return 'Text'  # Type par défaut si colonne non trouvée
            
        except Exception as e:
            logger.error(f"Erreur récupération type colonne {column_id}: {e}")
            return 'Text'
    
    def prepare_sync_columns(self, table_id: str) -> Tuple[bool, Any]:
        """Prépare les colonnes de statut de synchronisation si elles n'existent pas"""
        try:
            # Vérifier si les colonnes existent déjà
            success, columns_data = self.get_table_columns(table_id)
            if not success:
                return False, "Impossible de récupérer les colonnes"
            
            existing_columns = [col['id'] for col in columns_data.get('columns', [])]
            
            columns_to_add = []
            
            # Colonnes nécessaires pour le suivi de synchronisation
            required_columns = {
                'sync_status': {'type': 'Text', 'label': 'Statut Sync'},
                'sync_date': {'type': 'DateTime', 'label': 'Date Sync'},
                'sync_message': {'type': 'Text', 'label': 'Message Sync'},
                'sync_hash': {'type': 'Text', 'label': 'Hash Données'}  # Nouvelle colonne pour détecter les changements
            }
            
            for col_id, col_config in required_columns.items():
                if col_id not in existing_columns:
                    columns_to_add.append({
                        'id': col_id,
                        **col_config
                    })
            
            if columns_to_add:
                logger.info(f"Colonnes à ajouter manuellement à la table {table_id}: {columns_to_add}")
                return True, {"columns_to_add": columns_to_add, "message": "Ajoutez ces colonnes manuellement"}
            
            return True, {"message": "Toutes les colonnes nécessaires existent"}
            
        except Exception as e:
            logger.error(f"Erreur préparation colonnes sync: {e}")
            return False, str(e)
    
    # Autres méthodes inchangées...
    def add_record(self, table_id: str, fields: Dict[str, Any]) -> Tuple[bool, Any]:
        """Ajoute un nouvel enregistrement à une table Grist"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
            
            data = {
                "records": [
                    {
                        "fields": fields
                    }
                ]
            }
            
            response = requests.post(url, headers=self.headers, json=data, timeout=30)
            
            if response.status_code == 200:
                return True, response.json()
            else:
                logger.error(f"Erreur ajout record: {response.status_code} - {response.text}")
                return False, f"HTTP {response.status_code}: {response.text}"
                
        except Exception as e:
            logger.error(f"Erreur ajout record Grist: {e}")
            return False, str(e)
    
    def delete_record(self, table_id: str, record_id: int) -> Tuple[bool, Any]:
        """Supprime un enregistrement d'une table Grist"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
            
            data = {
                "records": [record_id]
            }
            
            response = requests.delete(url, headers=self.headers, json=data, timeout=30)
            
            if response.status_code == 200:
                return True, response.json()
            else:
                logger.error(f"Erreur suppression record: {response.status_code} - {response.text}")
                return False, f"HTTP {response.status_code}: {response.text}"
                
        except Exception as e:
            logger.error(f"Erreur suppression record Grist: {e}")
            return False, str(e)
    
    def get_filtered_records(self, table_id: str, filters: Dict[str, Any] = None, limit: Optional[int] = None) -> Tuple[bool, Any]:
        """Récupère les enregistrements avec filtres optionnels"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
            params = {}
            
            if limit:
                params['limit'] = limit
            
            # Ajouter les filtres si fournis
            if filters:
                for key, value in filters.items():
                    params[f"filter[{key}]"] = value
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            
            if response.status_code == 200:
                return True, response.json()
            return False, response.text
            
        except Exception as e:
            logger.error(f"Erreur récupération données filtrées: {e}")
            return False, str(e)
    
    def bulk_update_records(self, table_id: str, updates: List[Dict[str, Any]]) -> Tuple[bool, Any]:
        """Met à jour plusieurs enregistrements en une seule fois"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
            
            # Formater les données pour l'API Grist
            records = []
            for update in updates:
                if 'id' in update and 'fields' in update:
                    records.append({
                        "id": update['id'],
                        "fields": update['fields']
                    })
            
            if not records:
                return False, "Aucun enregistrement à mettre à jour"
            
            data = {"records": records}
            
            response = requests.patch(url, headers=self.headers, json=data, timeout=60)
            
            if response.status_code == 200:
                return True, response.json()
            else:
                logger.error(f"Erreur mise à jour bulk: {response.status_code} - {response.text}")
                return False, f"HTTP {response.status_code}: {response.text}"
                
        except Exception as e:
            logger.error(f"Erreur mise à jour bulk Grist: {e}")
            return False, str(e)
    
    def get_document_info(self) -> Tuple[bool, Any]:
        """Récupère les informations du document"""
        try:
            url = f"{self.base_url}/docs/{self.doc_id}"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                return True, response.json()
            return False, f"HTTP {response.status_code}: {response.text}"
            
        except Exception as e:
            logger.error(f"Erreur récupération info document: {e}")
            return False, str(e)