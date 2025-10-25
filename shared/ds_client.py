"""
Client pour l'API Démarches Simplifiées
Module autonome pour la gestion des annotations DS
Version complète et corrigée
"""

import requests
import logging
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional

logger = logging.getLogger(__name__)

class DSClient:
    def __init__(self, token: str, instructeur_id: str):
        self.token = token
        self.instructeur_id = instructeur_id
        self.base_url = "https://www.demarches-simplifiees.fr/api/v2/graphql"
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def test_connection(self) -> Tuple[bool, Any]:
        """Test la connexion à DS avec une requête simple"""
        try:
            query = """
            query {
                __schema {
                    queryType {
                        name
                    }
                }
            }
            """
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={'query': query},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' in data:
                    return False, data['errors']
                return True, "Connexion DS réussie"
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            logger.error(f"Erreur test connexion DS: {e}")
            return False, str(e)
    
    def get_instructeurs(self, demarche_number: int) -> Tuple[bool, Any]:
        """Récupère les groupes d'instructeurs et leurs instructeurs pour une démarche"""
        try:
            query = """
            query getInstructeurs($demarcheNumber: Int!) {
                demarche(number: $demarcheNumber) {
                    id
                    number
                    title
                    groupeInstructeurs {
                        id
                        number
                        label
                        instructeurs {
                            id
                            email
                        }
                    }
                }
            }
            """
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    'query': query,
                    'variables': {'demarcheNumber': int(demarche_number)}
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                if 'errors' in data:
                    return False, data['errors']
                if not data['data']['demarche']:
                    return False, "Démarche non trouvée"

                groupe_instructeurs = data['data']['demarche']['groupeInstructeurs']

                # Extraire tous les instructeurs uniques
                instructeurs_map = {}
                for groupe in groupe_instructeurs:
                    for instructeur in groupe['instructeurs']:
                        instructeur_id = instructeur['id']
                        if instructeur_id not in instructeurs_map:
                            instructeurs_map[instructeur_id] = {
                                'id': instructeur_id,
                                'email': instructeur['email'],
                                'groupes': []
                            }
                        instructeurs_map[instructeur_id]['groupes'].append({
                            'id': groupe['id'],
                            'label': groupe['label']
                        })

                instructeurs_list = list(instructeurs_map.values())
                logger.info(f"Instructeurs récupérés: {len(instructeurs_list)}")
                return True, instructeurs_list
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            logger.error(f"Erreur récupération instructeurs: {e}")
            return False, str(e)

    def get_dossiers(self, demarche_number: int, limit: int = 50) -> Tuple[bool, Any]:
        """Récupère les dossiers d'une démarche par son numéro"""
        try:
            query = """
            query getDossiers($demarcheNumber: Int!, $first: Int) {
                demarche(number: $demarcheNumber) {
                    id
                    number
                    title
                    dossiers(first: $first) {
                        nodes {
                            id
                            number
                            state
                            dateDerniereModification
                            annotations {
                                id
                                label
                                champDescriptorId
                                stringValue
                            }
                        }
                    }
                }
            }
            """
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    'query': query,
                    'variables': {'demarcheNumber': int(demarche_number), 'first': limit}
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                if 'errors' in data:
                    return False, data['errors']
                if not data['data']['demarche']:
                    return False, "Démarche non trouvée"

                dossiers = data['data']['demarche']['dossiers']['nodes']
                logger.info(f"Dossiers récupérés: {len(dossiers)}")
                return True, dossiers
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            logger.error(f"Erreur récupération dossiers: {e}")
            return False, str(e)
    
    def get_dossier_annotations(self, dossier_number: int) -> Tuple[bool, Any]:
        """Récupère les annotations d'un dossier par son numéro avec détection automatique des types"""
        try:
            query = """
            query getDossier($dossierNumber: Int!) {
                dossier(number: $dossierNumber) {
                    id
                    number
                    annotations {
                        id
                        label
                        champDescriptorId
                        stringValue
                        __typename
                    }
                }
            }
            """
            
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    'query': query,
                    'variables': {'dossierNumber': int(dossier_number)}
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' in data:
                    logger.error(f"Erreurs GraphQL: {data['errors']}")
                    return False, data['errors']
                if not data['data']['dossier']:
                    return False, "Dossier non trouvé"
                
                annotations = data['data']['dossier']['annotations']
                
                # Enrichir chaque annotation avec son type normalisé depuis __typename
                for annotation in annotations:
                    typename = annotation.get('__typename', 'TextChamp')
                    annotation['ds_type'] = self._normalize_annotation_type(typename)
                
                return True, annotations
            else:
                return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            logger.error(f"Erreur récupération annotations: {e}")
            return False, str(e)
    
    def _normalize_annotation_type(self, typename: str) -> str:
        """Convertit les __typename GraphQL en types normalisés"""
        type_mapping = {
            'TextChamp': 'text',
            'TextareaChamp': 'textarea', 
            'IntegerNumberChamp': 'integer_number',
            'DecimalNumberChamp': 'decimal_number',
            'CheckboxChamp': 'checkbox',
            'DateChamp': 'date',
            'DatetimeChamp': 'datetime',
            'DropDownListChamp': 'drop_down_list',
            # Fallbacks
            'NumberChamp': 'decimal_number',
        }
        
        return type_mapping.get(typename, 'text')
    
    def get_annotation_types(self, demarche_number: int) -> Tuple[bool, Any]:
        """DÉSACTIVÉ - Les types sont maintenant récupérés via __typename dans get_dossier_annotations"""
        logger.info("get_annotation_types désactivé - utilisation de __typename")
        return True, []
    
    def check_compatibility(self, grist_type: str, ds_annotation_type: str, grist_value: Any = None) -> str:
        """Vérifie la compatibilité entre un type Grist et un type DS"""
        compatibility_map = {
            'Text': {
                'text': 'compatible',
                'textarea': 'compatible', 
                'checkbox': 'needs_conversion',
                'date': 'needs_conversion',
                'datetime': 'needs_conversion',
                'number': 'needs_conversion',
                'integer_number': 'needs_conversion',
                'decimal_number': 'needs_conversion',
                'drop_down_list': 'compatible'
            },
            'Numeric': {
                'text': 'compatible',
                'textarea': 'compatible',
                'number': 'compatible',
                'integer_number': 'compatible',
                'decimal_number': 'compatible',
                'checkbox': 'incompatible',
                'date': 'incompatible',
                'datetime': 'incompatible',
                'drop_down_list': 'needs_conversion'
            },
            'Int': {
                'text': 'compatible',
                'textarea': 'compatible',
                'number': 'compatible',
                'integer_number': 'compatible',
                'decimal_number': 'compatible',
                'checkbox': 'incompatible',
                'date': 'incompatible',
                'datetime': 'incompatible',
                'drop_down_list': 'needs_conversion'
            },
            'Date': {
                'text': 'compatible',
                'textarea': 'compatible',
                'date': 'compatible',
                'datetime': 'compatible',
                'checkbox': 'incompatible',
                'number': 'incompatible',
                'integer_number': 'incompatible',
                'decimal_number': 'incompatible',
                'drop_down_list': 'incompatible'
            },
            'DateTime': {
                'text': 'compatible',
                'textarea': 'compatible',
                'date': 'needs_conversion',
                'datetime': 'compatible',
                'checkbox': 'incompatible',
                'number': 'incompatible',
                'integer_number': 'incompatible',
                'decimal_number': 'incompatible',
                'drop_down_list': 'incompatible'
            },
            'Bool': {
                'text': 'compatible',
                'textarea': 'compatible',
                'checkbox': 'compatible',
                'date': 'incompatible',
                'datetime': 'incompatible',
                'number': 'incompatible',
                'integer_number': 'incompatible',
                'decimal_number': 'incompatible',
                'drop_down_list': 'needs_conversion'
            }
        }
        
        ds_type_normalized = ds_annotation_type.lower().replace('annotation_descriptor_', '')
        
        if grist_type in compatibility_map:
            compatibility = compatibility_map[grist_type].get(ds_type_normalized, 'incompatible')
            
            # Vérifications spéciales avec la valeur
            if grist_value is not None:
                if ds_type_normalized == 'checkbox' and grist_type == 'Text':
                    if str(grist_value).lower() in ['true', 'false', '1', '0', 'oui', 'non', 'yes', 'no']:
                        compatibility = 'compatible'
                    else:
                        compatibility = 'incompatible'
                
                # Text → Int/Number : vérifier si la valeur peut être convertie
                if ds_type_normalized in ['number', 'integer_number', 'decimal_number'] and grist_type == 'Text':
                    try:
                        if ds_type_normalized == 'integer_number':
                            int(float(str(grist_value)))
                        else:
                            float(str(grist_value))
                        compatibility = 'compatible'
                    except (ValueError, TypeError):
                        compatibility = 'incompatible'
            
            return compatibility
        
        return 'incompatible'

    def format_value_for_ds(self, value: Any, ds_annotation_type: str, grist_type: str = None) -> Any:
        """Formate une valeur Grist pour l'annotation DS"""
        if value is None or value == '':
            return None
            
        ds_type = ds_annotation_type.lower().replace('annotation_descriptor_', '')
        
        try:
            if ds_type == 'checkbox':
                if isinstance(value, bool):
                    return value
                str_val = str(value).lower()
                return str_val in ['true', '1', 'oui', 'yes', 'on']
            
            elif ds_type in ['number', 'decimal_number']:
                return float(value)
                
            elif ds_type == 'integer_number':
                return int(float(value))
                
            elif ds_type in ['date', 'datetime']:
                if isinstance(value, str) and not value.endswith('Z'):
                    if 'T' not in value:
                        if ds_type == 'datetime':
                            value = f"{value}T00:00:00.000Z"
                    elif not value.endswith('Z'):
                        value = f"{value}Z"
                return value
            
            else:  # text, textarea, drop_down_list
                return str(value)
                
        except (ValueError, TypeError) as e:
            logger.error(f"Erreur formatage valeur {value} pour type {ds_type}: {e}")
            return str(value)
    
    def _execute_mutation(self, mutation: str, variables: dict) -> Tuple[bool, Any]:
        """Exécute une mutation GraphQL"""
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={'query': mutation, 'variables': variables},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'errors' in data:
                    return False, data['errors']
                return True, data
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            logger.error(f"Erreur exécution mutation: {e}")
            return False, str(e)
    
    def update_annotation_text(self, dossier_id: str, annotation_id: str, value: Any) -> Tuple[bool, Any]:
        """Met à jour une annotation texte"""
        mutation = """
        mutation dossierModifierAnnotationText($input: DossierModifierAnnotationTextInput!) {
            dossierModifierAnnotationText(input: $input) {
                annotation {
                    id
                    label
                    stringValue
                    updatedAt
                }
                errors {
                    message
                }
            }
        }
        """
        
        variables = {
            "input": {
                "annotationId": annotation_id,
                "clientMutationId": f"update-text-{annotation_id}",
                "dossierId": dossier_id,
                "instructeurId": self.instructeur_id,
                "value": str(value)
            }
        }
        
        success, data = self._execute_mutation(mutation, variables)
        if success:
            result = data['data']['dossierModifierAnnotationText']
            if result['errors']:
                return False, result['errors']
            return True, result['annotation']
        return False, data
    
    def update_annotation_checkbox(self, dossier_id: str, annotation_id: str, value: Any) -> Tuple[bool, Any]:
        """Met à jour une annotation checkbox"""
        mutation = """
        mutation dossierModifierAnnotationCheckbox($input: DossierModifierAnnotationCheckboxInput!) {
            dossierModifierAnnotationCheckbox(input: $input) {
                annotation {
                    id
                    label
                    stringValue
                    updatedAt
                }
                errors {
                    message
                }
            }
        }
        """
        
        bool_value = self.format_value_for_ds(value, 'checkbox')
        
        variables = {
            "input": {
                "annotationId": annotation_id,
                "clientMutationId": f"update-checkbox-{annotation_id}",
                "dossierId": dossier_id,
                "instructeurId": self.instructeur_id,
                "value": bool_value
            }
        }
        
        success, data = self._execute_mutation(mutation, variables)
        if success:
            result = data['data']['dossierModifierAnnotationCheckbox']
            if result['errors']:
                return False, result['errors']
            return True, result['annotation']
        return False, data
    
    def update_annotation_date(self, dossier_id: str, annotation_id: str, value: Any) -> Tuple[bool, Any]:
        """Met à jour une annotation date"""
        mutation = """
        mutation dossierModifierAnnotationDate($input: DossierModifierAnnotationDateInput!) {
            dossierModifierAnnotationDate(input: $input) {
                annotation {
                    id
                    label
                    stringValue
                    updatedAt
                }
                errors {
                    message
                }
            }
        }
        """
        
        formatted_value = self.format_value_for_ds(value, 'date')
        
        variables = {
            "input": {
                "annotationId": annotation_id,
                "clientMutationId": f"update-date-{annotation_id}",
                "dossierId": dossier_id,
                "instructeurId": self.instructeur_id,
                "value": str(formatted_value)
            }
        }
        
        success, data = self._execute_mutation(mutation, variables)
        if success:
            result = data['data']['dossierModifierAnnotationDate']
            if result['errors']:
                return False, result['errors']
            return True, result['annotation']
        return False, data
    
    def update_annotation_datetime(self, dossier_id: str, annotation_id: str, value: Any) -> Tuple[bool, Any]:
        """Met à jour une annotation datetime"""
        mutation = """
        mutation dossierModifierAnnotationDatetime($input: DossierModifierAnnotationDatetimeInput!) {
            dossierModifierAnnotationDatetime(input: $input) {
                annotation {
                    id
                    label
                    stringValue
                    updatedAt
                }
                errors {
                    message
                }
            }
        }
        """
        
        formatted_value = self.format_value_for_ds(value, 'datetime')
        
        variables = {
            "input": {
                "annotationId": annotation_id,
                "clientMutationId": f"update-datetime-{annotation_id}",
                "dossierId": dossier_id,
                "instructeurId": self.instructeur_id,
                "value": str(formatted_value)
            }
        }
        
        success, data = self._execute_mutation(mutation, variables)
        if success:
            result = data['data']['dossierModifierAnnotationDatetime']
            if result['errors']:
                return False, result['errors']
            return True, result['annotation']
        return False, data
    
    def update_annotation_integer_number(self, dossier_id: str, annotation_id: str, value: Any) -> Tuple[bool, Any]:
        """Met à jour une annotation nombre entier"""
        try:
            mutation = """
            mutation dossierModifierAnnotationIntegerNumber($input: DossierModifierAnnotationIntegerNumberInput!) {
                dossierModifierAnnotationIntegerNumber(input: $input) {
                    annotation {
                        id
                        label
                        stringValue
                        updatedAt
                    }
                    errors {
                        message
                    }
                }
            }
            """
            
            int_value = self.format_value_for_ds(value, 'integer_number')
            
            variables = {
                "input": {
                    "annotationId": annotation_id,
                    "clientMutationId": f"update-integer-{annotation_id}",
                    "dossierId": dossier_id,
                    "instructeurId": self.instructeur_id,
                    "value": int_value
                }
            }
            
            success, data = self._execute_mutation(mutation, variables)
            if success:
                result = data['data']['dossierModifierAnnotationIntegerNumber']
                if result['errors']:
                    return False, result['errors']
                return True, result['annotation']
            return False, data
        except Exception as e:
            logger.error(f"Exception mise à jour annotation integer: {e}")
            return False, str(e)
    
    def update_annotation_decimal_number(self, dossier_id: str, annotation_id: str, value: Any) -> Tuple[bool, Any]:
        """Met à jour une annotation nombre décimal"""
        mutation = """
        mutation dossierModifierAnnotationDecimalNumber($input: DossierModifierAnnotationDecimalNumberInput!) {
            dossierModifierAnnotationDecimalNumber(input: $input) {
                annotation {
                    id
                    label
                    stringValue
                    updatedAt
                }
                errors {
                    message
                }
            }
        }
        """
        
        decimal_value = self.format_value_for_ds(value, 'decimal_number')
        
        variables = {
            "input": {
                "annotationId": annotation_id,
                "clientMutationId": f"update-decimal-{annotation_id}",
                "dossierId": dossier_id,
                "instructeurId": self.instructeur_id,
                "value": decimal_value
            }
        }
        
        success, data = self._execute_mutation(mutation, variables)
        if success:
            result = data['data']['dossierModifierAnnotationDecimalNumber']
            if result['errors']:
                return False, result['errors']
            return True, result['annotation']
        return False, data
    
    def update_annotation_dropdown(self, dossier_id: str, annotation_id: str, value: Any) -> Tuple[bool, Any]:
        """Met à jour une annotation liste déroulante"""
        mutation = """
        mutation dossierModifierAnnotationDropDownList($input: DossierModifierAnnotationDropDownListInput!) {
            dossierModifierAnnotationDropDownList(input: $input) {
                annotation {
                    id
                    label
                    stringValue
                    updatedAt
                }
                errors {
                    message
                }
            }
        }
        """
        
        variables = {
            "input": {
                "annotationId": annotation_id,
                "clientMutationId": f"update-dropdown-{annotation_id}",
                "dossierId": dossier_id,
                "instructeurId": self.instructeur_id,
                "value": str(value)
            }
        }
        
        success, data = self._execute_mutation(mutation, variables)
        if success:
            result = data['data']['dossierModifierAnnotationDropDownList']
            if result['errors']:
                return False, result['errors']
            return True, result['annotation']
        return False, data
    
    def update_annotation_by_type(self, dossier_id: str, annotation_id: str, value: Any, annotation_type: str, grist_type: str = None) -> Tuple[bool, Any]:
        """Met à jour une annotation en utilisant la bonne mutation selon le type"""
        if not value and value != False:
            return True, "Valeur vide ignorée"
        
        ds_type = annotation_type.lower().replace('annotation_descriptor_', '')
        
        try:
            if ds_type in ['text', 'textarea']:
                return self.update_annotation_text(dossier_id, annotation_id, value)
            elif ds_type == 'checkbox':
                return self.update_annotation_checkbox(dossier_id, annotation_id, value)
            elif ds_type == 'date':
                return self.update_annotation_date(dossier_id, annotation_id, value)
            elif ds_type == 'datetime':
                return self.update_annotation_datetime(dossier_id, annotation_id, value)
            elif ds_type == 'integer_number':
                return self.update_annotation_integer_number(dossier_id, annotation_id, value)
            elif ds_type == 'decimal_number':
                return self.update_annotation_decimal_number(dossier_id, annotation_id, value)
            elif ds_type == 'drop_down_list':
                return self.update_annotation_dropdown(dossier_id, annotation_id, value)
            else:
                logger.warning(f"Type d'annotation non reconnu: {ds_type}, utilisation de text")
                return self.update_annotation_text(dossier_id, annotation_id, str(value))
                
        except Exception as e:
            logger.error(f"Erreur mise à jour annotation {annotation_id}: {e}")
            return False, str(e)
