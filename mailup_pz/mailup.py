"""
Libreria MailUpPZ
----------------
Modulo per l'integrazione semplificata con le API REST di MailUp.
Gestisce l'autenticazione OAuth2 (Password Grant e Refresh Token) con persistenza
locale e fornisce wrapper per la gestione di destinatari Email e SMS.
"""

from typing import Optional, Dict, Any, List
import requests
from datetime import datetime, timedelta
import json
import time
import base64
import logging
from logging import Logger
import os
import sys

class MailUpPZ:
    """
    Client principale per l'interazione con MailUp.

    Questa classe automatizza il recupero dei token e semplifica le chiamate
    ai vari endpoint per la gestione di liste, gruppi e messaggi.
    """

    _BASE_URL = "https://services.mailup.com"
    _API_VERSION = "v1.1"
    _PAGE_SIZE = 1000
    _DIZIONARIO = {
        'nome': 1,
        'cognome': 2,
        'azienda': 3,
        'città': 4,
        'provincia': 5,
        'cap': 6,
        'regione': 7,
        'paese': 8,
        'indirizzo': 9,
        'fax': 10,
        'telefono': 11,
        'IDCliente': 12,
        'IDUltimoOrdine': 13,
        'DataUltimoOrdine': 14,
        'TotaleUltimoOrdine': 15,
        'IDProdottiUltimoOrdine': 16,
        'IDCategorieUltimoOrdine': 17,
        'DataUltimoOrdineSpedito': 18,
        'IDUltimoOrdineSpedito': 19,
        'DataCarrelloAbbandonato': 20,
        'TotaleCarrelloAbbandonato': 21,
        'IDCarrelloAbbandonato': 22,
        'TotaleFatturato': 23,
        'TotaleFatturatoUltimi12Mesi': 24,
        'TotaleFatturatoUltimi30gg': 25,
        'IDTuttiProdottiAcquistati': 26,
        'Compleanno': 27,
        'Categoria': 28,
        'Segmento': 29,
        'Fonte': 30,
        'Lista origine': 31,
        'Tag Fiera': 32,
        'Lingua': 33,
        'Alias': 34,
        'IDstato': 35,
        'Stagionalità': 36,
    }


    def __init__(self, client_id: str, client_secret: str, username: str, password: str, logger: Logger = None) -> None:
        """
        Inizializza il client MailUp.

        Args:
            client_id (str): ID fornito da MailUp sezione Developer.
            client_secret (str): Secret fornito da MailUp sezione Developer.
            username (str): Nome utente per il login.
            password (str): Password per il login.
            logger (Logger, optional): Istanza di un logger per il debug.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.logger = logger
        self.access_token = None
        self.refresh_token = None
        self.obtained_time = None
        self.elapsed_time = None

    # =========================================================
    # METODI PRIVATI (Utility & Internal Requests)
    # =========================================================

    def _log_error(self,msg: str) -> None:
        if self.logger is not None:
            self.logger.error(msg)

    def _request(self,method: str,url: str,**kwargs) -> Optional[requests.Response]:
        """Wrapper interno per le richieste HTTP con gestione timeout ed errori."""
        try:
            kwargs.setdefault("timeout", 30)
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            self._log_error(f"Timeout while making {method} request to {url}")
        except requests.exceptions.RequestException as e:
            self._log_error(f"Error during {method} request to {url}: {e}")
        return None

    def _get_auth_headers(self) -> Dict[str, str]:
        """Genera gli header con il token Bearer valido."""
        return {"Authorization": f"Bearer {self._get_valid_token()}"}

    # =========================================================
    # METODI PRIVATI (Gestione Autenticazione e Token)
    # =========================================================

    def _get_token_file_path(self):
        """Restituisce il percorso del file JSON dei token."""
        # library_dir = os.path.dirname(os.path.abspath(__file__))
        main_script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        return os.path.join(main_script_dir, ".mailup_tokens.json")

    def _save_tokens(self, tokens):
        """
        Calcola la scadenza e salva i token su disco.
        Imposta la scadenza con 300 secondi di margine di sicurezza
        """
        expires_in = tokens.get('expires_in', 3600)
        tokens['timestamp_scadenza'] = (datetime.now() + timedelta(seconds=expires_in - 300)).timestamp()

        try:
            with open(self._get_token_file_path(), 'w') as f:
                json.dump(tokens, f)
        except Exception as e:
            if hasattr(self, 'logger') and self.logger:
                self.logger.error(f"Errore nel salvataggio dei token: {e}")

    def _load_tokens(self):
        """Carica i token dal file locale."""
        path = self._get_token_file_path()
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                if hasattr(self, 'logger') and self.logger:
                    self.logger.error(f"Errore nel caricamento dei token: {e}")
        return None

    def _password_grant_login(self, tentativo=1):
        """Esegue il login iniziale tramite Password Grant."""
        url = "https://services.mailup.com/Authorization/OAuth/Token"
        auth_str = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {auth_str}"
        }
        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password
        }

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()
        else:
            if hasattr(self, 'logger') and self.logger:
                self.logger.error(f"Errore login Password Grant: {response.text}. Tentativo {tentativo}.")

            # Mi fermo dopo 5 tentativi per evitare loop infiniti o ban dell'IP
            if tentativo < 5:
                try:
                    password_blocks = self.password.split("-")
                    self.password = "-".join(password_blocks[:-1]) + "-" + str(int(password_blocks[-1]) + 1)
                    if self.logger: self.logger.info(f"Ritento con una nuova password...")
                    return self._password_grant_login(tentativo + 1) # <-- Passo il contatore incrementato
                except ValueError:
                    if self.logger: self.logger.error("Formato password non valido per l'incremento numerico.")
                    return None
            else:
                if self.logger: self.logger.error("Raggiunto il limite massimo di tentativi per la password.")
                return None

    def _refresh_token_call(self, refresh_token):
        """Richiede un nuovo access token tramite refresh token."""
        url = "https://services.mailup.com/Authorization/OAuth/Token"
        auth_str = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {auth_str}"
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()
        else:
            if hasattr(self, 'logger') and self.logger:
                self.logger.warning(f"Refresh token fallito o scaduto: {response.text}")
            return None

    def _get_valid_token(self):
        """
        Recupera o genera un Access Token valido.
        """
        tokens = self._load_tokens()
        ora_attuale = datetime.now().timestamp()

        # CASO 1: Abbiamo il token salvato ed è ancora valido (non scaduto)
        if tokens and ora_attuale < tokens.get('timestamp_scadenza', 0):
            return tokens['access_token']

        # CASO 2: L'access token è scaduto, ma abbiamo un refresh_token
        if tokens and 'refresh_token' in tokens:
            new_tokens = self._refresh_token_call(tokens['refresh_token'])

            if new_tokens:
                self._save_tokens(new_tokens)
                return new_tokens['access_token']

        # CASO 3: Non c'è file locale, oppure anche il refresh_token era scaduto -> Login da zero
        if hasattr(self, 'logger') and self.logger:
            self.logger.info("Nessun token valido trovato. Eseguo login completo con credenziali...")

        new_tokens = self._password_grant_login()
        if new_tokens:
            self._save_tokens(new_tokens)
            return new_tokens['access_token']

        if hasattr(self, 'logger') and self.logger:
            self.logger.error("Impossibile ottenere un token da MailUp.")
        return None
    
    # =========================================================
    # METODI PRIVATI (Helper Core per SMS ed Email)
    # =========================================================

    def _get_sms_recipients(
            self,
            list_id: str,
            group_id: Optional[str] = None,
            page_number: int = 0
        ) -> Optional[List[Dict[str, str]]]:
        endpoint = f'{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Sms/List/{list_id}/Recipients/SmsOptins'
        if group_id is not None:
            endpoint += "?Groups=" + str(group_id)

        params = {
            "PageNumber" : page_number,
            "PageSize" : self._PAGE_SIZE
        }

        response = self._request("get", endpoint, headers=self._get_auth_headers(), params=params)
        if response is None:
            return None
        if response.status_code != 200:
            self._log_error(f"Error retrieving SMS recipients: {response.status_code} - {response.text}")
            return None
        
        recipients = []
        for item in response.json()["Items"]:
            recipient = {}
            recipient["idRecipient"] = str(item["idRecipient"])
            recipient["MobileNumber"] = item["MobileNumber"]
            recipient["MobilePrefix"] = item["MobilePrefix"]
            recipient["Status"] = item["Status"]
            recipient["Optin_Date"] = item["Optin_Date"]
            recipients.append(recipient)
        
        if response.json().get("IsPaginated"):
            skipped = response.json()["Skipped"]
            TotalElementsCount = response.json()["TotalElementsCount"]
            if skipped + self._PAGE_SIZE < TotalElementsCount:
                recipients.extend(self._get_sms_recipients(list_id, group_id, page_number + 1))
        
        return recipients
    
    def _get_email_recipients(
            self,
            recipient_type: str,
            list_id: str,
            group_id: Optional[str] = None,
            page_number: int = 0
    ) -> Optional[List[Dict[str, str]]]:
        endpoint = f'{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/List/{list_id}/Recipients/{recipient_type}'
        if group_id is not None:
            endpoint += "?Groups=" + str(group_id)
        params = {
            "PageNumber": page_number,
            "PageSize": self._PAGE_SIZE
        }

        response = self._request("get", endpoint, headers=self._get_auth_headers(), params=params)
        if response is None:
            return None
        if response.status_code != 200:
            self._log_error(f"Error retrieving email recipients: {response.status_code} - {response.text}")
            return None

        data = response.json()
        recipients = []
        for item in data.get("Items", []):
            recipient = {}
            if "Fields" in item:
                recipient = {
                    f.get("Description"): f.get("Value") if f.get("Value") != "" else None
                    for f in item["Fields"]
                }
            recipient["idRecipient"] = str(item["idRecipient"])
            recipient["Email"] = item["Email"]
            if "Optin_Date" in item:
                recipient["Optin_Date"] = item["Optin_Date"]
            if "MobileNumber" in item and "MobilePrefix" in item:
                recipient["MobileNumber"] = item["MobileNumber"]
                recipient["MobilePrefix"] = item["MobilePrefix"]
            recipients.append(recipient)

        if response.json().get("IsPaginated"):
            skipped = response.json()["Skipped"]
            TotalElementsCount = response.json()["TotalElementsCount"]
            if skipped + self._PAGE_SIZE < TotalElementsCount:
                recipients.extend(self._get_email_recipients(recipient_type, list_id, group_id, page_number + 1))

        return recipients

    def _create_recipient(
            self,
            endpoint: str,
            email: str,
            mobile_prefix: str = "",
            mobile_number: str = "",
            fields: Dict[str, str] = None
        ) -> Optional[str]:
        if fields is None:
            fields = {}
        for nome, _ in fields.items(): #Controllo che tutti i campi siano presenti nel dizionario
            if nome not in self._DIZIONARIO:
                self._log_error(f"Invalid field: {nome}, creation aborted.")
                return None
            
        data = {}
        data["Email"] = email

        if mobile_prefix and mobile_number:
            data["MobilePrefix"] = mobile_prefix
            data["MobileNumber"] = mobile_number

        data["Fields"] = [
                {
                    "Description": nome,
                    "Id": self._DIZIONARIO[nome],
                    "Value": valore
                } for nome, valore in fields.items()
            ]

        response = self._request("post", endpoint, headers=self._get_auth_headers(), json=data)
        if response is None:
            return None
        if response.status_code != 200:
            self._log_error(f"Error creating recipient: {response.status_code} - {response.text}")
            return None
        
        return str(response.json())

    # =========================================================
    # FUNZIONALITÀ PUBBLICHE: SMS
    # =========================================================

    def get_sms_recipients(self, list_id: str, group_id: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """Restituisce tutti i destinatari SMS (Iscritti, Disiscritti, Sospesi)."""
        return self._get_sms_recipients(list_id, group_id)

    def send_message(self, id_message: str, id_recipient: str) -> bool:
        recipient = self.get_recipient_by_id(id_recipient)
        if recipient is None:
            self._log_error(f"Recipient with ID {id_recipient} not found.")
            return False
        
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Sms/Send"

        data = {
            "Number":recipient["MobileNumber"],
            "Prefix":recipient["MobilePrefix"],
            "idMessage":id_message
        }
        
        response = self._request("post", endpoint, headers=self._get_auth_headers(), json=data)
        if response is None:
            return False
        if response.status_code != 200 or response.json()["Sent"] != 1:
            self._log_error(f"Error sending message: {response.status_code} - {response.text}")
            return False
        
        return True
    
    def get_sms_list_recipients(self, list_id: str) -> Optional[List[Dict[str, str]]]:
        """[DEPRECATED] Usa get_sms_recipients."""
        return self._get_sms_recipients(list_id)
    
    def get_sms_group_recipients(self, list_id: str, group_id: str) -> Optional[List[Dict[str, str]]]:
        """[DEPRECATED] Usa get_sms_recipients con group_id."""
        return self._get_sms_recipients(list_id, group_id)
    
    # =========================================================
    # FUNZIONALITÀ PUBBLICHE: EMAIL
    # =========================================================

    def get_email_list_recipients(self, list_id: str, group_id: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """Restituisce i destinatari EmailOptins."""
        return self._get_email_recipients("EmailOptins", list_id, group_id)

    def get_email_list_recipients_subscribed(self, list_id: str, group_id: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """Restituisce solo i destinatari iscritti."""
        return self._get_email_recipients("Subscribed", list_id, group_id)

    def get_email_list_recipients_unsubscribed(self, list_id: str, group_id: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """Restituisce solo i destinatari disiscritti."""
        return self._get_email_recipients("Unsubscribed", list_id, group_id)

    # =========================================================
    # FUNZIONALITÀ PUBBLICHE: GENERICHE (Recipient & Fields)
    # =========================================================

    def get_available_fields(self) -> List[str]:
        """Restituisce la lista dei campi disponibili."""
        return list(self._DIZIONARIO.keys())
    
    def get_id_from_email(self, email: str) -> Optional[str]:
        """Recupera l'ID interno di MailUp partendo dall'indirizzo email."""
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Recipients?email=\"{email}\""

        response = self._request("get", endpoint, headers=self._get_auth_headers())
        if response is None:
            return None
        
        data = response.json()
        items = data.get("Items", [])
        if response.status_code != 200 or len(items) == 0:
            self._log_error(f"Error retrieving recipient ID: {response.status_code} - {response.text}")
            return None
        return str(items[0]["idRecipient"])

    def get_recipient_by_id(self, id_recipient: str) -> Optional[Dict[str, str]]:
        """Recupera i dettagli completi di un destinatario tramite il suo ID."""
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Recipients/{id_recipient}"

        response = self._request("get", endpoint, headers=self._get_auth_headers())
        if response is None:
            return None
        if response.status_code != 200:
            self._log_error(f"Error retrieving recipient: {response.status_code} - {response.text}")
            return None
        
        fields = {f.get("Description"):f.get("Value") for f in response.json()["Fields"] if f.get("Value") != ""} #Prendi i field se non sono vuoti
        fields["idRecipient"] = response.json()["idRecipient"]
        fields["Email"] = response.json()["Email"]
        fields["MobileNumber"] = response.json()["MobileNumber"]
        fields["MobilePrefix"] = response.json()["MobilePrefix"]

        return fields

    def create_recipient_to_list(self, list_id: str, email: str, mobile_prefix: str = "", mobile_number: str = "", fields: Dict[str, str] = None) -> Optional[str]:
        """Crea un nuovo destinatario all'interno di una lista specifica."""
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/List/{list_id}/Recipient"
        return self._create_recipient(endpoint, email, mobile_prefix, mobile_number, fields)

    def create_recipient_to_group(self, group_id: str, email: str, mobile_prefix: str = "", mobile_number: str = "", fields: Dict[str, str] = None) -> Optional[str]:
        """Crea un nuovo destinatario all'interno di un gruppo specifico."""
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Group/{group_id}/Recipient"
        return self._create_recipient(endpoint, email, mobile_prefix, mobile_number, fields)
    
    def subscribe_recipient_to_group(self, group_id: str, id: str) -> None:
        """Iscrive un destinatario esistente a un gruppo specifico."""
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Group/{group_id}/Subscribe/{id}"
        self._request("post", endpoint, headers=self._get_auth_headers())
