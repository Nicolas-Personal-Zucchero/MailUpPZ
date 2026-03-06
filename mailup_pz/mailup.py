#pip install .
from typing import Optional, Dict, Any, List
import requests
from datetime import datetime
import json
import time
import base64
import logging
from logging import Logger

class MailUpPZ:
    # client = None
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
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.logger = logger
        self.access_token = None
        self.refresh_token = None
        self.obtained_time = None
        self.elapsed_time = None

    ##########Privati##########

    def _log_error(
            self,
            msg: str
        ) -> None:
        if self.logger is not None:
            self.logger.error(msg)

    def _request(
            self,
            method: str,
            url: str,
            **kwargs
        ) -> Optional[requests.Response]:
        try:
            kwargs.setdefault("timeout", 10)
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            self._log_error(f"Timeout while making {method} request to {url}")
        except requests.exceptions.RequestException as e:
            self._log_error(f"Error during {method} request to {url}: {e}")
        return None

    def _get_auth_headers(
            self
        ) -> Dict[str, str]:
        current_time = time.time()
        
        # Request a new access token if not already obtained or if it has expired
        if (self.access_token is None or
            self.obtained_time is None or
            self.elapsed_time is None or
            current_time - self.obtained_time >= self.elapsed_time - 100
        ):
            # Encode client data in base64
            client_data = f"{self.client_id}:{self.client_secret}"
            client_data_bytes = client_data.encode("ascii")
            base64_client_data_bytes = base64.b64encode(client_data_bytes)
            base64_client_data = base64_client_data_bytes.decode("ascii")
            
            # Perform authentication
            auth_url = f"{self._BASE_URL}/Authorization/OAuth/Token"
            auth_data = {
                "grant_type": "password",
                "username": self.username,
                "password": self.password
            }
            auth_headers = {
                "Authorization": f"Basic {base64_client_data}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            response = self._request("post", auth_url, data=auth_data, headers=auth_headers)
            if response is None:
                return {}
            response_data = response.json()
            
            # Update token variables
            self.access_token = response_data["access_token"]
            self.elapsed_time = response_data["expires_in"]
            self.refresh_token = response_data["refresh_token"]
            self.obtained_time = time.time()
        
        return {
            "Authorization": f"Bearer {self.access_token}"
        }
    
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
            recipient["Optin_Date"] = item["Optin_Date"]
            # recipient["MobileNumber"] = item["MobileNumber"]
            # recipient["MobilePrefix"] = item["MobilePrefix"]
            recipients.append(recipient)

        if response.json().get("IsPaginated"):
            skipped = response.json()["Skipped"]
            TotalElementsCount = response.json()["TotalElementsCount"]
            if skipped + self._PAGE_SIZE < TotalElementsCount:
                recipients.extend(self._get_email_recipients(recipient_type, list_id, group_id, page_number + 1))

        return recipients

    ##########Pubblici##########

    #Tested and working
    def get_email_list_recipients_subscribed(self, list_id: str, group_id: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        return self._get_email_recipients("Subscribed", list_id, group_id)

    def get_email_list_recipients_unsubscribed(self, list_id: str, group_id: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        return self._get_email_recipients("Unsubscribed", list_id, group_id)
        
    #Tested and working
    def get_available_fields(self) -> List[str]:
        return list(self._DIZIONARIO.keys())
    
    #Tested and working
    def get_email_list_recipients(self, list_id: str, group_id: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        return self._get_email_recipients("EmailOptins", list_id, group_id)
    
    #Tested and working
    def get_sms_list_recipients(self, list_id: str) -> Optional[List[Dict[str, str]]]:
        return self._get_sms_recipients(list_id)
    
    #Tested and working
    def get_sms_group_recipients(self, list_id: str, group_id: str) -> Optional[List[Dict[str, str]]]:
        return self._get_sms_recipients(list_id, group_id)
    
    #Tested and working
    def get_id_from_email(self, email: str) -> Optional[str]:
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

    #Tested and working
    def get_recipient_by_id(self, id_recipient: str) -> Optional[Dict[str, str]]:
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

    #Tested and working
    def create_recipient_to_list(self, list_id: str, email: str, mobile_prefix: str = "", mobile_number: str = "", fields: Dict[str, str] = None) -> Optional[str]:
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/List/{list_id}/Recipient"
        return self._create_recipient(endpoint, email, mobile_prefix, mobile_number, fields)
    
    #Tested and working
    def create_recipient_to_group(self, group_id: str, email: str, mobile_prefix: str = "", mobile_number: str = "", fields: Dict[str, str] = None) -> Optional[str]:
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Group/{group_id}/Recipient"
        return self._create_recipient(endpoint, email, mobile_prefix, mobile_number, fields)
    
    #Tested and working
    def subscribe_recipient_to_group(self, group_id: str, id: str) -> None:
        endpoint = f"{self._BASE_URL}/API/{self._API_VERSION}/Rest/ConsoleService.svc/Console/Group/{group_id}/Subscribe/{id}"
        
        self._request("post", endpoint, headers=self._get_auth_headers())
        return
    
    #Tested and working
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
