"""
Pabau API Client
Based on Pabau API v2 documentation
"""
import hashlib
import httpx
from typing import Dict, List, Optional, Any
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from config import settings


class PabauClient:
    """Client for interacting with Pabau API"""
    
    def __init__(self):
        self.api_key = settings.pabau_api_key
        self.api_url = settings.pabau_api_url
        self.company_id = settings.pabau_company_id
        # Pabau OAuth API doesn't use Authorization header - API key is in URL
        self.headers = {
            "Content-Type": "application/json",
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> Dict:
        """Make HTTP request to Pabau API with retry logic"""
        url = f"{self.api_url}/{self.api_key}/{endpoint}"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    params=params,
                    json=json_data
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Pabau API error: {e.response.status_code} - {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"Pabau API request failed: {str(e)}")
                raise
    
    async def get_contacts(
        self, 
        page: int = 1, 
        page_size: int = 50,  # Pabau API max is 50 per page
        modified_since: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get contacts/clients from Pabau
        
        Args:
            page: Page number for pagination
            page_size: Number of records per page
            modified_since: ISO date string to filter modified contacts
            
        Returns:
            Dict containing contacts data and pagination info
        """
        params = {
            "page": page,
            "per_page": page_size,
        }
        
        if modified_since:
            params["modified_since"] = modified_since
        
        logger.info(f"Fetching contacts page {page} (size: {page_size})")
        return await self._request("GET", "clients", params=params)
    
    async def get_contact_by_id(self, contact_id: str) -> Dict[str, Any]:
        """Get a single contact by ID"""
        logger.info(f"Fetching contact {contact_id}")
        return await self._request("GET", f"contacts/{contact_id}")
    
    async def get_leads(
        self, 
        page: int = 1, 
        page_size: int = 50,  # Pabau API max is 50 per page
        modified_since: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get leads from Pabau
        
        Args:
            page: Page number for pagination
            page_size: Number of records per page
            modified_since: ISO date string to filter modified leads
            
        Returns:
            Dict containing leads data and pagination info
        """
        params = {
            "page": page,
            "per_page": page_size,
        }
        
        if modified_since:
            params["modified_since"] = modified_since
        
        logger.info(f"Fetching leads page {page} (size: {page_size})")
        return await self._request("GET", "leads", params=params)
    
    async def update_contact_marketing_preferences(
        self, 
        contact_id: str, 
        marketing_consent: bool
    ) -> Dict[str, Any]:
        """
        Update contact's marketing consent/preferences
        
        Args:
            contact_id: Pabau contact ID
            marketing_consent: Whether the contact has given marketing consent
        """
        data = {
            "marketing_consent": marketing_consent,
            "email_marketing": marketing_consent,
        }
        
        logger.info(f"Updating marketing preferences for contact {contact_id}: {marketing_consent}")
        return await self._request("PATCH", f"contacts/{contact_id}", json_data=data)
    
    async def get_all_contacts_paginated(
        self, 
        modified_since: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all contacts with automatic pagination
        
        Args:
            modified_since: ISO date string to filter modified contacts
            
        Returns:
            List of all contacts
        """
        all_contacts = []
        page = 1
        
        while True:
            logger.info(f"Fetching contacts page {page}...")
            response = await self.get_contacts(
                page=page, 
                page_size=50,  # Pabau max is 50
                modified_since=modified_since
            )
            
            # Pabau API returns contacts in 'clients' key
            contacts = response.get("clients", [])
            if not contacts:
                logger.info(f"Page {page} returned no contacts - stopping pagination")
                break
            
            all_contacts.extend(contacts)
            logger.info(f"Page {page}: Got {len(contacts)} contacts (total so far: {len(all_contacts)})")
            
            # Continue if we got a full page of 50 (indicates more data might exist)
            # Note: Pabau API's "total" field is unreliable
            if len(contacts) < 50:
                logger.info(f"Page {page} returned < 50 contacts - this is the last page")
                break
            
            page += 1
        
        logger.info(f"Pagination complete: Fetched {len(all_contacts)} total contacts across {page} pages")
        return all_contacts
    
    async def get_all_leads_paginated(
        self, 
        modified_since: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all leads with automatic pagination
        
        Args:
            modified_since: ISO date string to filter modified leads
            
        Returns:
            List of all leads
        """
        all_leads = []
        page = 1
        
        while True:
            logger.info(f"Fetching leads page {page}...")
            response = await self.get_leads(
                page=page, 
                page_size=50,  # Pabau max is 50
                modified_since=modified_since
            )
            
            # Pabau API returns leads in 'leads' key
            leads = response.get("leads", [])
            if not leads:
                logger.info(f"Page {page} returned no leads - stopping pagination")
                break
            
            all_leads.extend(leads)
            logger.info(f"Page {page}: Got {len(leads)} leads (total so far: {len(all_leads)})")
            
            # Continue if we got a full page of 50 (indicates more data might exist)
            # Note: Pabau API's "total" field is unreliable
            if len(leads) < 50:
                logger.info(f"Page {page} returned < 50 leads - this is the last page")
                break
            
            page += 1
        
        logger.info(f"Pagination complete: Fetched {len(all_leads)} total leads across {page} pages")
        return all_leads
    
    @staticmethod
    def calculate_data_hash(contact_data: Dict[str, Any]) -> str:
        """
        Calculate hash of contact data for change detection
        
        Args:
            contact_data: Contact data dictionary
            
        Returns:
            MD5 hash of relevant fields
        """
        # Extract fields we care about for sync
        relevant_fields = {
            "email": contact_data.get("email", ""),
            "first_name": contact_data.get("first_name", ""),
            "last_name": contact_data.get("last_name", ""),
            "phone": contact_data.get("phone", ""),
            "mobile": contact_data.get("mobile", ""),
        }
        
        # Create a stable string representation
        data_str = str(sorted(relevant_fields.items()))
        return hashlib.md5(data_str.encode()).hexdigest()

