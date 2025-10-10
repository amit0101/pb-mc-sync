"""
Mailchimp API Client
"""
import hashlib
import httpx
from typing import Dict, List, Optional, Any
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from config import settings


class MailchimpClient:
    """Client for interacting with Mailchimp API"""
    
    def __init__(self):
        self.api_key = settings.mailchimp_api_key
        self.api_url = settings.mailchimp_api_url
        self.list_id = settings.mailchimp_list_id
        self.auth = ("anystring", self.api_key)  # Mailchimp uses basic auth with API key as password
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> Dict:
        """Make HTTP request to Mailchimp API with retry logic"""
        url = f"{self.api_url}/{endpoint}"
        
        # Configure limits to prevent connection pool exhaustion
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        async with httpx.AsyncClient(timeout=60.0, limits=limits) as client:
            try:
                response = await client.request(
                    method=method,
                    url=url,
                    auth=self.auth,
                    params=params,
                    json=json_data
                )
                response.raise_for_status()
                
                # Some DELETE requests return empty response
                if response.status_code == 204:
                    return {}
                    
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Mailchimp API error: {e.response.status_code} - {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"Mailchimp API request failed: {str(e)}")
                raise
    
    @staticmethod
    def get_subscriber_hash(email: str) -> str:
        """
        Get MD5 hash of lowercase email for Mailchimp subscriber ID
        
        Args:
            email: Email address
            
        Returns:
            MD5 hash of lowercase email
        """
        return hashlib.md5(email.lower().encode()).hexdigest()
    
    async def add_or_update_member(
        self, 
        email: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        phone: Optional[str] = None,
        merge_fields: Optional[Dict[str, Any]] = None,
        status: str = "subscribed",
        tags: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Add or update a member in Mailchimp audience
        
        Args:
            email: Member email address
            first_name: First name
            last_name: Last name
            phone: Phone number
            merge_fields: Additional merge fields
            status: Subscription status (subscribed, unsubscribed, cleaned, pending)
            tags: List of tags to apply
            
        Returns:
            Mailchimp member data
        """
        subscriber_hash = self.get_subscriber_hash(email)
        
        # Build merge fields
        fields = merge_fields or {}
        if first_name:
            fields["FNAME"] = first_name
        if last_name:
            fields["LNAME"] = last_name
        if phone:
            fields["PHONE"] = phone
        
        data = {
            "email_address": email,
            "status_if_new": status,  # Only set status for NEW members, preserve existing status
            "merge_fields": fields,
        }
        
        # Use PUT to upsert (create or update)
        logger.info(f"Upserting member {email} to Mailchimp")
        result = await self._request(
            "PUT", 
            f"lists/{self.list_id}/members/{subscriber_hash}",
            json_data=data
        )
        
        # Add tags if provided
        if tags:
            await self.add_tags(email, tags)
        
        return result
    
    async def get_member(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Get member information by email
        
        Args:
            email: Member email address
            
        Returns:
            Member data or None if not found
        """
        subscriber_hash = self.get_subscriber_hash(email)
        
        try:
            logger.info(f"Fetching member {email} from Mailchimp")
            return await self._request("GET", f"lists/{self.list_id}/members/{subscriber_hash}")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
    
    async def update_member_status(self, email: str, status: str) -> Dict[str, Any]:
        """
        Update member subscription status
        
        Args:
            email: Member email address
            status: New status (subscribed, unsubscribed, cleaned, pending)
            
        Returns:
            Updated member data
        """
        subscriber_hash = self.get_subscriber_hash(email)
        data = {"status": status}
        
        logger.info(f"Updating member {email} status to {status}")
        return await self._request(
            "PATCH",
            f"lists/{self.list_id}/members/{subscriber_hash}",
            json_data=data
        )
    
    async def batch_subscribe(
        self, 
        members: List[Dict[str, Any]], 
        update_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Batch subscribe members to list
        
        Args:
            members: List of member dictionaries
            update_existing: Whether to update existing members
            
        Returns:
            Batch operation results
        """
        data = {
            "members": members,
            "update_existing": update_existing
        }
        
        logger.info(f"Batch subscribing {len(members)} members to Mailchimp")
        return await self._request("POST", f"lists/{self.list_id}", json_data=data)
    
    async def add_tags(self, email: str, tags: List[str]) -> Dict[str, Any]:
        """
        Add tags to a member
        
        Args:
            email: Member email address
            tags: List of tag names
            
        Returns:
            Operation result
        """
        subscriber_hash = self.get_subscriber_hash(email)
        data = {
            "tags": [{"name": tag, "status": "active"} for tag in tags]
        }
        
        logger.info(f"Adding tags {tags} to member {email}")
        return await self._request(
            "POST",
            f"lists/{self.list_id}/members/{subscriber_hash}/tags",
            json_data=data
        )
    
    async def get_all_members(
        self, 
        status: Optional[str] = None,
        since_last_changed: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all members from the list with pagination
        
        Args:
            status: Filter by status (subscribed, unsubscribed, cleaned, pending)
            since_last_changed: ISO date string to filter changed members
            
        Returns:
            List of all members
        """
        all_members = []
        offset = 0
        count = 1000  # Max allowed by Mailchimp
        
        while True:
            params = {
                "count": count,
                "offset": offset,
            }
            
            if status:
                params["status"] = status
            if since_last_changed:
                params["since_last_changed"] = since_last_changed
            
            logger.info(f"Fetching members offset {offset}")
            response = await self._request("GET", f"lists/{self.list_id}/members", params=params)
            
            members = response.get("members", [])
            if not members:
                break
            
            all_members.extend(members)
            
            # Check if there are more members
            total = response.get("total_items", 0)
            if len(all_members) >= total:
                break
            
            offset += count
        
        logger.info(f"Fetched {len(all_members)} total members")
        return all_members
    
    @staticmethod
    def calculate_data_hash(member_data: Dict[str, Any]) -> str:
        """
        Calculate hash of member data for change detection
        
        Args:
            member_data: Member data dictionary
            
        Returns:
            MD5 hash of relevant fields
        """
        # Extract fields we care about for sync
        merge_fields = member_data.get("merge_fields", {})
        relevant_fields = {
            "email": member_data.get("email_address", ""),
            "status": member_data.get("status", ""),
            "first_name": merge_fields.get("FNAME", ""),
            "last_name": merge_fields.get("LNAME", ""),
            "phone": merge_fields.get("PHONE", ""),
        }
        
        # Create a stable string representation
        data_str = str(sorted(relevant_fields.items()))
        return hashlib.md5(data_str.encode()).hexdigest()

